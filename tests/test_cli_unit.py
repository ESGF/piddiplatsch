"""Unit tests for CLI module."""

import warnings
from datetime import date, timedelta
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from piddiplatsch.cli import cli
from piddiplatsch.commands import (
    Command,
    ConfigShowCommand,
    ConfigValidateCommand,
    HarvestCommand,
    MapCommand,
    PublishCommand,
    SyncCommand,
)
from piddiplatsch.config import config
from piddiplatsch.consumer import HarvestProcessor
from piddiplatsch.monitoring.progress import NoOpProgress, Progress
from piddiplatsch.result import (
    FeedResult,
    ProjectPublishResult,
    PublishResult,
)


@pytest.fixture
def runner():
    """Provides a Click CLI test runner."""
    return CliRunner()


class TestCLIBasics:

    def test_brotli_gil_fallback_warning_is_filtered_only_during_cli(
        self, runner, monkeypatch
    ):
        def execute(_command):
            warnings.warn(
                "The global interpreter lock (GIL) has been enabled to load module "
                "'_brotli', which has not declared that it can run safely without the GIL. "
                "To override this behavior, run with PYTHON_GIL=0.",
                RuntimeWarning,
                stacklevel=1,
            )
            warnings.warn("Unrelated runtime warning", RuntimeWarning, stacklevel=1)
            warnings.warn(
                "The global interpreter lock (GIL) has been enabled to load module "
                "'another_extension', which has not declared that it can run safely without the GIL.",
                RuntimeWarning,
                stacklevel=1,
            )

        monkeypatch.setattr(SyncCommand, "execute", execute)
        with warnings.catch_warnings(record=True) as emitted:
            warnings.simplefilter("always")
            original_filters = list(warnings.filters)
            result = runner.invoke(cli, ["--silent", "sync", "--workers", "4"])
            assert warnings.filters == original_filters
        assert result.exit_code == 0, result.output
        assert len(emitted) == 2
        assert str(emitted[0].message) == "Unrelated runtime warning"
        assert "another_extension" in str(emitted[1].message)

    """Test basic CLI functionality."""

    def test_commands_implement_common_api(self):
        command_types = (
            SyncCommand,
            HarvestCommand,
            MapCommand,
            PublishCommand,
            ConfigValidateCommand,
            ConfigShowCommand,
        )
        assert all(issubclass(command_type, Command) for command_type in command_types)
        assert all(callable(command_type.execute) for command_type in command_types)

    def test_cli_help(self, runner):
        """Test that CLI shows help message."""
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "CLI to interact with Kafka and Handle Service" in result.output
        assert "consume" not in result.output
        assert "sync" in result.output
        assert "  batch " not in result.output
        assert "harvest" in result.output
        assert "map" in result.output
        assert "publish" in result.output
        assert "retry" not in result.output

    @patch("piddiplatsch.consumer.start_consumer")
    def test_consume_command_is_removed(self, start_consumer, runner):
        result = runner.invoke(cli, ["consume"])
        assert result.exit_code == 2
        assert "No such command 'consume'" in result.output
        start_consumer.assert_not_called()

    def test_cli_help_short(self, runner):
        """Test that CLI shows help with -h flag."""
        result = runner.invoke(cli, ["-h"])
        assert result.exit_code == 0
        assert "CLI to interact with Kafka and Handle Service" in result.output

    def test_cli_version(self, runner):
        """Test that CLI shows version."""
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        # Version output varies, just check it doesn't error

    def test_cli_no_command(self, runner):
        """Test that CLI shows usage when no command is provided."""
        result = runner.invoke(cli, [])
        # Click returns exit code 2 when required command is missing
        assert result.exit_code == 2 or result.exit_code == 0
        assert "Usage:" in result.output or "Commands:" in result.output


class TestHarvestCommand:
    def test_native_runtime_loads_before_progress(self):
        calls = []

        def prepare_runtime():
            calls.append("native import warning")

        def create_progress(**kwargs):
            calls.append("progress")
            return NoOpProgress()

        with (
            patch(
                "piddiplatsch.consumer.prepare_kafka_runtime",
                side_effect=prepare_runtime,
            ),
            patch.object(HarvestCommand, "progress", side_effect=create_progress),
            patch("piddiplatsch.consumer.start_consumer") as consume,
        ):
            HarvestCommand().run_consumer(title="harvest")

        assert calls == ["native import warning", "progress"]
        consume.assert_called_once()

    def test_harvest_help(self, runner):
        result = runner.invoke(cli, ["harvest", "--help"])
        assert result.exit_code == 0
        assert "raw JSONL" in result.output
        assert "--idle-timeout" in result.output
        assert "--limit" in result.output

    @patch("piddiplatsch.consumer.start_consumer")
    def test_harvest_dumps_without_mapping(self, mock_start_consumer, runner):
        result = runner.invoke(cli, ["harvest"])
        assert result.exit_code == 0
        kwargs = mock_start_consumer.call_args.kwargs
        assert isinstance(kwargs["processor"], HarvestProcessor)
        assert kwargs["dump_messages"] is True
        assert kwargs["force"] is True
        assert kwargs["idle_timeout"] == 5.0
        assert kwargs["limit"] is None
        assert kwargs["monitor_db"] is True

    @patch("piddiplatsch.consumer.start_consumer")
    def test_harvest_zero_idle_timeout_keeps_consuming(
        self, mock_start_consumer, runner
    ):
        result = runner.invoke(cli, ["harvest", "--idle-timeout", "0"])
        assert result.exit_code == 0, result.output
        assert mock_start_consumer.call_args.kwargs["idle_timeout"] is None

    @patch("piddiplatsch.consumer.start_consumer")
    def test_harvest_passes_idle_timeout(self, mock_start_consumer, runner):
        result = runner.invoke(cli, ["harvest", "--idle-timeout", "2.5"])

        assert result.exit_code == 0
        assert mock_start_consumer.call_args.kwargs["idle_timeout"] == 2.5

    @patch("piddiplatsch.consumer.start_consumer")
    def test_harvest_passes_limit(self, mock_start_consumer, runner):
        result = runner.invoke(cli, ["harvest", "--limit", "10"])

        assert result.exit_code == 0
        assert mock_start_consumer.call_args.kwargs["limit"] == 10

    @patch("piddiplatsch.consumer.start_consumer")
    def test_harvest_rejects_non_positive_limit(self, mock_start_consumer, runner):
        result = runner.invoke(cli, ["harvest", "--limit", "0"])

        assert result.exit_code == 2
        assert "Invalid value for '--limit'" in result.output
        mock_start_consumer.assert_not_called()

    @patch("piddiplatsch.consumer.start_consumer")
    def test_harvest_with_verbose_uses_stream_progress(
        self, mock_start_consumer, runner
    ):
        result = runner.invoke(cli, ["--verbose", "harvest"])

        assert result.exit_code == 0
        assert isinstance(mock_start_consumer.call_args.kwargs["progress"], Progress)


class TestMapCommand:
    def test_map_help(self, runner):
        result = runner.invoke(cli, ["map", "--help"])
        assert result.exit_code == 0
        assert "catalog" in result.output
        assert "Handle JSONL" in result.output
        assert "--catalog-dir" in result.output
        assert "--version-history" in result.output
        assert "--date" in result.output
        assert "--from" in result.output
        assert "--to" in result.output
        assert "--force" not in result.output

    @patch("piddiplatsch.commands.map.map_catalog")
    def test_map_calls_mapper(self, mapper, runner, tmp_path):
        mapper.return_value = FeedResult(total=3, succeeded=3)
        result = runner.invoke(
            cli,
            [
                "--silent",
                "map",
                "--catalog-dir",
                str(tmp_path),
                "--project",
                "cmip6",
                "--limit",
                "3",
                "--offset",
                "2",
                "--version-history",
                "--handle-profile",
                "dkrz-test",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Mapped 3/3 catalog items" in result.output
        kwargs = mapper.call_args.kwargs
        assert isinstance(kwargs.pop("progress"), NoOpProgress)
        assert kwargs == {
            "directory": tmp_path,
            "input_date": "last",
            "from_date": None,
            "to_date": None,
            "projects": ("cmip6",),
            "limit": 3,
            "offset": 2,
            "version_history": True,
            "verbose": False,
            "handle_profile": "dkrz-test",
        }

    @patch("piddiplatsch.commands.map.map_catalog")
    def test_map_passes_global_verbose(self, mapper, runner):
        mapper.return_value = FeedResult(total=1, succeeded=1)
        result = runner.invoke(cli, ["--verbose", "map"])
        assert result.exit_code == 0, result.output
        assert mapper.call_args.kwargs["verbose"] is True
        assert isinstance(mapper.call_args.kwargs["progress"], Progress)
        assert mapper.call_args.kwargs["directory"] is None

    @patch("piddiplatsch.commands.map.map_catalog")
    @pytest.mark.parametrize("options", [["--force"], ["dump.jsonl"]])
    def test_map_rejects_raw_dump_arguments(self, mapper, runner, options):
        result = runner.invoke(cli, ["map", *options])
        assert result.exit_code == 2
        mapper.assert_not_called()

    def test_map_requires_harvested_project(self, runner):
        result = runner.invoke(cli, ["map", "--project", "cmip6"])
        assert result.exit_code == 1
        assert "run harvest first" in result.output

    @patch("piddiplatsch.commands.map.map_catalog")
    def test_map_rejects_named_and_all_projects(self, mapper, runner):
        result = runner.invoke(cli, ["map", "--project", "cmip6", "--all-projects"])
        assert result.exit_code == 2
        assert "cannot be combined" in result.output
        mapper.assert_not_called()

    @patch("piddiplatsch.commands.map.map_catalog")
    def test_map_can_disable_version_history(self, mapper, runner):
        mapper.return_value = FeedResult()
        result = runner.invoke(cli, ["map", "--all-projects", "--no-version-history"])
        assert result.exit_code == 0, result.output
        assert mapper.call_args.kwargs["version_history"] is False
        assert mapper.call_args.kwargs["projects"] == "all"
        assert "No catalog items found" in result.output


class TestPublishCommand:
    def test_publish_help(self, runner):
        result = runner.invoke(cli, ["publish", "--help"])

        assert result.exit_code == 0
        assert "Publish prepared handles" in result.output
        assert "--limit" in result.output
        assert "--offset" in result.output
        assert "--retries" in result.output
        assert "--retry-delay" in result.output
        assert "--workers" in result.output
        assert "--project" in result.output
        assert "--handle-profile" in result.output
        assert "--date" in result.output
        assert "--from" in result.output
        assert "--to" in result.output

    @pytest.mark.parametrize("relative", (False, True))
    @patch("piddiplatsch.commands.publish.HandlePublisher")
    def test_publish_selects_inclusive_date_range(
        self, publisher_cls, runner, tmp_path, relative
    ):
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=3)
        handles_dir = tmp_path / "cmip6" / "handles"
        handles_dir.mkdir(parents=True)
        selected = tuple(
            handles_dir / f"handles_{day.isoformat()}.jsonl"
            for day in (start, start + timedelta(days=2), end)
        )
        for source in reversed(selected):
            source.touch()
        for day in (start - timedelta(days=1), end + timedelta(days=1)):
            (handles_dir / f"handles_{day.isoformat()}.jsonl").touch()
        (handles_dir / "handles_last.jsonl").touch()
        (handles_dir / "handles_2026-02-30.jsonl").touch()
        (
            handles_dir / f"handles_{(start + timedelta(days=1)).isoformat()}.jsonl"
        ).mkdir()
        config._set("consumer", "output_dir", str(tmp_path))
        publisher_cls.return_value.run.return_value = PublishResult(
            total=3, succeeded=3
        )

        result = runner.invoke(
            cli,
            [
                "publish",
                "--project",
                "CMIP6",
                "--from",
                "today-4" if relative else start.isoformat(),
                "--to",
                "today-1" if relative else end.isoformat(),
                "--offset",
                "1",
                "--limit",
                "2",
            ],
        )

        assert result.exit_code == 0, result.output
        publisher_cls.return_value.run.assert_called_once()
        assert publisher_cls.return_value.run.call_args.args[0] == selected
        assert publisher_cls.return_value.run.call_args.kwargs["offset"] == 1
        assert publisher_cls.return_value.run.call_args.kwargs["limit"] == 2

    @pytest.mark.parametrize(
        ("args", "message"),
        [
            (["--from", "today"], "must be provided together"),
            (["--to", "today"], "must be provided together"),
            (["--from", "today", "--to", "yesterday"], "on or before"),
            (["--from", "last", "--to", "today"], "range boundary"),
            (["--from", "today", "--to", "last"], "range boundary"),
            (["--from", "2026-02-30", "--to", "today"], "Invalid value"),
            (
                ["--from", "today", "--to", "today", "--date", "last"],
                "cannot be combined",
            ),
        ],
    )
    @patch("piddiplatsch.commands.publish.HandlePublisher")
    def test_publish_rejects_invalid_ranges(self, publisher_cls, runner, args, message):
        result = runner.invoke(cli, ["publish", "--project", "cmip6", *args])

        assert result.exit_code == 2
        assert message in result.output
        publisher_cls.assert_not_called()

    @patch("piddiplatsch.commands.publish.HandlePublisher")
    def test_publish_range_requires_project(self, publisher_cls, runner):
        result = runner.invoke(cli, ["publish", "--from", "yesterday", "--to", "today"])

        assert result.exit_code == 2
        assert "requires --project" in result.output
        publisher_cls.assert_not_called()

    @patch("piddiplatsch.commands.publish.HandlePublisher")
    def test_publish_rejects_path_and_range(self, publisher_cls, runner, tmp_path):
        result = runner.invoke(
            cli,
            [
                "publish",
                str(tmp_path),
                "--project",
                "cmip6",
                "--from",
                "today",
                "--to",
                "today",
            ],
        )

        assert result.exit_code == 2
        assert "cannot be combined" in result.output
        publisher_cls.assert_not_called()

    @patch("piddiplatsch.commands.publish.HandlePublisher")
    def test_publish_range_requires_matching_files(
        self, publisher_cls, runner, tmp_path
    ):
        config._set("consumer", "output_dir", str(tmp_path))
        result = runner.invoke(
            cli,
            ["publish", "--project", "cmip6", "--from", "today", "--to", "today"],
        )

        assert result.exit_code == 1
        assert "No dated cmip6 Handle files found from" in result.output
        publisher_cls.assert_not_called()

    @patch("piddiplatsch.commands.publish.HandlePublisher")
    def test_publish_reports_success(self, publisher_cls, runner, tmp_path):
        source = tmp_path / "handles.jsonl"
        source.touch()
        publisher_cls.return_value.run.return_value = PublishResult(
            total=3,
            succeeded=3,
            result_file=tmp_path / "published" / "results.jsonl",
        )

        result = runner.invoke(cli, ["publish", str(source)])

        assert result.exit_code == 0
        assert "Published 3/3 handles" in result.output
        assert "Publication results:" in result.output
        assert "results.jsonl" in result.output
        publisher_cls.return_value.run.assert_called_once()
        assert publisher_cls.return_value.run.call_args.kwargs["limit"] is None
        assert publisher_cls.return_value.run.call_args.kwargs["offset"] == 0
        assert publisher_cls.return_value.run.call_args.kwargs["retries"] == 0
        assert publisher_cls.return_value.run.call_args.kwargs["retry_delay"] == 1.0
        assert publisher_cls.return_value.run.call_args.kwargs["workers"] == 1

    @patch("piddiplatsch.commands.publish.HandlePublisher")
    def test_publish_resolves_project_date_from_output_dir(
        self, publisher_cls, runner, tmp_path
    ):
        output_dir = tmp_path / "outputs"
        source = output_dir / "cmip6" / "handles" / "handles_2026-08-27.jsonl"
        source.parent.mkdir(parents=True)
        source.touch()
        config._set("consumer", "output_dir", str(output_dir))
        publisher_cls.return_value.run.return_value = PublishResult(
            total=1, succeeded=1
        )

        result = runner.invoke(
            cli,
            ["publish", "--project", "CMIP6", "--date", "2026-08-27"],
        )

        assert result.exit_code == 0
        assert publisher_cls.return_value.run.call_args.args[0] == (source,)
        assert publisher_cls.return_value.run.call_args.kwargs["project"] == "CMIP6"

    @pytest.mark.parametrize("date_args", ((), ("--date", "last")))
    @patch("piddiplatsch.commands.publish.HandlePublisher")
    def test_publish_defaults_to_latest_project_file(
        self, publisher_cls, runner, tmp_path, date_args
    ):
        handles_dir = tmp_path / "outputs" / "cmip6" / "handles"
        handles_dir.mkdir(parents=True)
        older = handles_dir / "handles_2026-08-26.jsonl"
        latest = handles_dir / "handles_2026-08-28.jsonl"
        malformed = handles_dir / "handles_latest.jsonl"
        for source in (older, latest, malformed):
            source.touch()
        config._set("consumer", "output_dir", str(tmp_path / "outputs"))
        publisher_cls.return_value.run.return_value = PublishResult(
            total=1, succeeded=1
        )

        result = runner.invoke(cli, ["publish", "--project", "CMIP6", *date_args])

        assert result.exit_code == 0
        assert publisher_cls.return_value.run.call_args.args[0] == (latest,)

    @patch("piddiplatsch.commands.publish.HandlePublisher")
    def test_publish_resolves_relative_date(self, publisher_cls, runner, tmp_path):
        output_dir = tmp_path / "outputs"
        selected_date = date.today() - timedelta(days=1)
        source = (
            output_dir
            / "cmip6"
            / "handles"
            / f"handles_{selected_date.isoformat()}.jsonl"
        )
        source.parent.mkdir(parents=True)
        source.touch()
        config._set("consumer", "output_dir", str(output_dir))
        publisher_cls.return_value.run.return_value = PublishResult(
            total=1, succeeded=1
        )

        result = runner.invoke(
            cli,
            ["publish", "--project", "cmip6", "--date", "yesterday"],
        )

        assert result.exit_code == 0
        assert publisher_cls.return_value.run.call_args.args[0] == (source,)

    @patch("piddiplatsch.commands.publish.HandlePublisher")
    def test_publish_date_requires_project(self, publisher_cls, runner):
        result = runner.invoke(cli, ["publish", "--date", "2026-08-27"])

        assert result.exit_code == 2
        assert "--date requires --project" in result.output
        publisher_cls.assert_not_called()

    @patch("piddiplatsch.commands.publish.HandlePublisher")
    def test_publish_latest_requires_a_dated_handle_file(
        self, publisher_cls, runner, tmp_path
    ):
        config._set("consumer", "output_dir", str(tmp_path / "outputs"))
        result = runner.invoke(cli, ["publish", "--project", "cmip6"])

        assert result.exit_code == 1
        assert "No dated cmip6 Handle files found" in result.output
        publisher_cls.assert_not_called()

    @patch("piddiplatsch.commands.publish.HandlePublisher")
    def test_publish_without_path_or_project_is_rejected(self, publisher_cls, runner):
        result = runner.invoke(cli, ["publish"])

        assert result.exit_code == 2
        assert "Provide PATH or --project" in result.output
        publisher_cls.assert_not_called()

    @patch("piddiplatsch.commands.publish.HandlePublisher")
    def test_publish_rejects_path_and_date(self, publisher_cls, runner, tmp_path):
        source = tmp_path / "handles.jsonl"
        source.touch()

        result = runner.invoke(
            cli,
            [
                "publish",
                str(source),
                "--project",
                "cmip6",
                "--date",
                "2026-08-27",
            ],
        )

        assert result.exit_code == 2
        assert "cannot be combined" in result.output
        publisher_cls.assert_not_called()

    @patch("piddiplatsch.commands.publish.HandlePublisher")
    def test_publish_exits_nonzero_after_failures(
        self, publisher_cls, runner, tmp_path
    ):
        source = tmp_path / "handles.jsonl"
        source.touch()
        publisher_cls.return_value.run.return_value = PublishResult(
            total=3,
            succeeded=2,
            failed=1,
            errors=["handles.jsonl:3: server unavailable"],
        )

        result = runner.invoke(cli, ["publish", str(source)])

        assert result.exit_code == 1
        assert "Published 2/3 handles" in result.output
        assert "server unavailable" in result.output

    @patch("piddiplatsch.monitoring.progress.tqdm")
    @patch("piddiplatsch.commands.publish.HandlePublisher")
    def test_publish_verbose_progress(self, publisher_cls, tqdm_cls, runner, tmp_path):
        source = tmp_path / "handles.jsonl"
        source.touch()

        def run(
            paths,
            limit,
            offset,
            retries,
            retry_delay,
            workers,
            progress_callback,
            project,
            handle_profile,
        ):
            progress_callback(1, 1, "21.TEST/abc", None)
            return PublishResult(total=1, succeeded=1)

        publisher_cls.return_value.run.side_effect = run

        result = runner.invoke(
            cli,
            [
                "--verbose",
                "publish",
                "--project",
                "cmip6",
                "--offset",
                "1000",
                str(source),
            ],
        )

        assert result.exit_code == 0
        assert "Processed handles: 1001-1001" in result.output
        tqdm_cls.assert_called_once_with(
            total=1,
            desc="publish cmip6 handles 1001-1001",
            unit="handle",
            dynamic_ncols=True,
        )
        progress_bar = tqdm_cls.return_value
        progress_bar.update.assert_called_once_with(1)
        progress_bar.close.assert_called_once()

    @patch("piddiplatsch.monitoring.progress.tqdm")
    @patch("piddiplatsch.commands.publish.HandlePublisher")
    def test_publish_silent_skips_progress_bar(
        self, publisher_cls, tqdm_cls, runner, tmp_path
    ):
        source = tmp_path / "handles.jsonl"
        source.touch()

        def run(
            paths,
            limit,
            offset,
            retries,
            retry_delay,
            workers,
            progress_callback,
            project,
            handle_profile,
        ):
            progress_callback(1, 1, "21.TEST/abc", None)
            return PublishResult(total=1, succeeded=1)

        publisher_cls.return_value.run.side_effect = run

        result = runner.invoke(cli, ["--silent", "publish", str(source)])

        assert result.exit_code == 0
        assert "Published 1/1 handles" in result.output
        tqdm_cls.assert_not_called()

    @patch("piddiplatsch.commands.publish.HandlePublisher")
    def test_publish_passes_project_and_prints_project_summary(
        self, publisher_cls, runner, tmp_path
    ):
        source = tmp_path / "handles.jsonl"
        source.touch()
        publisher_cls.return_value.run.return_value = PublishResult(
            total=3,
            succeeded=2,
            failed=1,
            projects={"cmip6": ProjectPublishResult(total=3, succeeded=2, failed=1)},
        )

        result = runner.invoke(
            cli,
            [
                "publish",
                "--project",
                "cmip6",
                "--handle-profile",
                "dkrz-test",
                str(source),
            ],
        )

        assert result.exit_code == 1
        assert publisher_cls.return_value.run.call_args.kwargs["project"] == "cmip6"
        assert (
            publisher_cls.return_value.run.call_args.kwargs["handle_profile"]
            == "dkrz-test"
        )
        assert "cmip6: 2/3 published, 1 failed" in result.output

    @patch("piddiplatsch.commands.publish.HandlePublisher")
    def test_publish_reports_project_mismatch_cleanly(
        self, publisher_cls, runner, tmp_path
    ):
        source = tmp_path / "handles.jsonl"
        source.touch()
        publisher_cls.return_value.run.side_effect = ValueError(
            "Handle batch does not match project 'cmip6'"
        )

        result = runner.invoke(cli, ["publish", "--project", "cmip6", str(source)])

        assert result.exit_code == 1
        assert "does not match project" in result.output
        assert result.exception.__class__.__name__ != "ValueError"

    @patch("piddiplatsch.commands.publish.HandlePublisher")
    def test_publish_passes_limit(self, publisher_cls, runner, tmp_path):
        source = tmp_path / "handles.jsonl"
        source.touch()
        publisher_cls.return_value.run.return_value = PublishResult(
            total=1000, succeeded=1000
        )

        result = runner.invoke(cli, ["publish", "--limit", "1000", str(source)])

        assert result.exit_code == 0
        assert publisher_cls.return_value.run.call_args.kwargs["limit"] == 1000
        assert "Stopped after reaching the limit of 1000 handles" in result.output

    @patch("piddiplatsch.commands.publish.HandlePublisher")
    def test_publish_passes_offset(self, publisher_cls, runner, tmp_path):
        source = tmp_path / "handles.jsonl"
        source.touch()
        publisher_cls.return_value.run.return_value = PublishResult(
            total=1000, succeeded=1000
        )

        result = runner.invoke(
            cli,
            [
                "publish",
                "--offset",
                "1000",
                "--limit",
                "1000",
                str(source),
            ],
        )

        assert result.exit_code == 0
        assert publisher_cls.return_value.run.call_args.kwargs["offset"] == 1000

    @patch("piddiplatsch.commands.publish.HandlePublisher")
    def test_publish_passes_retry_options(self, publisher_cls, runner, tmp_path):
        source = tmp_path / "handles.jsonl"
        source.touch()
        publisher_cls.return_value.run.return_value = PublishResult(
            total=1, succeeded=1, retry_attempts=2
        )

        result = runner.invoke(
            cli,
            [
                "publish",
                "--retries",
                "3",
                "--retry-delay",
                "0.25",
                str(source),
            ],
        )

        assert result.exit_code == 0
        kwargs = publisher_cls.return_value.run.call_args.kwargs
        assert kwargs["retries"] == 3
        assert kwargs["retry_delay"] == 0.25
        assert "Retry attempts: 2" in result.output

    @patch("piddiplatsch.commands.publish.HandlePublisher")
    def test_publish_passes_workers(self, publisher_cls, runner, tmp_path):
        source = tmp_path / "handles.jsonl"
        source.touch()
        publisher_cls.return_value.run.return_value = PublishResult(
            total=1, succeeded=1
        )

        result = runner.invoke(cli, ["publish", "--workers", "4", str(source)])

        assert result.exit_code == 0
        assert publisher_cls.return_value.run.call_args.kwargs["workers"] == 4


class TestCLIOptions:
    """Test global CLI options."""

    @patch("piddiplatsch.consumer.start_consumer")
    def test_debug_flag(self, mock_start_consumer, runner):
        """Test --debug flag."""
        runner.invoke(cli, ["--debug", "harvest"])
        # Debug should configure logging but not affect command execution
        assert mock_start_consumer.called

    @patch("piddiplatsch.cli.config.configure_logging")
    @patch("piddiplatsch.commands.HarvestCommand")
    def test_repeated_verbose_controls_logging(
        self, command_cls, configure_logging, runner
    ):
        result = runner.invoke(cli, ["-vv", "--no-progress", "harvest"])

        assert result.exit_code == 0
        configure_logging.assert_called_once_with(verbosity=2, debug=False, log=None)
        assert command_cls.call_args.kwargs["verbose"] is False

    @patch("piddiplatsch.cli.config.configure_logging")
    @patch("piddiplatsch.commands.HarvestCommand")
    def test_debug_and_silent_remain_aliases(
        self, command_cls, configure_logging, runner
    ):
        result = runner.invoke(cli, ["--debug", "--silent", "harvest"])

        assert result.exit_code == 0
        configure_logging.assert_called_once_with(verbosity=0, debug=True, log=None)
        assert command_cls.call_args.kwargs["verbose"] is False

    @patch("piddiplatsch.consumer.start_consumer")
    def test_log_file_option(self, mock_start_consumer, runner, tmp_path):
        """Test --log option."""
        log_file = tmp_path / "test.log"
        runner.invoke(cli, ["--log", str(log_file), "harvest"])
        assert mock_start_consumer.called

    @patch("piddiplatsch.consumer.start_consumer")
    @patch("piddiplatsch.cli.config.load_config_layers")
    def test_default_config_file(
        self, mock_load_config_layers, mock_start_consumer, runner
    ):
        """Test that the default site and local layers are loaded."""
        result = runner.invoke(cli, ["harvest"])

        assert result.exit_code == 0
        mock_load_config_layers.assert_called_once_with("custom.toml")

    @patch("piddiplatsch.consumer.start_consumer")
    @patch("piddiplatsch.cli.config.load_config_layers")
    def test_config_file_option(
        self, mock_load_config_layers, mock_start_consumer, runner, tmp_path
    ):
        """Test that --config replaces the default local layer."""
        config_file = tmp_path / "custom.toml"
        config_file.write_text('[plugin]\nprocessor = "test"\n')

        result = runner.invoke(cli, ["--config", str(config_file), "harvest"])

        assert result.exit_code == 0
        mock_load_config_layers.assert_called_once_with(str(config_file))
        assert mock_start_consumer.called


class TestConfigCommand:
    def test_show_key_requires_section(self, runner):
        result = runner.invoke(cli, ["config", "show", "--key", "topic"])

        assert result.exit_code == 2
        assert "--key requires --section" in result.output
