"""Unit tests for CLI module."""

from unittest.mock import patch

import pytest
from click.testing import CliRunner

from piddiplatsch.cli import cli
from piddiplatsch.result import PublishResult


@pytest.fixture
def runner():
    """Provides a Click CLI test runner."""
    return CliRunner()


class TestCLIBasics:
    """Test basic CLI functionality."""

    def test_cli_help(self, runner):
        """Test that CLI shows help message."""
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "CLI to interact with Kafka and Handle Service" in result.output
        assert "consume" in result.output
        assert "publish" in result.output
        assert "retry" in result.output

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


class TestConsumeCommand:
    """Test consume command."""

    def test_consume_help(self, runner):
        """Test consume command help."""
        result = runner.invoke(cli, ["consume", "--help"])
        assert result.exit_code == 0
        assert "Start the Kafka consumer" in result.output
        assert "--dump" in result.output
        assert "--dry-run" in result.output
        assert "--project" in result.output
        assert "--all-projects" in result.output

    @patch("piddiplatsch.cli.start_consumer")
    def test_consume_basic(self, mock_start_consumer, runner):
        """Test consume command calls start_consumer."""
        runner.invoke(cli, ["consume"])
        # Will fail without Kafka, but should call start_consumer
        assert mock_start_consumer.called

    @patch("piddiplatsch.cli.start_consumer")
    def test_consume_with_dump(self, mock_start_consumer, runner):
        """Test consume command with --dump flag."""
        runner.invoke(cli, ["consume", "--dump"])
        assert mock_start_consumer.called
        # Check that dump_messages=True was passed
        call_kwargs = mock_start_consumer.call_args.kwargs
        assert call_kwargs.get("dump_messages") is True

    @patch("piddiplatsch.cli.start_consumer")
    def test_consume_with_dry_run(self, mock_start_consumer, runner):
        """Test consume command with --dry-run flag."""
        runner.invoke(cli, ["consume", "--dry-run"])
        assert mock_start_consumer.called
        call_kwargs = mock_start_consumer.call_args.kwargs
        assert call_kwargs.get("dry_run") is True

    @patch("piddiplatsch.cli.start_consumer")
    def test_consume_with_verbose(self, mock_start_consumer, runner):
        """Test consume command with --verbose flag."""
        runner.invoke(cli, ["--verbose", "consume"])
        assert mock_start_consumer.called
        call_kwargs = mock_start_consumer.call_args.kwargs
        assert call_kwargs.get("verbose") is True

    @patch("piddiplatsch.cli.start_consumer")
    def test_consume_with_several_projects(self, mock_start_consumer, runner):
        result = runner.invoke(
            cli,
            ["consume", "--project", "cmip6", "--project", "cmip7"],
        )
        assert result.exit_code == 0
        assert mock_start_consumer.call_args.kwargs["projects"] == ("cmip6", "cmip7")

    @patch("piddiplatsch.cli.start_consumer")
    def test_consume_with_all_projects(self, mock_start_consumer, runner):
        result = runner.invoke(cli, ["consume", "--all-projects"])
        assert result.exit_code == 0
        assert mock_start_consumer.call_args.kwargs["projects"] == "all"

    @patch("piddiplatsch.cli.start_consumer")
    def test_consume_rejects_named_and_all_projects(self, mock_start_consumer, runner):
        result = runner.invoke(
            cli,
            ["consume", "--project", "cmip6", "--all-projects"],
        )
        assert result.exit_code == 2
        assert "cannot be combined" in result.output
        mock_start_consumer.assert_not_called()


class TestRetryCommand:
    """Test retry command."""

    def test_retry_help(self, runner):
        """Test retry command help."""
        result = runner.invoke(cli, ["retry", "--help"])
        assert result.exit_code == 0
        assert "Retry failed items" in result.output
        assert "--delete-after" in result.output
        assert "--dry-run" in result.output

    def test_retry_no_path(self, runner):
        """Test retry command without path argument fails."""
        result = runner.invoke(cli, ["retry"])
        assert result.exit_code == 2
        assert "Missing argument" in result.output

    def test_retry_nonexistent_path(self, runner):
        """Test retry command with nonexistent path fails."""
        result = runner.invoke(cli, ["retry", "nonexistent.jsonl"])
        assert result.exit_code == 2
        assert "does not exist" in result.output

    @patch("piddiplatsch.cli.RetryRunner.run_batch")
    def test_retry_calls_retry_batch(self, mock_run_batch, runner, tmp_path):
        """Test that retry command calls retry.retry_batch."""
        # Create a dummy file
        test_file = tmp_path / "test.jsonl"
        test_file.write_text("{}\n")

        # Mock the return value
        from piddiplatsch.result import RetryResult

        mock_run_batch.return_value = RetryResult(total=0)

        result = runner.invoke(cli, ["retry", str(test_file)])
        assert result.exit_code == 0
        assert mock_run_batch.called
        assert "No retry files found" in result.output

    @patch("piddiplatsch.cli.RetryRunner.run_batch")
    def test_retry_with_success(self, mock_run_batch, runner, tmp_path):
        """Test retry command with successful result."""
        test_file = tmp_path / "test.jsonl"
        test_file.write_text("{}\n")

        from piddiplatsch.result import RetryResult

        mock_run_batch.return_value = RetryResult(
            total=5, succeeded=5, failed=0, failure_files=set()
        )

        result = runner.invoke(cli, ["retry", str(test_file)])
        assert result.exit_code == 0
        assert "5/5 succeeded" in result.output
        assert "All items processed successfully" in result.output

    @patch("piddiplatsch.cli.RetryRunner.run_batch")
    def test_retry_with_failures(self, mock_run_batch, runner, tmp_path):
        """Test retry command with some failures."""
        test_file = tmp_path / "test.jsonl"
        test_file.write_text("{}\n")

        from piddiplatsch.result import RetryResult

        mock_run_batch.return_value = RetryResult(
            total=10, succeeded=7, failed=3, failure_files=set()
        )

        result = runner.invoke(cli, ["retry", str(test_file)])
        assert result.exit_code == 0
        assert "7/10 succeeded" in result.output
        assert "3 items failed again" in result.output
        assert "70.0% success rate" in result.output

    @patch("piddiplatsch.cli.RetryRunner.run_batch")
    def test_retry_with_new_failure_files(self, mock_run_batch, runner, tmp_path):
        """Test retry command shows new failure files."""
        test_file = tmp_path / "test.jsonl"
        test_file.write_text("{}\n")

        # Create a mock failure directory structure
        failures_dir = tmp_path / "failures" / "r1"
        failures_dir.mkdir(parents=True)
        new_failure = failures_dir / "failed_items_2026-01-16.jsonl"
        new_failure.write_text("{}\n")

        from piddiplatsch.result import RetryResult

        mock_run_batch.return_value = RetryResult(
            total=5, succeeded=3, failed=2, failure_files={new_failure}
        )

        from piddiplatsch.config import config

        config._set("consumer", "output_dir", str(tmp_path))
        result = runner.invoke(cli, ["retry", str(test_file)])
        assert result.exit_code == 0
        assert "3/5 succeeded" in result.output
        assert "New failures saved to:" in result.output
        assert "r1/failed_items_2026-01-16.jsonl" in result.output

    @patch("piddiplatsch.cli.RetryRunner")
    def test_retry_passes_delete_after(self, mock_runner_cls, runner, tmp_path):
        """Test retry command passes --delete-after flag."""
        test_file = tmp_path / "test.jsonl"
        test_file.write_text("{}\n")

        from piddiplatsch.result import RetryResult

        # Configure instance to return a dummy result
        instance = mock_runner_cls.return_value
        instance.run_batch.return_value = RetryResult(total=0)

        result = runner.invoke(cli, ["retry", str(test_file), "--delete-after"])
        assert result.exit_code == 0
        # Ensure the class was instantiated with delete_after=True
        init_kwargs = mock_runner_cls.call_args.kwargs
        assert init_kwargs.get("delete_after") is True

    @patch("piddiplatsch.cli.RetryRunner")
    def test_retry_passes_dry_run(self, mock_runner_cls, runner, tmp_path):
        """Test retry command passes --dry-run flag."""
        test_file = tmp_path / "test.jsonl"
        test_file.write_text("{}\n")

        from piddiplatsch.result import RetryResult

        instance = mock_runner_cls.return_value
        instance.run_batch.return_value = RetryResult(total=0)

        result = runner.invoke(cli, ["retry", str(test_file), "--dry-run"])
        assert result.exit_code == 0
        init_kwargs = mock_runner_cls.call_args.kwargs
        assert init_kwargs.get("dry_run") is True

    @patch("piddiplatsch.cli.RetryRunner")
    def test_retry_multiple_paths(self, mock_runner_cls, runner, tmp_path):
        """Test retry command with multiple file paths."""
        file1 = tmp_path / "test1.jsonl"
        file2 = tmp_path / "test2.jsonl"
        file1.write_text("{}\n")
        file2.write_text("{}\n")

        from piddiplatsch.result import RetryResult

        instance = mock_runner_cls.return_value
        instance.run_batch.return_value = RetryResult(total=0)

        result = runner.invoke(cli, ["retry", str(file1), str(file2)])
        assert result.exit_code == 0
        assert instance.run_batch.called
        # Check that paths were passed as tuple to run_batch
        call_args = instance.run_batch.call_args.args
        assert len(call_args[0]) == 2


class TestPublishCommand:
    def test_publish_help(self, runner):
        result = runner.invoke(cli, ["publish", "--help"])

        assert result.exit_code == 0
        assert "Publish prepared handles" in result.output
        assert "--limit" in result.output
        assert "--offset" in result.output
        assert "--retries" in result.output
        assert "--retry-delay" in result.output

    @patch("piddiplatsch.cli.HandlePublisher")
    def test_publish_reports_success(self, publisher_cls, runner, tmp_path):
        source = tmp_path / "handles.jsonl"
        source.touch()
        publisher_cls.return_value.run.return_value = PublishResult(
            total=3, succeeded=3
        )

        result = runner.invoke(cli, ["publish", str(source)])

        assert result.exit_code == 0
        assert "Published 3/3 handles" in result.output
        publisher_cls.return_value.run.assert_called_once()
        assert publisher_cls.return_value.run.call_args.kwargs["limit"] is None
        assert publisher_cls.return_value.run.call_args.kwargs["offset"] == 0
        assert publisher_cls.return_value.run.call_args.kwargs["retries"] == 0
        assert publisher_cls.return_value.run.call_args.kwargs["retry_delay"] == 1.0

    @patch("piddiplatsch.cli.HandlePublisher")
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

    @patch("piddiplatsch.cli.HandlePublisher")
    def test_publish_verbose_progress(self, publisher_cls, runner, tmp_path):
        source = tmp_path / "handles.jsonl"
        source.touch()

        def run(paths, limit, offset, retries, retry_delay, progress_callback):
            progress_callback(1, 1, "21.TEST/abc", None)
            return PublishResult(total=1, succeeded=1)

        publisher_cls.return_value.run.side_effect = run

        result = runner.invoke(
            cli, ["--verbose", "publish", "--offset", "1000", str(source)]
        )

        assert result.exit_code == 0
        assert "starting at 1001" in result.output
        assert "[record 1001 | batch 1/1] 21.TEST/abc: published" in result.output
        assert "Processed record range: 1001-1001" in result.output

    @patch("piddiplatsch.cli.HandlePublisher")
    def test_publish_passes_limit(self, publisher_cls, runner, tmp_path):
        source = tmp_path / "handles.jsonl"
        source.touch()
        publisher_cls.return_value.run.return_value = PublishResult(
            total=1000, succeeded=1000
        )

        result = runner.invoke(cli, ["publish", "--limit", "1000", str(source)])

        assert result.exit_code == 0
        assert publisher_cls.return_value.run.call_args.kwargs["limit"] == 1000
        assert "Stopped after reaching the limit of 1000 records" in result.output

    @patch("piddiplatsch.cli.HandlePublisher")
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

    @patch("piddiplatsch.cli.HandlePublisher")
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


class TestCLIOptions:
    """Test global CLI options."""

    @patch("piddiplatsch.cli.start_consumer")
    def test_debug_flag(self, mock_start_consumer, runner):
        """Test --debug flag."""
        runner.invoke(cli, ["--debug", "consume"])
        # Debug should configure logging but not affect command execution
        assert mock_start_consumer.called

    @patch("piddiplatsch.cli.start_consumer")
    def test_log_file_option(self, mock_start_consumer, runner, tmp_path):
        """Test --log option."""
        log_file = tmp_path / "test.log"
        runner.invoke(cli, ["--log", str(log_file), "consume"])
        assert mock_start_consumer.called

    @patch("piddiplatsch.cli.start_consumer")
    def test_config_file_option(self, mock_start_consumer, runner, tmp_path):
        """Test --config option."""
        config_file = tmp_path / "custom.toml"
        config_file.write_text('[plugin]\nprocessor = "test"\n')

        runner.invoke(cli, ["--config", str(config_file), "consume"])
        assert mock_start_consumer.called
