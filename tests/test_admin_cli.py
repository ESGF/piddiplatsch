from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from piddiplatsch.cli import cli
from piddiplatsch.config import config


def test_dangerous_jobs_are_grouped_in_admin_help():
    root = CliRunner().invoke(cli, ["--help"])
    assert root.exit_code == 0
    commands = {
        line.split()[0]
        for line in root.output.split("Commands:")[1].splitlines()
        if line.strip()
    }
    assert "admin" in commands
    assert not {"reset", "rebuild"} & commands
    admin = CliRunner().invoke(cli, ["admin", "--help"])
    assert admin.exit_code == 0
    assert all(name in admin.output for name in ("reset", "rebuild", "recover"))
    for command in ("reset", "rebuild", "recover"):
        help_result = CliRunner().invoke(cli, ["admin", command, "--help"])
        assert "--apply" in help_result.output


def test_catalog_cleanup_is_only_exposed_by_admin_recover():
    normal = CliRunner().invoke(cli, ["harvest", "--help"])
    admin = CliRunner().invoke(cli, ["admin", "recover", "--help"])
    assert normal.exit_code == admin.exit_code == 0
    assert "--archive" in normal.output
    for option in ("--clean-catalog", "--clean-project"):
        assert option not in normal.output
        assert option in admin.output
        rejected = CliRunner().invoke(cli, ["harvest", option])
        assert rejected.exit_code == 2
        assert "No such option" in rejected.output


@pytest.mark.parametrize("admin", [False, True])
def test_harvest_and_admin_recover_share_workflow_with_safe_defaults(
    monkeypatch, admin
):
    command = MagicMock()
    monkeypatch.setattr("piddiplatsch.commands.HarvestCommand", command)
    args = (
        ["--silent"]
        + (["admin", "recover"] if admin else ["harvest"])
        + ["--limit", "1"]
    )
    if admin:
        args.extend(["--clean-catalog", "--apply"])
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output
    assert command.call_args.kwargs["clean_catalog"] is admin
    assert command.call_args.kwargs["limit"] == 1
    assert command.call_args.kwargs["verbose"] is False
    assert command.call_args.kwargs["preview"] is False
    command.return_value.execute.assert_called_once()


@pytest.mark.parametrize(
    "arguments", [["reset", "--project", "cmip6"], ["rebuild"], ["recover"]]
)
def test_admin_previews_do_not_require_log_file(monkeypatch, arguments):
    configure = MagicMock(side_effect=AssertionError("Preview must not open logs"))
    monkeypatch.setattr("piddiplatsch.config.config.configure_logging", configure)
    result = CliRunner().invoke(cli, ["admin", *arguments])
    assert result.exit_code == 0, result.output
    assert "Preview only" in result.output
    configure.assert_not_called()


def test_recovery_preview_lists_input_and_clean_scope_without_opening_database_or_kafka(
    tmp_path, monkeypatch
):
    root = tmp_path / "live"
    root.mkdir()
    database = root / "piddi.db"
    database.write_bytes(b"existing database must not be opened by preview")
    backup = tmp_path / "backup"
    backup.mkdir()
    part = backup / "dump_messages_2026-11-01_000001.jsonl.gz"
    part.write_bytes(
        b"preview lists the input; it does not scan or change its contents"
    )
    config._set("catalog", "directory", str(root))
    config._set("consumer", "output_dir", str(root))
    processor = MagicMock(
        side_effect=AssertionError("Preview must not create a processor")
    )
    kafka = MagicMock(side_effect=AssertionError("Preview must not connect to Kafka"))
    monkeypatch.setattr("piddiplatsch.commands.harvest.HarvestProcessor", processor)
    monkeypatch.setattr("piddiplatsch.consumer.prepare_kafka_runtime", kafka)
    before = {path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}
    result = CliRunner().invoke(
        cli,
        [
            "admin",
            "recover",
            "--clean-catalog",
            "--archive",
            str(backup),
            "--limit",
            "10",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Preview only" in result.output
    assert "1 saved dump part(s)" in result.output
    assert str(database) in result.output
    assert str(part) in result.output
    assert "Clear catalog/work" in result.output
    assert "Input limit: 10" in result.output
    assert {
        path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()
    } == before
    processor.assert_not_called()
    kafka.assert_not_called()


@pytest.mark.parametrize("apply", [False, True])
def test_apply_does_not_bypass_recovery_validation(tmp_path, apply):
    config._set("catalog", "directory", str(tmp_path))
    arguments = ["admin", "recover", "--clean-catalog", "--clean-project", "cmip6"]
    result = CliRunner().invoke(cli, arguments + (["--apply"] if apply else []))
    assert result.exit_code == 2
    assert "cannot be combined" in result.output
    assert not (tmp_path / "piddi.db").exists()
