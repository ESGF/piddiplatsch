from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from piddiplatsch.cli import cli


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
        args.append("--clean-catalog")
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output
    assert command.call_args.kwargs["clean_catalog"] is admin
    assert command.call_args.kwargs["limit"] == 1
    assert command.call_args.kwargs["verbose"] is False
    command.return_value.execute.assert_called_once()


@pytest.mark.parametrize("arguments", [["reset", "--project", "cmip6"], ["rebuild"]])
def test_admin_previews_do_not_require_log_file(monkeypatch, arguments):
    configure = MagicMock(side_effect=AssertionError("Preview must not open logs"))
    monkeypatch.setattr("piddiplatsch.config.config.configure_logging", configure)
    result = CliRunner().invoke(cli, ["admin", *arguments])
    assert result.exit_code == 0, result.output
    assert "Preview only" in result.output
    configure.assert_not_called()
