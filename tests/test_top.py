import json
from types import SimpleNamespace
from unittest.mock import patch

from click.testing import CliRunner

from piddiplatsch.cli import cli
from piddiplatsch.config import config
from piddiplatsch.monitoring.stats import Stats
from piddiplatsch.monitoring.status import read_status
from piddiplatsch.result import FeedResult


def _monitoring_db(path):
    stats = Stats(enable_db=False)
    stats.configure_for_run(
        enable_db=True,
        db_path=str(path),
        command="consume",
        topic="publication",
        consumer_group="piddi",
        selected_projects=("cmip6", "cmip7"),
        heartbeat_interval_seconds=60,
    )
    stats.record_result(
        SimpleNamespace(
            key="one",
            plugin="cmip6",
            project="cmip6",
            filtered=False,
            skipped=False,
            success=True,
            num_handles=2,
            handle_processing_time=0,
            error=None,
        )
    )
    stats.close(status="completed")


def test_top_json_is_project_aware(tmp_path):
    db_path = tmp_path / "piddi.db"
    _monitoring_db(db_path)

    result = CliRunner().invoke(
        cli,
        ["--log", str(tmp_path / "test.log"), "top", "--db", str(db_path), "--json"],
    )

    assert result.exit_code == 0
    status = json.loads(result.output)
    projects = {row["project"]: row for row in status["runs"][0]["projects"]}
    assert projects["cmip6"]["consumed"] == 1
    assert projects["cmip7"]["consumed"] == 0
    history = {row["project"]: row for row in status["runs"][0]["history"]}
    assert history["cmip6"]["deltas"]["consumed"] == 1


def test_top_once_filters_project(tmp_path):
    db_path = tmp_path / "piddi.db"
    _monitoring_db(db_path)

    result = CliRunner().invoke(
        cli,
        [
            "--log",
            str(tmp_path / "test.log"),
            "top",
            "--db",
            str(db_path),
            "--project",
            "cmip7",
            "--once",
        ],
    )

    assert result.exit_code == 0
    assert "cmip7" in result.output
    assert "cmip6" not in result.output


def test_top_missing_database_is_clear(tmp_path):
    result = CliRunner().invoke(
        cli,
        [
            "--log",
            str(tmp_path / "test.log"),
            "top",
            "--db",
            str(tmp_path / "missing.db"),
            "--once",
        ],
    )

    assert result.exit_code == 1
    assert "does not exist" in result.output


def test_top_once_shows_history(tmp_path):
    db_path = tmp_path / "piddi.db"
    _monitoring_db(db_path)

    result = CliRunner().invoke(
        cli,
        [
            "--log",
            str(tmp_path / "test.log"),
            "top",
            "--db",
            str(db_path),
            "--once",
            "--history",
            "30",
        ],
    )

    assert result.exit_code == 0
    assert "History · last 30 minutes" in result.output
    assert "ΔMsg" in result.output


@patch("piddiplatsch.commands.map.map_dump_files")
def test_map_command_owns_monitoring_database(mock_map_dump_files, tmp_path):
    source = tmp_path / "dump.jsonl"
    source.write_text("{}\n")
    db_path = tmp_path / "piddi.db"
    config._set("stats", "enable_db", True)
    config._set("stats", "db_path", str(db_path))
    mock_map_dump_files.return_value = FeedResult(total=1, succeeded=1)

    result = CliRunner().invoke(
        cli,
        ["--log", str(tmp_path / "test.log"), "map", str(source), "--project", "cmip6"],
    )

    assert result.exit_code == 0
    status = read_status(db_path)
    assert status["runs"][0]["command"] == "map"
    assert status["runs"][0]["state"] == "completed"


@patch("piddiplatsch.commands.map.map_dump_files")
def test_map_failures_close_monitoring_run_as_failed(mock_map_dump_files, tmp_path):
    source = tmp_path / "dump.jsonl"
    source.write_text("{}\n")
    db_path = tmp_path / "piddi.db"
    config._set("stats", "enable_db", True)
    config._set("stats", "db_path", str(db_path))
    mock_map_dump_files.return_value = FeedResult(total=2, succeeded=1, failed=1)

    result = CliRunner().invoke(
        cli,
        ["--log", str(tmp_path / "test.log"), "map", str(source), "--project", "cmip6"],
    )

    assert result.exit_code == 1
    run = read_status(db_path)["runs"][0]
    assert run["state"] == "failed"
    assert run["error_summary"] == "1 mapping messages failed"
