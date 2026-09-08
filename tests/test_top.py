import json
from types import SimpleNamespace

from click.testing import CliRunner

from piddiplatsch.cli import cli
from piddiplatsch.monitoring.stats import Stats


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
    db_path = tmp_path / "monitor.db"
    _monitoring_db(db_path)

    result = CliRunner().invoke(cli, ["--log", str(tmp_path / "test.log"), "top", "--db", str(db_path), "--json"])

    assert result.exit_code == 0
    status = json.loads(result.output)
    projects = {row["project"]: row for row in status["runs"][0]["projects"]}
    assert projects["cmip6"]["consumed"] == 1
    assert projects["cmip7"]["consumed"] == 0


def test_top_once_filters_project(tmp_path):
    db_path = tmp_path / "monitor.db"
    _monitoring_db(db_path)

    result = CliRunner().invoke(
        cli,
        ["--log", str(tmp_path / "test.log"), "top", "--db", str(db_path), "--project", "cmip7", "--once"],
    )

    assert result.exit_code == 0
    assert "cmip7" in result.output
    assert "cmip6" not in result.output


def test_top_missing_database_is_clear(tmp_path):
    result = CliRunner().invoke(
        cli,
        ["--log", str(tmp_path / "test.log"), "top", "--db", str(tmp_path / "missing.db"), "--once"],
    )

    assert result.exit_code == 1
    assert "does not exist" in result.output
