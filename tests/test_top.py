import json
from io import StringIO
from types import SimpleNamespace

import pytest
from click.testing import CliRunner
from rich.console import Console

from piddiplatsch.cli import cli
from piddiplatsch.commands.top import TopCommand
from piddiplatsch.config import config
from piddiplatsch.monitoring.stats import Stats, publication_monitor
from piddiplatsch.monitoring.status import read_status
from piddiplatsch.result import ProcessingResult


def _monitoring_db(path):
    config._set("catalog", "directory", str(path.parent))
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
    config._set("catalog", "directory", str(tmp_path))
    _monitoring_db(db_path)

    result = CliRunner().invoke(
        cli,
        ["--log", str(tmp_path / "test.log"), "top", "--db", str(db_path), "--json"],
    )

    assert result.exit_code == 0
    status = json.loads(result.output)
    assert "storage" in status["health"]
    projects = {row["project"]: row for row in status["runs"][0]["projects"]}
    assert projects["cmip6"]["consumed"] == 1
    assert projects["cmip7"]["consumed"] == 0
    history = {row["project"]: row for row in status["runs"][0]["history"]}
    assert history["cmip6"]["deltas"]["consumed"] == 1


def test_top_once_filters_project(tmp_path):
    db_path = tmp_path / "piddi.db"
    config._set("catalog", "directory", str(tmp_path))
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


def test_top_error_column_counts_finished_and_active_attempts_per_project(tmp_path):
    db = tmp_path / "piddi.db"
    config._set("catalog", "directory", str(tmp_path))
    config._set("stats", "enable_db", True)
    stats = Stats()
    for command, project, errors in (
        ("sync", "cmip6plus", 12),
        ("map", "cmip6plus", 3),
        ("sync", "cmip6", 2),
    ):
        stats.configure_for_run(
            enable_db=True,
            db_path=str(db),
            command=command,
            selected_projects=(project,),
        )
        for number in range(errors):
            stats.record_result(
                ProcessingResult(key=str(number), plugin=project, error="bad item")
            )
        stats.close(status="failed")
    with publication_monitor("cmip6plus"):
        pass  # A finished empty publication must not hide mapping failures.
    stats.configure_for_run(
        enable_db=True,
        db_path=str(db),
        command="sync",
        selected_projects=("cmip6plus",),
    )
    stats.record_result(
        ProcessingResult(key="new", plugin="cmip6plus", error="bad item")
    )
    stats._heartbeat_once(force_sample=True)

    def render(project=None):
        status = read_status(db, project=project)
        status["health"] = {"projects": [], "storage": []}
        stream = StringIO()
        Console(file=stream, width=250).print(TopCommand._overview(status))
        return stream.getvalue()

    text = render()
    assert "Errors" in text
    cmip6plus = next(
        line for line in text.splitlines() if line.lstrip().startswith("cmip6plus ")
    )
    cmip6 = next(
        line for line in text.splitlines() if line.lstrip().startswith("cmip6 ")
    )
    assert "16" in cmip6plus.split()
    assert "2" in cmip6.split()
    stats.close(status="failed")
    text = render("cmip6plus")
    assert "16" in text.split()
    assert "cmip6 " not in text


@pytest.mark.parametrize("mapping_status", ["failed", "completed"])
def test_finished_mapping_errors_survive_empty_publication_and_clear_after_clean_map(
    tmp_path,
    mapping_status,
):
    db = tmp_path / "piddi.db"
    config._set("catalog", "directory", str(tmp_path))
    config._set("stats", "enable_db", True)
    config._set("consumer", "projects", ["cmip6plus"])
    stats = Stats()
    stats.configure_for_run(
        enable_db=True,
        db_path=str(db),
        command="sync",
        selected_projects=("cmip6plus",),
    )
    stats.record_result(
        ProcessingResult(key="bad", plugin="cmip6plus", error="invalid checksum")
    )
    stats.close(status=mapping_status, error_summary="Rejected item")
    with publication_monitor("cmip6plus"):
        pass  # The failure-only batch publishes zero Handles successfully.
    result = CliRunner().invoke(cli, ["top", "--db", str(db), "--once"])
    assert result.exit_code == 0, result.output
    assert "errors recorded" in result.output
    assert "cmip6plus: errors recorded" in result.output

    # Activity clears the old alert after a later successful mapping pass.
    stats.configure_for_run(
        enable_db=True,
        db_path=str(db),
        command="sync",
        selected_projects=("cmip6plus",),
    )
    stats.record_result(
        ProcessingResult(key="good", plugin="cmip6plus", success=True, num_handles=2)
    )
    stats.close(status="completed")
    result = CliRunner().invoke(cli, ["top", "--db", str(db), "--once"])
    assert result.exit_code == 0, result.output
    assert "errors recorded" not in result.output
    assert "· errors" not in result.output


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


def test_top_harvest_reports_durable_writes_and_duplicate_events(tmp_path):
    db_path = tmp_path / "piddi.db"
    config._set("catalog", "directory", str(tmp_path))
    stats = Stats(enable_db=False)
    stats.configure_for_run(
        enable_db=True,
        db_path=str(db_path),
        command="harvest",
        selected_projects=("cmip6",),
        heartbeat_interval_seconds=60,
    )
    stats.record_result(
        ProcessingResult(
            key="write",
            success=True,
            plugin="cmip6",
            catalog_written=True,
            catalog_refreshed=True,
        )
    )
    stats.record_result(
        ProcessingResult(key="duplicate", success=True, plugin="cmip6", filtered=True)
    )
    stats.close(status="completed")
    status = read_status(db_path)
    project = status["runs"][0]["projects"][0]
    assert project["consumed"] == 2
    assert project["catalog_writes"] == project["catalog_refreshes"] == 1
    history = status["runs"][0]["history"][0]
    assert history["deltas"]["catalog_writes"] == 1
    result = CliRunner().invoke(cli, ["top", "--db", str(db_path), "--once"])
    assert result.exit_code == 0, result.output
    assert "cmip6" in result.output
    assert "ΔWrites" not in result.output


def test_top_json_uses_fixed_recent_history(tmp_path):
    db_path = tmp_path / "piddi.db"
    config._set("catalog", "directory", str(tmp_path))
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
            "--json",
        ],
    )

    assert result.exit_code == 0
    status = json.loads(result.output)
    assert status["history_seconds"] == 300
    assert status["runs"][0]["history"]


def test_top_default_is_one_table_without_history(tmp_path):
    db_path = tmp_path / "piddi.db"
    config._set("catalog", "directory", str(tmp_path))
    _monitoring_db(db_path)
    result = CliRunner().invoke(cli, ["top", "--db", str(db_path), "--once"])
    assert result.exit_code == 0, result.output
    assert "Harvest: inactive" in result.output
    assert "Mapped" in result.output
    assert result.output.count("cmip6") == 1
    assert "Active runs" not in result.output
    assert "Publication estimates" not in result.output
    assert "Trend" not in result.output
    assert "--details" not in result.output


def test_stale_runs_are_hidden_only_when_superseded():
    from piddiplatsch.commands.top import _active_runs

    stale = dict(
        command="publish",
        status="running",
        state="stale",
        started_at="2026-10-30T20:00:00+00:00",
        projects=[{"project": "cmip6"}],
    )
    finished = dict(
        stale,
        status="completed",
        state="completed",
        started_at="2026-10-30T20:10:00+00:00",
    )
    assert _active_runs({"runs": [stale, finished]}) == []
    assert _active_runs({"runs": [stale]}) == [stale]
    healthy = dict(stale, state="healthy")
    assert _active_runs({"runs": [healthy, finished]}) == [healthy]
    other_project = dict(finished, projects=[{"project": "cmip7"}])
    assert _active_runs({"runs": [stale, other_project]}) == [stale]


def test_compact_overview_keeps_eta_and_actionable_warnings():
    status = {
        "history_seconds": 1800,
        "health": {
            "storage": [],
            "projects": [
                {"project": "cordex-cmip6", "pending_items": 1000, "failed_batches": 1}
            ],
        },
        "runs": [
            {
                "command": "harvest",
                "status": "running",
                "state": "healthy",
                "started_at": "2026-10-30T20:00:00+00:00",
                "projects": [
                    {"project": "cordex-cmip6", "full_events": 123, "patch_events": 45}
                ],
                "history": [
                    {
                        "project": "cordex-cmip6",
                        "elapsed_seconds": 300,
                        "message_rate": 123.4,
                        "message_rate_series": [0, 50, 100],
                    }
                ],
            },
            {
                "command": "publish",
                "status": "running",
                "state": "healthy",
                "expected_handles": 50000,
                "projects": [{"project": "cordex-cmip6"}],
                "publication": {
                    "published_handles": 12345,
                    "handles_per_second": 227.5,
                    "eta_seconds": 186,
                    "estimated_end": "2026-10-30T21:28:00+00:00",
                },
            },
            {
                "command": "map",
                "status": "running",
                "state": "stale",
                "heartbeat_age_seconds": 100,
                "projects": [{"project": "cmip7"}],
            },
        ],
        "backlog_estimate": {"reason": "waiting for complete counts"},
    }
    output = StringIO()
    Console(file=output, width=200).print(TopCommand._overview(status))
    rendered = output.getvalue()
    assert "cordex-cmip6" in rendered
    assert "Handle totals: since 2026-10-30 20:00 UTC" in rendered
    assert "Rate windows" not in rendered
    assert "(5m avg)" in rendered
    assert "Harvest: running · 123.4 events/s" in rendered
    assert "stored: 123 full / 45 patch" in rendered
    assert "0 events since 2026-10-30 20:00 UTC" in rendered
    assert "Harvest/s" in rendered
    assert "Harvest trend" in rendered
    assert "▁▅█" in rendered
    assert rendered.count("123.4") == 2
    assert "227.5" in rendered
    assert "Batch progress" in rendered
    assert "12,345 / 50,000" in rendered
    assert "3m 6s (21:28 UTC)" in " ".join(rendered.split())
    assert "errors recorded" in rendered
    assert "cmip7: map heartbeat is stale (100s)" in rendered
    assert "waiting for complete counts" not in rendered


def test_harvest_sparkline_covers_whole_history():
    from piddiplatsch.commands.top import _sparkline

    assert _sparkline([]) == "—"
    assert _sparkline([0] * 100) == "▁" * 12
    assert _sparkline([0] * 120 + [100] * 120) == "▁" * 6 + "█" * 6


def test_catalog_and_generated_totals_include_all_projects():
    from piddiplatsch.commands.top import _totals_line

    status = {
        "health": {
            "projects": [
                {"total_items": 800000, "pending_items": 100},
                {"total_items": 200000, "pending_items": 0},
            ]
        },
        "handle_totals": [{"mapped_items": 500000}, {"mapped_items": 250000}],
    }
    assert (
        _totals_line(status).plain
        == "Catalog: 1,000,000 items · Mapped: 750,000 events"
    )
    status["health"]["projects"][1]["total_items"] = None
    assert "Catalog: — items" in _totals_line(status).plain


def test_top_explicit_database_uses_its_own_health(tmp_path, monkeypatch):
    database = tmp_path / "piddi.db"
    _monitoring_db(database)
    config._set("catalog", "directory", str(tmp_path / "unrelated"))
    monkeypatch.setattr(
        "piddiplatsch.monitoring.disk.disk_status",
        lambda: pytest.fail("top must only read its database"),
    )
    result = CliRunner().invoke(
        cli,
        ["--log", str(tmp_path / "test.log"), "top", "--db", str(database), "--json"],
    )
    assert result.exit_code == 0, result.output
    status = json.loads(result.output)
    assert {row["project"] for row in status["health"]["projects"]} == {
        "cmip6",
        "cmip7",
    }
    assert all(not row["errors"] for row in status["health"]["projects"])
    assert not (tmp_path / "unrelated").exists()
