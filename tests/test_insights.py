"""Historical counters must survive sample expiry and avoid phase double counts."""

import datetime
import json
import sqlite3
from contextlib import closing

import pytest
from click.testing import CliRunner

from piddiplatsch.cli import cli
from piddiplatsch.monitoring.insights import read_insights
from piddiplatsch.monitoring.stats import StatusReporter


def test_daily_history_across_midnight_and_repeated_heartbeats(tmp_path, monkeypatch):
    real_datetime = datetime.datetime

    class Clock(real_datetime):
        current = real_datetime(2026, 11, 5, 23, 59, tzinfo=datetime.UTC)

        @classmethod
        def now(cls, tz=None):
            return cls.current

    monkeypatch.setattr(datetime, "datetime", Clock)
    database = tmp_path / "piddi.db"
    summary = dict(
        run=dict(
            run_id="harvest",
            command="harvest",
            status="running",
            started_at=Clock.current.isoformat(),
        ),
        projects={"cmip6": dict(consumed=10, handles=0, failed=1)},
    )
    reporter = StatusReporter(database)
    reporter.heartbeat(summary, force_sample=True)
    reporter.heartbeat(summary, force_sample=True)
    Clock.current = real_datetime(2026, 11, 6, 2, tzinfo=datetime.UTC)
    summary["projects"]["cmip6"].update(consumed=14, failed=2)
    reporter.heartbeat(summary, force_sample=True)
    reporter.close()

    for command, counts in (
        ("map", dict(succeeded=4, handles=12, failed=1)),
        ("publish", dict(consumed=12, handles=11, failed=1)),
        ("batch", dict(consumed=900, handles=900, failed=900)),
    ):
        summary["run"].update(run_id=command, command=command)
        summary["projects"] = {"cmip6": counts}
        with closing(StatusReporter(database)) as reporter:
            reporter.heartbeat(summary)
            reporter.heartbeat(summary)

    history = read_insights(database, days=2, now=Clock.current)
    assert [row["events"] for row in history["daily"]] == [10, 4]
    assert history["totals"] == [
        dict(
            project="cmip6",
            events=14,
            mapped=4,
            prepared_handles=12,
            published_handles=11,
            errors=4,
        )
    ]
    assert not history["warnings"]
    assert (
        read_insights(database, days=1, now=Clock.current)["totals"][0]["events"] == 4
    )
    assert (
        read_insights(database, project="CMIP6", now=Clock.current)["totals"]
        == history["totals"]
    )
    assert read_insights(database, project="cmip7", now=Clock.current)["daily"] == []
    with closing(sqlite3.connect(database)) as connection:
        payload = json.loads(
            connection.execute(
                "SELECT runtime FROM stats WHERE id='harvest'"
            ).fetchone()[0]
        )
    assert (
        len(payload["samples"]) == 1
    )  # Last-hour samples expired; daily history did not.


def test_legacy_history_is_explicitly_labelled(tmp_path):
    database = tmp_path / "piddi.db"
    now = datetime.datetime.now(datetime.UTC)
    with closing(StatusReporter(database)) as reporter:
        reporter.heartbeat(
            dict(
                run=dict(
                    run_id="legacy",
                    command="harvest",
                    status="completed",
                    started_at=now.isoformat(),
                ),
                projects={"cmip7": dict(consumed=20, failed=1)},
            )
        )
    with closing(sqlite3.connect(database)) as connection, connection:
        payload = json.loads(
            connection.execute("SELECT runtime FROM stats").fetchone()[0]
        )
        del payload["daily"]
        connection.execute("UPDATE stats SET runtime=?", (json.dumps(payload),))
    history = read_insights(database)
    assert history["totals"][0]["events"] == 20
    assert "Legacy" in history["warnings"][0]


def test_insights_cli_outputs_and_errors(tmp_path):
    database = tmp_path / "piddi.db"
    with closing(StatusReporter(database)) as reporter:
        reporter.heartbeat(
            dict(
                run=dict(
                    run_id="one",
                    command="map",
                    status="completed",
                    started_at=datetime.datetime.now(datetime.UTC).isoformat(),
                ),
                projects={"cmip7": dict(succeeded=3, handles=9, failed=1)},
            )
        )
    runner = CliRunner()
    args = ["insights", "--db", str(database)]
    result = runner.invoke(cli, [*args, "--json", "--days", "7"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["days"] == 7
    assert json.loads(result.output)["totals"][0]["prepared_handles"] == 9
    for options in ([], ["--summary"]):
        result = runner.invoke(cli, [*args, *options])
        assert result.exit_code == 0, result.output
        assert "cmip7" in result.output
        assert "Errors" in result.output
    result = runner.invoke(cli, [*args, "--project", "absent"])
    assert "No recorded activity" in result.output
    assert runner.invoke(cli, [*args, "--days", "0"]).exit_code == 2
    missing = tmp_path / "missing.db"
    result = runner.invoke(cli, ["insights", "--db", str(missing)])
    assert result.exit_code == 1
    assert "does not exist" in result.output
    assert not missing.exists()
    database.write_text("not sqlite")
    result = runner.invoke(cli, args)
    assert result.exit_code == 1
    assert "not a valid" in result.output


@pytest.mark.parametrize("days", [0, -1])
def test_invalid_history_window(days, tmp_path):
    with pytest.raises(ValueError, match="at least 1"):
        read_insights(tmp_path / "missing.db", days=days)


@pytest.mark.parametrize("retention,expected_days", [(1, 1), (0, 2)])
def test_long_running_history_retention(
    tmp_path, monkeypatch, retention, expected_days
):
    real_datetime = datetime.datetime

    class Clock(real_datetime):
        current = real_datetime(2026, 10, 1, tzinfo=datetime.UTC)

        @classmethod
        def now(cls, tz=None):
            return cls.current

    monkeypatch.setattr(datetime, "datetime", Clock)
    database = tmp_path / "piddi.db"
    summary = dict(
        run=dict(
            run_id="long",
            command="harvest",
            status="running",
            started_at=Clock.current.isoformat(),
        ),
        projects={"cmip6": dict(consumed=10)},
    )
    with closing(StatusReporter(database, sample_retention_days=retention)) as reporter:
        reporter.heartbeat(summary)
        Clock.current = real_datetime(2026, 11, 6, tzinfo=datetime.UTC)
        summary["projects"]["cmip6"]["consumed"] = 15
        reporter.heartbeat(summary)
    result = read_insights(database, days=60, now=Clock.current)
    assert len(result["daily"]) == expected_days
    assert result["daily"][-1]["events"] == 5
