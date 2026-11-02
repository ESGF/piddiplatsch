import json
from datetime import UTC, datetime, timedelta
from io import StringIO

import pytest
from rich.console import Console

from piddiplatsch.catalog.sqlite import SQLiteCatalog
from piddiplatsch.commands.top import _processing_rate_lines
from piddiplatsch.monitoring.rates import processing_rates
from piddiplatsch.monitoring.status import read_status

NOW = datetime(2026, 11, 2, 21, tzinfo=UTC)


def payload(command, points, project="cmip6", now=NOW):
    samples = [
        dict(
            project=project,
            sampled_at=(now + timedelta(seconds=offset)).isoformat(),
            consumed=count,
            succeeded=count,
            handles=count,
        )
        for offset, count in points
    ]
    return dict(
        run=dict(
            command=command,
            heartbeat_at=samples[-1]["sampled_at"],
            started_at=samples[0]["sampled_at"],
            status="completed",
            selected_projects=[project],
        ),
        samples=samples,
        projects=[samples[-1]],
    )


def test_rates_weight_batches_by_active_time_and_keep_phases_separate():
    row = processing_rates(
        [
            payload("sync", [(-100, 0), (-99, 1000)]),
            payload("map", [(-600, 0), (-500, 100)]),
            payload("publish", [(-200, 0), (-100, 20000)]),
            payload("harvest", [(-200, 0), (-100, 999999)]),
            payload("batch", [(-200, 0), (-100, 999999)]),
            payload("publish", [(-200, 0), (-100, 1000)], project="cmip7"),
        ],
        NOW,
        project="CMIP6",
    )
    assert row["map"]["10m"]["per_second"] == pytest.approx(1100 / 101)
    assert row["map"]["10m"]["active_seconds"] == 101
    assert row["publish"]["10m"]["per_second"] == 200
    assert row["harvest"]["10m"]["per_second"] == 9999.99


def test_each_window_uses_its_own_recent_intervals():
    rows = processing_rates(
        [payload("publish", [(-3600, 0), (-1800, 18000), (-600, 42000), (0, 60000)])],
        NOW,
    )
    rates = rows["publish"]
    assert rates["10m"] == dict(per_second=30, active_seconds=600)
    assert rates["30m"]["per_second"] == pytest.approx(42000 / 1800)
    assert rates["1h"]["per_second"] == pytest.approx(60000 / 3600)
    assert rows["map"]["10m"]["per_second"] is None


def test_partial_window_interpolates_counts_and_includes_current_counters():
    run = payload("publish", [(-900, 0), (-300, 600), (0, 1200)])
    # Last periodic sample is older than the latest heartbeat/counters.
    run["samples"].pop()
    rates = processing_rates([run], NOW)["publish"]
    assert rates["10m"] == dict(per_second=1.5, active_seconds=600)
    assert rates["30m"]["per_second"] == pytest.approx(1200 / 900)


def test_stalls_count_but_no_progress_is_invented_after_a_stale_heartbeat():
    rates = processing_rates([payload("publish", [(-300, 100), (-60, 100)])], NOW)
    assert rates["publish"]["10m"] == dict(per_second=0, active_seconds=240)
    # Unknown mapping speed is distinct from an observed publication stall.
    assert rates["map"]["10m"]["per_second"] is None
    older = processing_rates([payload("publish", [(-900, 0), (-700, 1000)])], NOW)
    assert older["publish"]["10m"]["per_second"] is None
    assert older["publish"]["30m"]["per_second"] == 5


def test_missing_expired_or_reset_samples_do_not_produce_bogus_rates():
    assert (
        processing_rates(
            [
                payload("publish", [(-4000, 0), (-3600, 500)]),
                payload("publish", [(-10, 100)]),
                payload("map", [(-100, 100), (0, 0)]),
            ],
            NOW,
        )
        == {}
    )


def test_status_exposes_rates_without_extending_the_five_minute_history(tmp_path):
    path = tmp_path / "piddi.db"
    recent = payload("map", [(-1200, 0), (-600, 6000)], now=datetime.now(UTC))
    with SQLiteCatalog("cmip6", path) as catalog:
        catalog._execute(
            "INSERT INTO stats(kind,project,id,status,created_at,updated_at,runtime) "
            "VALUES ('run','cmip6','map-test','completed',?,?,?)",
            (
                recent["run"]["started_at"],
                recent["run"]["heartbeat_at"],
                json.dumps(recent),
            ),
        )
    status = read_status(
        path, history_seconds=300, include_health=False, include_table_sizes=False
    )
    assert status["runs"][0]["history"] == []
    assert status["processing_rates"]["map"]["30m"]["per_second"] == 10
    assert (
        read_status(
            path, project="cmip7", include_health=False, include_table_sizes=False
        )["processing_rates"]
        == {}
    )


def test_compact_rate_lines_show_units_missing_data_and_observed_zero():
    rates = processing_rates([payload("publish", [(-60, 0), (0, 0)])], NOW)
    stream = StringIO()
    console = Console(file=stream, width=160)
    for line in _processing_rate_lines(rates):
        console.print(line)
    rendered = stream.getvalue()
    assert "Avg ~ 10m/30m/1h" in rendered
    assert "Harvest —/—/— events/s" in rendered
    assert "Map —/—/— events/s" in rendered
    assert "Publish 0.0/0.0/0.0 handles/s" in rendered
    assert len(_processing_rate_lines(rates)) == 1
    assert "cmip6" not in rendered
    assert _processing_rate_lines({}) == []


def test_combined_meter_weights_projects_by_processing_time():
    rates = processing_rates(
        [
            payload("map", [(-300, 0), (-290, 1000)]),
            payload("map", [(-200, 0), (-100, 1000)], project="cmip7"),
            payload("publish", [(-300, 0), (-290, 2000)]),
            payload("publish", [(-200, 0), (-100, 10000)], project="cmip7"),
        ],
        NOW,
    )
    rendered = "\n".join(line.plain for line in _processing_rate_lines(rates))
    assert "Map 18.2/18.2/18.2 events/s" in rendered
    assert "Publish 109.1/109.1/109.1 handles/s" in rendered
    assert "cmip6" not in rendered
    assert "cmip7" not in rendered


def test_harvest_sums_projects_without_counting_the_shared_clock_twice():
    run = payload("harvest", [(-100, 0), (0, 1000)])
    other = payload("harvest", [(-100, 0), (0, 500)], project="cmip7")
    idle = payload("harvest", [(-100, 0), (0, 0)], project="cmip6plus")
    for extra in (other, idle):
        run["samples"].extend(extra["samples"])
        run["projects"].extend(extra["projects"])
    # Received events include filtered/skipped events, matching live Harvest/s.
    for sample in run["samples"]:
        sample["succeeded"] = 0
    rates = processing_rates([run], NOW)
    assert rates["harvest"]["10m"] == dict(per_second=15, active_seconds=100)
    assert processing_rates([run], NOW, "cmip7")["harvest"]["10m"] == dict(
        per_second=5, active_seconds=100
    )
    assert "Harvest 15.0/15.0/15.0 events/s" in _processing_rate_lines(rates)[0].plain


def test_harvest_averages_across_restarts_by_run_duration():
    rates = processing_rates(
        [
            payload("harvest", [(-300, 0), (-200, 1000)]),
            payload("harvest", [(-10, 0), (0, 1000)], project="cmip7"),
        ],
        NOW,
    )
    assert rates["harvest"]["10m"]["per_second"] == pytest.approx(2000 / 110)
    assert rates["harvest"]["10m"]["active_seconds"] == 110
