from copy import deepcopy
from datetime import UTC, datetime, timedelta
from io import StringIO

import pytest
from rich.console import Console

from piddiplatsch.commands.top import TopCommand
from piddiplatsch.monitoring.eta import backlog_estimate, publication_progress

START = datetime(2026, 10, 30, 21, tzinfo=UTC)
NOW = START + timedelta(seconds=60)


def publication():
    run = {
        "command": "publish",
        "status": "running",
        "state": "healthy",
        "batch_id": "current-batch",
        "expected_handles": 100000,
        "heartbeat_at": NOW.isoformat(),
        "projects": [{"project": "cmip6", "handles": 16000, "failed": 0}],
    }
    samples = [
        {"sampled_at": (START - timedelta(seconds=120)).isoformat(), "handles": 0},
        {"sampled_at": START.isoformat(), "handles": 1000},
    ]
    run["publication"] = publication_progress(run, samples, NOW)
    return run, samples


def test_batch_eta_uses_recent_success_speed_and_current_batch_total():
    run, _ = publication()
    estimate = run["publication"]
    assert estimate["handles_per_second"] == 250
    assert estimate["remaining_handles"] == 84000
    assert estimate["eta_seconds"] == 336
    assert datetime.fromisoformat(estimate["estimated_end"]) == NOW + timedelta(
        seconds=336
    )


@pytest.mark.parametrize(
    "condition", ["stale", "failed", "stalled", "warming", "unknown-total"]
)
def test_batch_eta_does_not_invent_an_end_time(condition):
    run, samples = publication()
    if condition == "stale":
        run["state"] = "stale"
    elif condition == "failed":
        run["projects"][0]["failed"] = 1
    elif condition == "stalled":
        run["projects"][0]["handles"] = 1000
    elif condition == "warming":
        samples = [
            {"sampled_at": (NOW - timedelta(seconds=2)).isoformat(), "handles": 15900}
        ]
    else:
        run.pop("expected_handles")
    estimate = publication_progress(run, samples, NOW)
    assert estimate["eta_seconds"] is None
    assert estimate["estimated_end"] is None
    assert estimate["reason"]


def backlog():
    run, _ = publication()
    mapping = {
        "command": "sync",
        "projects": [{"project": "cmip6", "handles": 100000, "succeeded": 10000}],
    }
    return {
        "generated_at": NOW.isoformat(),
        "runs": [run, mapping],
        "health": {
            "projects": [
                {
                    "project": "cmip6",
                    "pending_items": 1000,
                    "ready_handles": 100000,
                    "ready_batch_ids": ["current-batch"],
                    "errors": [],
                }
            ]
        },
    }


def test_backlog_eta_uses_observed_ratio_without_double_counting_active_batch():
    status = backlog()
    # Cumulative counters may contain repeated publications and are irrelevant
    # to remaining work in the catalog and ready batches.
    status["handle_totals"] = [
        {"project": "cmip6", "prepared": 900000, "published": 999999}
    ]
    estimate = backlog_estimate(status)
    assert estimate["approximate"]
    assert estimate["remaining_handles"] == 94000
    assert estimate["eta_seconds"] == 376
    more_pending = deepcopy(status)
    more_pending["health"]["projects"][0]["pending_items"] += 1000
    assert backlog_estimate(more_pending)["remaining_handles"] == 104000


def test_backlog_eta_keeps_known_batch_when_pending_ratio_is_unknown():
    status = backlog()
    status["runs"] = status["runs"][:1]
    estimate = backlog_estimate(status)
    assert estimate["partial"]
    assert estimate["remaining_handles"] == 84000
    assert estimate["eta_seconds"] == 336
    assert estimate["unestimated_projects"] == [
        dict(project="cmip6", pending_events=1000, reason="no mapping history yet")
    ]


def test_manual_publication_with_unknown_size_does_not_show_zero_remaining():
    status = backlog()
    status["health"]["projects"] = []
    status["runs"][0]["publication"]["remaining_handles"] = None
    assert backlog_estimate(status)["eta_seconds"] is None


def test_top_renders_speed_batch_completion_and_known_backlog():
    status = backlog()
    status["health"]["storage"] = []
    status["backlog_estimate"] = backlog_estimate(status)
    stream = StringIO()
    console = Console(file=stream, width=200)
    console.print(TopCommand._overview(status))
    text = stream.getvalue()
    assert "Overall publish ETA: ~6m 16s (2026-10-30 21:07 UTC)" in text
    assert "current backlog only" in text
    assert "250.0" in text
    assert "5m 36s (21:06 UTC)" in text


def test_unmapped_project_does_not_block_known_backlog_eta():
    status = backlog()
    status["health"]["projects"].extend(
        [
            dict(
                project="cordex-cmip6",
                pending_items=36,
                ready_handles=0,
                ready_batch_ids=[],
                errors=[],
            ),
            dict(
                project="cmip7",
                pending_items=0,
                ready_handles=0,
                ready_batch_ids=[],
                errors=[],
            ),
        ]
    )
    estimate = backlog_estimate(status)
    assert estimate["eta_seconds"] == 376
    assert estimate["partial"]
    assert estimate["estimated_projects"] == ["cmip6"]
    assert estimate["unestimated_projects"] == [
        dict(project="cordex-cmip6", pending_events=36, reason="no mapping history yet")
    ]
    status["backlog_estimate"] = estimate
    status["health"]["storage"] = []
    stream = StringIO()
    Console(file=stream, width=240).print(TopCommand._overview(status))
    text = " ".join(stream.getvalue().split())
    assert "Overall publish ETA (partial): ~6m 16s" in text
    assert (
        "Not estimated: cordex-cmip6 (36 pending events; no mapping history yet)"
        in text
    )


def test_eta_uses_retained_batch_totals_after_mapping_run_history_cleanup():
    status = backlog()
    status["runs"] = status["runs"][:1]
    status["handle_totals"] = [
        dict(project="cmip6", prepared=100000, mapped_items=10000, published=0)
    ]
    estimate = backlog_estimate(status)
    assert estimate["remaining_handles"] == 94000
    assert estimate["eta_seconds"] == 376
    assert not estimate["partial"]
    assert estimate["unestimated_projects"] == []


@pytest.mark.parametrize("problem", ["counts", "mapping", "manual-batch"])
def test_total_eta_remains_unavailable_when_no_remaining_work_can_be_estimated(problem):
    status = backlog()
    status["health"]["projects"][0]["ready_handles"] = 0
    status["runs"] = status["runs"][:1]
    if problem == "counts":
        status["health"]["projects"][0]["pending_items"] = None
    elif problem == "manual-batch":
        status["health"]["projects"] = []
        status["runs"][0]["publication"]["remaining_handles"] = None
    estimate = backlog_estimate(status)
    assert estimate["eta_seconds"] is None
    assert estimate["remaining_handles"] is None
    assert estimate["reason"]
    assert estimate["unestimated_projects"]


def test_unknown_project_counts_keep_known_active_batch_progress():
    status = backlog()
    status["health"]["projects"][0]["ready_handles"] = None
    estimate = backlog_estimate(status)
    assert estimate["partial"]
    assert estimate["remaining_handles"] == 84000
    assert estimate["eta_seconds"] == 336
    assert estimate["unestimated_projects"][0]["reason"] == "backlog counts unavailable"


def test_top_shows_reason_when_total_eta_is_not_available():
    from piddiplatsch.commands.top import _backlog_eta_lines

    status = backlog()
    status["runs"] = status["runs"][1:]
    estimate = backlog_estimate(status)
    text = " ".join(line.plain for line in _backlog_eta_lines(estimate))
    assert "Overall publish ETA: unavailable" in text
    assert "waiting for active publication" in text
