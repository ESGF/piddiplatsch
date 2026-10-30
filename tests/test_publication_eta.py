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


def test_backlog_eta_waits_until_message_to_handle_ratio_is_known():
    status = backlog()
    status["runs"] = status["runs"][:1]
    assert backlog_estimate(status)["estimated_end"] is None


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
    console = Console(file=stream, width=120)
    console.print(TopCommand._overview(status))
    text = stream.getvalue()
    assert "Known backlog: ~6m 16s" in text
    assert "250.0" in text
    assert "5m 36s (21:06 UTC)" in text
