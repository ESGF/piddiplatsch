"""Lifecycle activity must stay live across blocking work and close on exit."""

import threading

import pytest

from piddiplatsch.config import config
from piddiplatsch.monitoring.activity import batch_activity
from piddiplatsch.monitoring.status import read_status


def test_activity_heartbeats_continue_without_record_progress(tmp_path, monkeypatch):
    config._set("catalog", "directory", str(tmp_path))
    config._set("stats", "enable_db", True)
    config._set("stats", "heartbeat_interval_seconds", 0.1)
    beat = threading.Event()
    with batch_activity("cmip7") as activity:
        activity.set_phase("sealing")
        original = activity.reporter.heartbeat

        def observe(*args, **kwargs):
            original(*args, **kwargs)
            if threading.current_thread() is activity.thread:
                beat.set()

        monkeypatch.setattr(activity.reporter, "heartbeat", observe)
        assert beat.wait(3), "No heartbeat while record processing was paused"
        status = read_status(tmp_path / "piddi.db")
        run = status["runs"][0]
        assert run["phase"] == "sealing"
        assert run["state"] == "healthy"
        assert run["projects"] == []
        assert status["handle_totals"] == []
    assert not activity.thread.is_alive()
    assert read_status(tmp_path / "piddi.db")["runs"][0]["status"] == "completed"


@pytest.mark.parametrize("error", [RuntimeError("work failed"), KeyboardInterrupt()])
def test_activity_closes_on_errors_and_interrupts(tmp_path, error):
    config._set("catalog", "directory", str(tmp_path))
    config._set("stats", "enable_db", True)
    with pytest.raises(type(error)), batch_activity("cmip7") as activity:
        raise error
    assert not activity.thread.is_alive()
    run = read_status(tmp_path / "piddi.db")["runs"][0]
    assert run["status"] == "failed"
    assert run["finished_at"] is not None
    assert run["error_summary"]


def test_disabled_monitoring_creates_no_database(tmp_path):
    config._set("catalog", "directory", str(tmp_path))
    config._set("stats", "enable_db", False)
    with batch_activity("cmip7") as activity:
        assert activity is None
    assert not (tmp_path / "piddi.db").exists()


def test_unavailable_monitoring_does_not_stop_work(monkeypatch):
    config._set("stats", "enable_db", True)

    def unavailable():
        raise OSError("monitoring unavailable")

    monkeypatch.setattr("piddiplatsch.monitoring.activity.StatusReporter", unavailable)
    with batch_activity("cmip7") as activity:
        assert activity is None
