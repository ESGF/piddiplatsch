import sqlite3
from threading import Event

import pytest

from piddiplatsch.catalog.sqlite import SQLiteCatalog
from piddiplatsch.monitoring import sampling
from piddiplatsch.monitoring.status import read_status


def test_slow_metrics_publish_incrementally_without_blocking_or_overlapping(
    tmp_path, monkeypatch
):
    db = tmp_path / "piddi.db"
    with SQLiteCatalog("cmip7", db):
        pass
    before = db.read_bytes()
    health_started, release_health = Event(), Event()
    sizes_started, release_sizes = Event(), Event()
    original_health = sampling.database_health
    original_sizes = sampling.estimate_table_sizes
    calls = []
    clock = [0]

    def health(connection, project, *, fast=False):
        calls.append("health")
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            connection.execute("CREATE TABLE forbidden (id INTEGER)")
        health_started.set()
        assert release_health.wait(5)
        return original_health(connection, "cmip7", fast=fast)

    def sizes(connection, health):
        calls.append("sizes")
        sizes_started.set()
        assert release_sizes.wait(5)
        return original_sizes(connection, health)

    monkeypatch.setattr(sampling, "database_health", health)
    monkeypatch.setattr(sampling, "estimate_table_sizes", sizes)
    monkeypatch.setattr(sampling.time, "monotonic", lambda: clock[0])
    metrics = sampling.BackgroundMetrics(db, "cmip7")

    def refresh():
        snapshot = read_status(
            db, project="cmip7", include_health=False, include_table_sizes=False
        )
        metrics.update(snapshot)
        return snapshot

    try:
        first = refresh()
        assert first["metrics"]["sampling"]
        assert health_started.wait(5)
        assert first["health"]["projects"][0]["total_items"] is None
        worker = metrics.worker
        clock[0] = 120  # A slow scan must never cause a second overlapping scan.
        refresh()
        assert metrics.worker is worker
        release_health.set()
        assert sizes_started.wait(5)
        partial = refresh()
        assert partial["health"]["projects"][0]["total_items"] == 0
        assert partial["metrics"]["counts_sampled_at"]
        assert partial["table_sizes"] is None
        release_sizes.set()
        worker.join(5)
        assert not worker.is_alive()
        complete = refresh()
        assert complete["table_sizes"]["available"]
        assert complete["table_sizes"]["tables"]
        assert not complete["metrics"]["sampling"]
        clock[0] += 59
        assert refresh()["table_sizes"] == complete["table_sizes"]
        assert calls == ["health", "sizes"]
        clock[0] += 1
        refreshing = refresh()
        assert refreshing["table_sizes"] == complete["table_sizes"]
        assert refreshing["metrics"]["sampling"]
        assert db.read_bytes() == before
    finally:
        release_health.set()
        release_sizes.set()
        if metrics.worker is not None:
            metrics.worker.join(5)


def test_background_metric_errors_are_visible(tmp_path):
    metrics = sampling.BackgroundMetrics(tmp_path / "missing.db")
    snapshot = dict(health=dict(projects=[]), handle_totals=[])
    metrics.update(snapshot)
    metrics.worker.join(5)
    metrics.update(snapshot)
    assert "unable to open database" in snapshot["metrics"]["error"]
    assert not snapshot["table_sizes"]["available"]
    assert not (tmp_path / "missing.db").exists()
