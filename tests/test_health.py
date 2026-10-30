import json
import sqlite3

from piddiplatsch.config import config
from piddiplatsch.monitoring.health import read_health


def test_health_reads_backlog_failed_work_and_last_success_without_migration(tmp_path):
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("consumer", "projects", ["cmip7"])
    catalog = tmp_path / "catalog/cmip7.sqlite"
    catalog.parent.mkdir()
    with sqlite3.connect(catalog) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        connection.executemany("INSERT INTO items VALUES (?)", [(1,), (2,), (3,)])
        connection.execute("CREATE TABLE pending_items (id INTEGER PRIMARY KEY)")
        connection.executemany("INSERT INTO pending_items VALUES (?)", [(1,), (2,)])
    original = catalog.read_bytes()
    root = tmp_path / "batches/cmip7"
    (root / "ready/batch-one").mkdir(parents=True)
    (root / "ready/batch-one/manifest.json").write_text('{"status":"ready"}')
    failed = root / ".building-failed/manifest.json"
    failed.parent.mkdir()
    failed.write_text('{"status":"failed"}')
    marker = {"published_at": "2026-10-30T19:00:00+00:00", "handle_count": 17}
    (root / "last-publication.json").write_text(json.dumps(marker))

    health = read_health("cmip7")

    assert health["projects"] == [
        {
            "project": "cmip7",
            "total_items": 3,
            "pending_items": 2,
            "ready_batches": 1,
            "failed_batches": 1,
            "last_publication": marker,
            "ready_handles": None,
            "ready_batch_ids": ["batch-one"],
            "errors": [],
        }
    ]
    assert catalog.read_bytes() == original
    assert health["storage"][0]["free_bytes"] > 0


def test_health_reports_old_or_unreadable_catalog_without_creating_missing_paths(
    tmp_path,
):
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("consumer", "projects", ["cmip6", "cmip7"])
    catalog = tmp_path / "catalog/cmip6.sqlite"
    catalog.parent.mkdir()
    catalog.write_text("not a database")

    rows = read_health()["projects"]

    assert rows[0]["pending_items"] is None
    assert rows[0]["total_items"] is None
    assert "Catalog status unavailable" in rows[0]["errors"][0]
    assert rows[1]["project"] == "cmip7"
    assert not (tmp_path / "batches").exists()
    assert not (tmp_path / "catalog/cmip7.sqlite").exists()


def test_health_counts_publication_failure_as_ready_and_failed(tmp_path):
    config._set("consumer", "output_dir", str(tmp_path))
    manifest = tmp_path / "batches/cmip7/ready/failed-publication/manifest.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text('{"status":"ready","publication_error":"Server unavailable"}')
    row = read_health("cmip7")["projects"][0]
    assert row["ready_batches"] == row["failed_batches"] == 1
    assert row["last_publication"] is None
