import sqlite3

from piddiplatsch.catalog import CatalogEntry
from piddiplatsch.catalog.sqlite import SQLiteCatalog
from piddiplatsch.config import config
from piddiplatsch.core.messages import KafkaSource
from piddiplatsch.monitoring.health import database_health, read_health


def seed(path):
    with SQLiteCatalog("cmip7", path) as writer:
        with writer.transaction() as transaction:
            for index in range(5):
                entry = CatalogEntry("CMIP7", {"id": f"item{index}"}, "dataset", index)
                transaction.put_item(entry)
                transaction.add_work(entry, KafkaSource("topic", 0, index))
        writer.seal_batch("ready", [{"id": 1, "error": None, "handle_count": 3}])
        writer.publish_batch("ready", 3, error="Server unavailable")
        writer.seal_batch("done", [{"id": 2, "error": None, "handle_count": 17}])
        writer.publish_batch("done", 17)
        writer.seal_batch("invalid", [{"id": 3, "error": "Invalid item"}])


def test_health_reads_only_database_without_modifying_it(tmp_path):
    config._set("consumer", "output_dir", str(tmp_path))
    catalog = tmp_path / "piddi.db"
    seed(catalog)
    original = catalog.read_bytes()
    # Contradictory and corrupt sidecars cannot affect database health.
    root = tmp_path / "batches/cmip7/ready/fake"
    root.mkdir(parents=True)
    (root / "manifest.json").write_text("broken")
    (root.parent.parent / "last-publication.json").write_text('{"handle_count":999}')

    row = read_health("cmip7")["projects"][0]
    marker = row.pop("last_publication")
    assert marker["handle_count"] == 17
    assert marker["batch_id"] == "done"
    assert row == dict(
        project="cmip7",
        total_items=5,
        pending_items=2,
        pending_full_events=2,
        pending_patch_events=0,
        invalid_items=1,
        ready_batches=1,
        failed_batches=1,
        ready_handles=3,
        ready_batch_ids=["ready"],
        errors=[],
    )
    assert catalog.read_bytes() == original


def test_health_reports_unreadable_database_without_creating_paths(tmp_path):
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("consumer", "projects", ["cmip6", "cmip7"])
    catalog = tmp_path / "piddi.db"
    catalog.write_text("not a database")
    rows = read_health()["projects"]
    assert rows[0]["pending_items"] is None
    assert rows[0]["total_items"] is None
    assert "Catalog status unavailable" in rows[0]["errors"][0]
    assert rows[1]["project"] == "cmip7"
    assert not (tmp_path / "batches").exists()
    assert catalog.read_text() == "not a database"


def test_health_preserves_last_publication_after_work_retention(tmp_path):
    config._set("consumer", "output_dir", str(tmp_path))
    database = tmp_path / "piddi.db"
    seed(database)
    with sqlite3.connect(database) as connection:
        connection.execute("DELETE FROM work_items WHERE state='published'")
    row = read_health("cmip7")["projects"][0]
    assert row["ready_batches"] == row["failed_batches"] == 1
    assert row["last_publication"]["handle_count"] == 17


def test_fast_health_counts_via_covering_indexes_without_payload_or_event_reads(
    tmp_path,
):
    database = tmp_path / "piddi.db"
    seed(database)
    with sqlite3.connect(database) as connection:
        expected = database_health(connection)

        def authorize(action, table, column, *args):
            if (
                action == sqlite3.SQLITE_READ
                and table in {"catalog_items", "work_items"}
                and column in {"stac_blob", "event_type"}
            ):
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        connection.set_authorizer(authorize)
        fast = database_health(connection, fast=True)
        assert fast["projects"][0]["work_items"] == 5
        assert fast["projects"][0]["pending_full_events"] is None
        for key in (
            "total_items",
            "pending_items",
            "invalid_items",
            "ready_handles",
            "last_publication",
        ):
            assert fast["projects"][0][key] == expected["projects"][0][key]
        for query in (
            "SELECT COUNT(*) FROM catalog_items INDEXED BY catalog_versions WHERE project='cmip7'",
            "SELECT state,COUNT(*) FROM work_items INDEXED BY work_selection WHERE project='cmip7' GROUP BY state",
        ):
            plan = connection.execute("EXPLAIN QUERY PLAN " + query).fetchall()
            assert any("COVERING INDEX" in row[3] for row in plan)
