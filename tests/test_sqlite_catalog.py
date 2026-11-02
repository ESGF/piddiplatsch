"""Persistence, transaction recovery, and indexed reads for SQLite catalogs."""

import sqlite3
from contextlib import closing

import pytest

from piddiplatsch.catalog import CatalogEntry, CatalogError, KafkaCheckpoint
from piddiplatsch.catalog.sqlite import SQLiteCatalog


def entry(version=20260101, *, collection="CMIP6", dataset="dataset", **properties):
    return CatalogEntry(
        collection,
        {
            "id": f"{dataset}.v{version}",
            "collection": collection,
            "properties": {"custom:label": "München", **properties},
            "assets": {"file": {"href": "https://example.test/file"}},
            "links": [],
        },
        dataset,
        version,
    )


@pytest.fixture
def catalog(tmp_path):
    with SQLiteCatalog("cmip6", tmp_path / "piddi.db") as value:
        yield value


def test_persists_complete_items_and_checkpoints_across_reopen(tmp_path):
    path = tmp_path / "nested" / "piddi.db"
    original = entry()
    checkpoint = KafkaCheckpoint("publications", 0, 42)
    with SQLiteCatalog("cmip6", path) as catalog:
        assert catalog.get_item("CMIP6", original.item_id) is None
        assert catalog.get_checkpoint("publications", 0) is None
        with catalog.transaction() as transaction:
            transaction.put_item(original)
            transaction.set_checkpoint(checkpoint)
            assert transaction.get_item("CMIP6", original.item_id) == original
            assert transaction.get_checkpoint("publications", 0) == checkpoint
    with SQLiteCatalog("cmip6", path) as reopened:
        assert reopened.get_item("CMIP6", original.item_id) == original
        assert reopened.get_checkpoint("publications", 0) == checkpoint


def test_failed_transaction_rolls_back_items_and_checkpoints(catalog):
    with (
        pytest.raises(RuntimeError, match="interrupted"),
        catalog.transaction() as transaction,
    ):
        transaction.put_item(entry())
        transaction.set_checkpoint(KafkaCheckpoint("topic", 0, 10))
        raise RuntimeError("interrupted")
    assert catalog.get_item("CMIP6", entry().item_id) is None
    assert catalog.get_checkpoint("topic", 0) is None
    with catalog.transaction() as transaction:
        transaction.put_item(entry())
    assert catalog.get_item("CMIP6", entry().item_id) is not None


def test_replace_updates_index_metadata_and_retains_other_versions(catalog):
    original = entry()
    older = entry(20250101)
    with catalog.transaction() as transaction:
        transaction.put_item(original)
        transaction.put_item(older)
    changed = CatalogEntry(
        "CMIP6", original.item | {"properties": {"updated": True}}, "renamed", 2
    )
    with catalog.transaction() as transaction:
        transaction.put_item(changed)
    assert catalog.get_item("CMIP6", original.item_id) == changed
    assert catalog.find_previous_version("CMIP6", "dataset", 20260102) == older.item_id
    assert catalog.find_previous_version("CMIP6", "renamed", 3) == original.item_id
    assert len(list(catalog.iter_items())) == 2


def test_read_and_write_payloads_are_detached(catalog):
    original = entry()
    with catalog.transaction() as transaction:
        transaction.put_item(original)
        original.item["properties"]["changed"] = True
        loaded = transaction.get_item("CMIP6", original.item_id)
        assert "changed" not in loaded.item["properties"]
        loaded.item["assets"].clear()
        assert transaction.get_item("CMIP6", original.item_id).item["assets"]
    assert catalog.get_item("CMIP6", original.item_id).item["assets"]


def test_previous_version_is_strict_and_collection_dataset_scoped(catalog):
    entries = [entry(v) for v in (1, 3, 5)]
    with catalog.transaction() as transaction:
        for value in [
            *entries,
            entry(4, collection="other"),
            entry(4, dataset="other"),
        ]:
            transaction.put_item(value)
        assert (
            transaction.find_previous_version("CMIP6", "dataset", 5)
            == entries[1].item_id
        )
    assert catalog.find_previous_version("CMIP6", "dataset", 3) == entries[0].item_id
    assert catalog.find_previous_version("CMIP6", "dataset", 1) is None
    assert catalog.find_previous_version("missing", "dataset", 9) is None


def test_checkpoints_are_partition_scoped_and_never_move_backwards(catalog):
    with catalog.transaction() as transaction:
        transaction.set_checkpoint(KafkaCheckpoint("topic", 0, 10))
        transaction.set_checkpoint(KafkaCheckpoint("topic", 1, 3))
        transaction.set_checkpoint(KafkaCheckpoint("other", 0, 2))
        transaction.set_checkpoint(KafkaCheckpoint("topic", 0, 10))
    # Catching an operation failure must not allow partial changes to commit.
    with (
        pytest.raises(CatalogError, match="failed catalog transaction"),
        catalog.transaction() as transaction,
    ):
        transaction.put_item(entry())
        with pytest.raises(CatalogError, match="backwards"):
            transaction.set_checkpoint(KafkaCheckpoint("topic", 0, 9))
    assert catalog.get_item("CMIP6", entry().item_id) is None
    assert catalog.get_checkpoint("topic", 0).next_offset == 10
    assert catalog.get_checkpoint("topic", 1).next_offset == 3
    assert catalog.get_checkpoint("other", 0).next_offset == 2


def test_iterates_in_identity_order_with_collection_filter_and_snapshot(catalog):
    with catalog.transaction() as transaction:
        for value in [
            entry(3, collection="B"),
            entry(2, collection="A"),
            entry(1, collection="A"),
        ]:
            transaction.put_item(value)
    with closing(catalog.iter_items()) as items:
        assert next(items).item_id == entry(1).item_id
        with catalog.transaction() as transaction:
            transaction.put_item(entry(4, collection="A"))
        assert [(value.collection_id, value.version_number) for value in items] == [
            ("A", 2),
            ("B", 3),
        ]
    assert [
        value.version_number for value in catalog.iter_items(collection_id="A")
    ] == [1, 2, 4]
    assert list(catalog.iter_items(collection_id="missing")) == []
    assert not catalog._read_connections


def test_stream_ignores_uncommitted_writes_and_releases_early_reader(catalog):
    with catalog.transaction() as transaction:
        transaction.put_item(entry(1))
    with catalog.transaction() as transaction:
        transaction.put_item(entry(2))
        assert [value.version_number for value in catalog.iter_items()] == [1]
    items = catalog.iter_items()
    assert next(items).version_number == 1
    assert catalog._read_connections
    items.close()
    assert not catalog._read_connections


def test_nested_transactions_and_reset_are_rejected_without_losing_outer_writes(
    catalog,
):
    with catalog.transaction() as transaction:
        transaction.put_item(entry())
        with pytest.raises(CatalogError, match="Nested"), catalog.transaction():
            pass
        with pytest.raises(CatalogError, match="Nested"):
            catalog.reset()
    assert catalog.get_item("CMIP6", entry().item_id) is not None


def test_reset_clears_items_and_checkpoints_and_allows_replay(catalog):
    with catalog.transaction() as transaction:
        transaction.put_item(entry())
        transaction.set_checkpoint(KafkaCheckpoint("topic", 0, 10))
    catalog.reset()
    assert list(catalog.iter_items()) == []
    assert catalog.get_checkpoint("topic", 0) is None
    with catalog.transaction() as transaction:
        transaction.set_checkpoint(KafkaCheckpoint("topic", 0, 0))
    assert catalog.get_checkpoint("topic", 0).next_offset == 0


def test_project_file_ownership_and_schema_version_are_checked(tmp_path):
    path = tmp_path / "piddi.db"
    with SQLiteCatalog("cmip6", path) as catalog:
        with catalog.transaction() as transaction:
            transaction.put_item(entry())
    with SQLiteCatalog("cmip7", path) as catalog:
        assert list(catalog.iter_items()) == []
        assert catalog.get_checkpoint("topic", 0) is None
    with SQLiteCatalog("cmip6", path) as catalog:
        assert catalog.get_item("CMIP6", entry().item_id) is not None
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA user_version=999")
    with pytest.raises(CatalogError, match="schema"):
        SQLiteCatalog("cmip6", path)


def test_close_rolls_back_and_invalidates_transaction_and_reader(tmp_path):
    path = tmp_path / "piddi.db"
    catalog = SQLiteCatalog("cmip6", path)
    with catalog.transaction() as transaction:
        transaction.put_item(entry(1))
    items = catalog.iter_items()
    next(items)
    with (
        pytest.raises(CatalogError, match="closed"),
        catalog.transaction() as transaction,
    ):
        transaction.put_item(entry(2))
        transaction.set_checkpoint(KafkaCheckpoint("topic", 0, 2))
        catalog.close()
    catalog.close()
    with pytest.raises(CatalogError, match="closed"):
        next(items)
    with pytest.raises(CatalogError):
        transaction.get_item("CMIP6", entry(2).item_id)
    with SQLiteCatalog("cmip6", path) as reopened:
        assert reopened.get_item("CMIP6", entry(1).item_id) is not None
        assert reopened.get_item("CMIP6", entry(2).item_id) is None
        assert reopened.get_checkpoint("topic", 0) is None


def test_transaction_handle_cannot_be_reused(catalog):
    with catalog.transaction() as transaction:
        transaction.put_item(entry())
    with catalog.transaction(), pytest.raises(CatalogError, match="no longer active"):
        transaction.put_item(entry(2))


@pytest.mark.parametrize("invalid", [float("nan"), object()])
def test_invalid_json_does_not_allow_checkpoint_commit(catalog, invalid):
    with (
        pytest.raises(CatalogError, match="failed catalog transaction"),
        catalog.transaction() as transaction,
    ):
        transaction.set_checkpoint(KafkaCheckpoint("topic", 0, 10))
        with pytest.raises(CatalogError, match="store catalog item"):
            transaction.put_item(entry(invalid=invalid))
    assert catalog.get_checkpoint("topic", 0) is None


def test_corrupt_payload_is_an_error_but_predecessor_search_does_not_decode(catalog):
    with catalog.transaction() as transaction:
        transaction.put_item(entry(1))
    with sqlite3.connect(catalog.path) as connection:
        connection.execute("UPDATE catalog_items SET stac_blob = ?", (b"corrupt",))
    assert catalog.find_previous_version("CMIP6", "dataset", 2) == entry(1).item_id
    with pytest.raises(CatalogError, match="Invalid stored"):
        catalog.get_item("CMIP6", entry(1).item_id)
    with pytest.raises(CatalogError, match="Invalid stored"):
        list(catalog.iter_items())
    assert not catalog._read_connections


def test_compressed_storage_and_query_indexes(catalog):
    large = entry(repeated="stac-metadata " * 10000)
    with catalog.transaction() as transaction:
        transaction.put_item(large)
    with sqlite3.connect(catalog.path) as connection:
        assert (
            connection.execute(
                "SELECT length(stac_blob) FROM catalog_items"
            ).fetchone()[0]
            < 5000
        )
        exact_plan = connection.execute(
            "EXPLAIN QUERY PLAN SELECT stac_blob FROM catalog_items WHERE project='cmip6' AND collection_id = ? AND item_id = ?",
            ("CMIP6", large.item_id),
        ).fetchall()
        previous_plan = connection.execute(
            "EXPLAIN QUERY PLAN SELECT item_id FROM catalog_items "
            "WHERE project='cmip6' AND collection_id = ? AND dataset_id = ? AND version_number < ? "
            "ORDER BY version_number DESC, item_id DESC LIMIT 1",
            ("CMIP6", "dataset", 20260102),
        ).fetchall()
    assert any("SEARCH" in row[3] for row in exact_plan)
    assert any("INDEX catalog_versions" in row[3] for row in previous_plan)
    assert catalog.get_item("CMIP6", large.item_id) == large


def test_separate_connections_observe_committed_state_only(tmp_path):
    path = tmp_path / "piddi.db"
    with SQLiteCatalog("cmip6", path) as writer, SQLiteCatalog("cmip6", path) as reader:
        with writer.transaction() as transaction:
            transaction.put_item(entry())
            assert reader.get_item("CMIP6", entry().item_id) is None
        assert reader.get_item("CMIP6", entry().item_id) == entry()


def test_lock_failure_raises_catalog_error_and_allows_later_retry(tmp_path):
    path = tmp_path / "piddi.db"
    with (
        SQLiteCatalog("cmip6", path) as first,
        SQLiteCatalog("cmip6", path, timeout=0) as second,
    ):
        with first.transaction():
            with pytest.raises(CatalogError, match="locked"), second.transaction():
                pass
        with second.transaction() as transaction:
            transaction.put_item(entry())
        assert first.get_item("CMIP6", entry().item_id) == entry()


def test_commit_failure_rolls_back_and_catalog_can_be_reused(catalog, monkeypatch):
    execute = catalog._execute

    def fail_commit(sql, parameters=()):
        if sql == "COMMIT":
            raise CatalogError("simulated commit failure")
        return execute(sql, parameters)

    with monkeypatch.context() as patch:
        patch.setattr(catalog, "_execute", fail_commit)
        with (
            pytest.raises(CatalogError, match="commit failure"),
            catalog.transaction() as transaction,
        ):
            transaction.put_item(entry())
            transaction.set_checkpoint(KafkaCheckpoint("topic", 0, 10))
    assert catalog.get_item("CMIP6", entry().item_id) is None
    assert catalog.get_checkpoint("topic", 0) is None
    with catalog.transaction() as transaction:
        transaction.put_item(entry())


def test_reset_failure_preserves_items_and_checkpoints(catalog):
    with catalog.transaction() as transaction:
        transaction.put_item(entry())
        transaction.set_checkpoint(KafkaCheckpoint("topic", 0, 10))
    with sqlite3.connect(catalog.path) as connection:
        connection.execute(
            "CREATE TRIGGER prevent_checkpoint_delete BEFORE DELETE ON kafka_checkpoints "
            "BEGIN SELECT RAISE(ABORT, 'simulated reset failure'); END"
        )
    with pytest.raises(CatalogError, match="reset failure"):
        catalog.reset()
    assert catalog.get_item("CMIP6", entry().item_id) == entry()
    assert catalog.get_checkpoint("topic", 0).next_offset == 10


def test_compaction_reclaims_reset_storage_without_losing_remaining_state(catalog):
    with catalog.transaction() as transaction:
        for version in range(200):
            transaction.put_item(entry(version))
    catalog.reset()
    with catalog.transaction() as transaction:
        transaction.put_item(entry(1))
        transaction.set_checkpoint(KafkaCheckpoint("topic", 0, 1))
    catalog.compact()
    with sqlite3.connect(catalog.path) as connection:
        assert connection.execute("PRAGMA freelist_count").fetchone()[0] == 0
    assert catalog.get_item("CMIP6", entry(1).item_id) == entry(1)
    assert catalog.get_checkpoint("topic", 0).next_offset == 1


def test_compaction_rejects_active_transaction_and_iterator(catalog):
    with catalog.transaction() as transaction:
        transaction.put_item(entry())
        with pytest.raises(CatalogError, match="active"):
            catalog.compact()
    with closing(catalog.iter_items()) as items:
        next(items)
        with pytest.raises(CatalogError, match="active"):
            catalog.compact()
    catalog.compact()


def test_read_only_open_does_not_create_missing_database(tmp_path):
    path = tmp_path / "missing/cmip6.sqlite"
    with pytest.raises(CatalogError):
        SQLiteCatalog("cmip6", path, read_only=True)
    assert not path.parent.exists()


def test_read_only_snapshot_is_shared_by_stream_and_predecessor_queries(tmp_path):
    path = tmp_path / "piddi.db"
    with SQLiteCatalog("cmip6", path) as writer:
        with writer.transaction() as transaction:
            transaction.put_item(entry(1))
            transaction.put_item(entry(3))
        with SQLiteCatalog("cmip6", path, read_only=True) as reader:
            with writer.transaction() as transaction:
                transaction.put_item(entry(2))
            assert [value.version_number for value in reader.iter_items()] == [1, 3]
            assert (
                reader.find_previous_version("CMIP6", "dataset", 3) == entry(1).item_id
            )
            with pytest.raises(CatalogError, match="read-only"), reader.transaction():
                pass
            with pytest.raises(CatalogError, match="read-only"):
                reader.reset()
            with pytest.raises(CatalogError, match="read-only"):
                reader.compact()
        with SQLiteCatalog("cmip6", path, read_only=True) as reader:
            assert (
                reader.find_previous_version("CMIP6", "dataset", 3) == entry(2).item_id
            )


def test_work_snapshots_and_summary_acknowledgement_are_transactional(catalog):
    from piddiplatsch.core.messages import KafkaSource

    first = entry(custom="original")
    with pytest.raises(RuntimeError), catalog.transaction() as transaction:
        transaction.put_item(first)
        transaction.add_work(first, KafkaSource("topic", 0, 0))
        transaction.set_checkpoint(KafkaCheckpoint("topic", 0, 1))
        raise RuntimeError("interrupted")
    assert catalog.pending_work().count == 0
    assert catalog.get_checkpoint("topic", 0) is None
    for offset, record in enumerate((first, entry(custom="updated"))):
        with catalog.transaction() as transaction:
            transaction.put_item(record)
            transaction.add_work(record, KafkaSource("topic", 0, offset))
            transaction.set_checkpoint(KafkaCheckpoint("topic", 0, offset + 1))
    work = list(catalog.iter_pending(10))
    assert [row.entry.item["properties"]["custom"] for row in work] == [
        "original",
        "updated",
    ]
    outcomes = [{"id": work[0].id, "error": None, "handle_count": 2}]
    catalog.seal_batch("batch", outcomes)
    catalog.seal_batch("batch", outcomes)
    assert catalog.pending_work().count == 1
    catalog.publish_batch("batch", 2)
    catalog.publish_batch("batch", 2)
    assert catalog._execute(
        "SELECT items,handles,errors,prepared_handles FROM stats"
    ).fetchone() == (
        1,
        2,
        0,
        2,
    )


def test_fresh_database_has_transactional_outbox_tables(catalog):
    tables = {
        row[0]
        for row in catalog._execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }
    assert tables == {
        "catalog_items",
        "work_items",
        "kafka_checkpoints",
        "stats",
        "outbox_batches",
        "handle_outbox",
    }


def test_batch_stats_reuse_one_row_across_days_retries_and_work_cleanup(
    catalog, monkeypatch
):
    from piddiplatsch.core.messages import KafkaSource
    from piddiplatsch.monitoring.status import read_status

    clock = ["2026-10-01T12:00:00+00:00"]
    monkeypatch.setattr("piddiplatsch.catalog.sqlite._now", lambda: clock[0])
    with catalog.transaction() as transaction:
        for offset in range(3):
            transaction.add_work(
                entry(offset),
                KafkaSource("topic", 0, offset),
                event_type="patch" if offset == 1 else "full",
            )
    first = [
        dict(id=1, error=None, handle_count=3, alias_count=1),
        dict(id=2, error="invalid item"),
    ]
    catalog.seal_batch("first", first)
    catalog.seal_batch("second", [dict(id=3, error=None, handle_count=2)])
    clock[0] = "2026-10-02T12:00:00+00:00"
    catalog.publish_batch("first", 3, error="Service unavailable")
    assert catalog._execute(
        "SELECT status,handles,errors,message FROM stats WHERE id='first'"
    ).fetchone() == ("publication_failed", 0, 2, "Service unavailable")
    clock[0] = "2026-10-03T12:00:00+00:00"
    catalog.publish_batch("first", 3)
    catalog.publish_batch("first", 3)
    catalog.seal_batch("first", first)
    # A stale failed acknowledgement must not undo successful completion.
    catalog.publish_batch("first", 3, error="stale failure")
    assert catalog._execute(
        "SELECT id,status,items,invalid_items,prepared_handles,alias_handles,handles,errors,"
        "full_events,patch_events FROM stats ORDER BY id"
    ).fetchall() == [
        ("first", "published", 1, 1, 3, 1, 3, 2, 1, 1),
        ("second", "ready", 1, 0, 2, 0, 0, 0, 1, 0),
    ]
    assert catalog._execute(
        "SELECT mapped_at,published_at,message FROM stats WHERE id='first'"
    ).fetchone() == ("2026-10-01T12:00:00+00:00", clock[0], None)
    before = read_status(catalog.path)
    with catalog.transaction():
        catalog._execute("DELETE FROM work_items WHERE state='published'")
    after = read_status(catalog.path)
    assert (
        after["handle_totals"]
        == before["handle_totals"]
        == [
            dict(
                project="cmip6",
                mapped_items=2,
                prepared=5,
                published=3,
                errors=2,
                full_events=2,
                patch_events=1,
            )
        ]
    )
    assert after["totals_since"] == "2026-10-01T12:00:00+00:00"
    health = after["health"]["projects"][0]
    assert health["ready_batches"] == 1
    assert health["ready_handles"] == 2
    assert health["ready_batch_ids"] == ["second"]
    assert health["last_publication"] == dict(
        batch_id="first", published_at=clock[0], handle_count=3
    )


@pytest.mark.parametrize(
    "counts", [dict(handle_count=-1), dict(handle_count=2, alias_count=3)]
)
def test_invalid_batch_counts_roll_back_all_work_and_summary(catalog, counts):
    from piddiplatsch.core.messages import KafkaSource

    with catalog.transaction() as transaction:
        for offset in range(2):
            transaction.add_work(entry(offset), KafkaSource("topic", 0, offset))
    with pytest.raises(CatalogError, match="Handle count"):
        catalog.seal_batch(
            "batch",
            [dict(id=1, error=None, handle_count=2), dict(id=2, error=None, **counts)],
        )
    assert catalog.pending_work().count == 2
    assert catalog._execute("SELECT COUNT(*) FROM stats").fetchone() == (0,)
