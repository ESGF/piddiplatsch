"""SQLite owns queued Handle payloads and progress, independently of exports."""

import hashlib
import json
import shutil
import sqlite3
from contextlib import closing
from copy import deepcopy
from datetime import UTC, datetime

import pytest

from piddiplatsch.catalog.models import CatalogEntry
from piddiplatsch.catalog.outbox import OutboxJournal
from piddiplatsch.catalog.sqlite import SQLiteCatalog
from piddiplatsch.config import config
from piddiplatsch.core.messages import KafkaSource
from piddiplatsch.exceptions import CatalogError
from piddiplatsch.runners.publish import HandlePublisher
from piddiplatsch.runners.sync import SyncWorker


class Backend:
    prefix = "21.TEST"

    def __init__(self):
        self.writes = []

    def add(self, pid, payload):
        self.writes.append((pid, deepcopy(payload)))


@pytest.fixture
def queue(tmp_path):
    config._set("consumer", "projects", ["cmip7"])
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("catalog", "directory", str(tmp_path))
    path = tmp_path / "piddi.db"
    item = CatalogEntry("CMIP7", {"id": "dataset.v1"}, "dataset", 1)
    with SQLiteCatalog("cmip7", path) as catalog:
        with catalog.transaction() as transaction:
            transaction.put_item(item)
            transaction.add_work(item, KafkaSource("topic", 0, 0))
    return path


def payloads():
    return [
        dict(
            handle="21.TEST/same",
            URL="https://example.test",
            project="cmip7",
            data={"VERSION": str(version)},
        )
        for version in (1, 2, 3)
    ]


def metadata(records):
    return dict(
        schema_version=3,
        project="cmip7",
        batch_id="20261102T200000-test",
        status="ready",
        created_at=datetime.now(UTC).isoformat(),
        handle_profile=config.get_handle_profile("cmip7"),
        handle_count=len(records),
        outcomes=[dict(id=1, error=None, handle_count=len(records))],
        item_count=1,
        succeeded_count=1,
        failed_count=0,
        mapping_acknowledged=True,
    )


def enqueue(path, records=None):
    records = payloads() if records is None else records
    manifest = metadata(records)
    with SQLiteCatalog("cmip7", path) as catalog:
        catalog.seal_batch(
            manifest["batch_id"],
            manifest["outcomes"],
            records=records,
            manifest=manifest,
        )
    return manifest


def versions(backend):
    assert all(pid == "same" for pid, _ in backend.writes)
    return [value["VERSION"] for _, value in backend.writes]


def test_mapping_and_payloads_roll_back_together(queue):
    manifest = metadata(payloads())

    def interrupted():
        yield payloads()[0]
        raise OSError("mapping transaction interrupted")

    with SQLiteCatalog("cmip7", queue) as catalog:
        with pytest.raises(OSError):
            catalog.seal_batch(
                manifest["batch_id"],
                manifest["outcomes"],
                records=interrupted(),
                manifest=manifest,
            )
        assert catalog.pending_work().count == 1
        for query in (
            "SELECT COUNT(*) FROM stats",
            "SELECT COUNT(*) FROM outbox_batches",
            "SELECT COUNT(*) FROM handle_outbox",
        ):
            assert catalog._execute(query).fetchone()[0] == 0
        catalog.seal_batch(
            manifest["batch_id"],
            manifest["outcomes"],
            records=payloads(),
            manifest=manifest,
        )
        assert catalog.pending_work().count == 0
        assert catalog._execute("SELECT COUNT(*) FROM handle_outbox").fetchone()[0] == 3


def test_unacknowledged_payloads_cannot_be_marked_published(queue):
    manifest = enqueue(queue)
    with SQLiteCatalog("cmip7", queue) as catalog:
        with pytest.raises(CatalogError, match="unacknowledged"):
            catalog.publish_batch(manifest["batch_id"], 3)


def test_publication_needs_no_batch_files(queue, monkeypatch):
    enqueue(queue)
    backend = Backend()
    worker = SyncWorker(publisher=HandlePublisher(backend))
    # Simulate unavailable optional exports as well as absent manifests/payloads.
    monkeypatch.setattr(worker, "_save_export", lambda *args: None)
    worker.run()
    assert not worker.errors
    assert versions(backend) == ["1", "2", "3"]
    assert not (queue.parent / "batches").exists()
    with SQLiteCatalog("cmip7", queue, read_only=True) as catalog:
        assert catalog._execute("SELECT state FROM work_items").fetchone() == (
            "published",
        )
        assert catalog._execute("SELECT COUNT(*) FROM handle_outbox").fetchone()[0] == 0
        assert catalog._execute(
            "SELECT items,handles FROM stats WHERE kind='batch'"
        ).fetchone() == (1, 3)


@pytest.mark.parametrize("workers", [1, 3])
@pytest.mark.parametrize("chunk_size", [1, 256])
def test_restart_never_replays_older_handle_after_a_newer_write(
    queue, monkeypatch, workers, chunk_size
):
    enqueue(queue)
    monkeypatch.setattr("piddiplatsch.runners.publish.BATCH_SIZE", chunk_size)
    backend = Backend()
    original = OutboxJournal.record

    def interrupt_after_remote_write(self, outcome):
        if outcome.line_number == 2:
            raise OSError("stopped before SQLite acknowledgement")
        return original(self, outcome)

    monkeypatch.setattr(OutboxJournal, "record", interrupt_after_remote_write)
    worker = SyncWorker(publisher=HandlePublisher(backend), workers=workers)
    worker.run()
    assert worker.errors
    assert versions(backend) == ["1", "2"]
    with SQLiteCatalog("cmip7", queue, read_only=True) as catalog:
        assert catalog._execute(
            "SELECT published FROM handle_outbox ORDER BY position"
        ).fetchall() == [(1,), (0,), (0,)]
    # All file state may be lost; committed SQLite progress must be sufficient.
    shutil.rmtree(queue.parent / "batches")
    monkeypatch.setattr(OutboxJournal, "record", original)
    resumed = SyncWorker(publisher=HandlePublisher(backend), workers=workers)
    resumed.run()
    assert not resumed.errors
    assert versions(backend) == ["1", "2", "2", "3"]
    resumed.run()
    assert versions(backend) == ["1", "2", "2", "3"]


def test_completion_failure_rolls_back_work_counters_and_outbox_together(
    queue, monkeypatch
):
    enqueue(queue)
    backend = Backend()
    original = SQLiteCatalog._execute

    def fail_completion(self, sql, parameters=()):
        if (
            sql.startswith("UPDATE outbox_batches SET status=")
            and parameters[0] == "published"
        ):
            raise OSError("completion interrupted")
        return original(self, sql, parameters)

    monkeypatch.setattr(SQLiteCatalog, "_execute", fail_completion)
    worker = SyncWorker(publisher=HandlePublisher(backend))
    worker.run()
    assert worker.errors
    with SQLiteCatalog("cmip7", queue, read_only=True) as catalog:
        assert catalog._execute("SELECT state FROM work_items").fetchone() == (
            "mapped",
        )
        assert catalog._execute(
            "SELECT handles FROM stats WHERE kind='batch'"
        ).fetchone() == (0,)
        assert (
            catalog._execute(
                "SELECT COUNT(*) FROM handle_outbox WHERE published=1"
            ).fetchone()[0]
            == 3
        )
    monkeypatch.setattr(SQLiteCatalog, "_execute", original)
    resumed = SyncWorker(publisher=HandlePublisher(backend))
    resumed.run()
    assert not resumed.errors
    assert versions(backend) == ["1", "2", "3"]
    with SQLiteCatalog("cmip7", queue, read_only=True) as catalog:
        assert catalog._execute("SELECT state FROM work_items").fetchone() == (
            "published",
        )
        assert catalog._execute(
            "SELECT handles FROM stats WHERE kind='batch'"
        ).fetchone() == (3,)
        assert catalog._execute("SELECT COUNT(*) FROM handle_outbox").fetchone()[0] == 0


def legacy_queue(queue, confirmed):
    records = payloads()
    manifest = enqueue(queue, records)
    batch = queue.parent / "batches/cmip7/ready" / manifest["batch_id"]
    batch.mkdir(parents=True)
    output = "".join(json.dumps(row) + "\n" for row in records)
    manifest["sha256"] = hashlib.sha256(output.encode()).hexdigest()
    manifest["failures_sha256"] = hashlib.sha256(b"").hexdigest()
    (batch / "manifest.json").write_text(json.dumps(manifest))
    (batch / "handles.jsonl").write_text(output)
    (batch / "failures.jsonl").touch()
    if confirmed:
        (batch / "publication-progress.jsonl").write_text(
            json.dumps(
                dict(handle="21.TEST/same", line=confirmed, sha256=manifest["sha256"])
            )
            + "\n"
        )
    with closing(sqlite3.connect(queue)) as connection:
        connection.execute("DROP TABLE handle_outbox")
        connection.execute("DROP TABLE outbox_batches")
        connection.execute("PRAGMA user_version=4")
        connection.commit()
    return batch


@pytest.mark.parametrize("confirmed", [0, 1, 2, 3])
def test_version_four_upgrade_imports_legacy_payloads_and_progress(queue, confirmed):
    legacy_queue(queue, confirmed)
    backend = Backend()
    worker = SyncWorker(publisher=HandlePublisher(backend))
    worker.run()
    assert not worker.errors
    assert versions(backend) == [str(value) for value in range(confirmed + 1, 4)]
    with SQLiteCatalog("cmip7", queue, read_only=True) as catalog:
        assert catalog._execute("PRAGMA user_version").fetchone()[0] == 5
        assert catalog._execute("SELECT state FROM work_items").fetchone() == (
            "published",
        )
        assert catalog._execute(
            "SELECT handles FROM stats WHERE kind='batch'"
        ).fetchone() == (3,)


def test_invalid_legacy_progress_does_not_partially_import_or_publish(queue):
    batch = legacy_queue(queue, confirmed=1)
    journal = batch / "publication-progress.jsonl"
    progress = json.loads(journal.read_text())
    progress["handle"] = "21.TEST/not-the-handle-at-this-position"
    journal.write_text(json.dumps(progress) + "\n")
    backend = Backend()
    worker = SyncWorker(publisher=HandlePublisher(backend))
    worker.run()
    assert any("does not match outbox payloads" in error for error in worker.errors)
    assert not backend.writes
    with SQLiteCatalog("cmip7", queue, read_only=True) as catalog:
        assert catalog._execute("SELECT state FROM work_items").fetchone() == (
            "mapped",
        )
        assert (
            catalog._execute("SELECT COUNT(*) FROM outbox_batches").fetchone()[0] == 0
        )
        assert catalog._execute("SELECT COUNT(*) FROM handle_outbox").fetchone()[0] == 0
    assert journal.is_file()


def test_publication_rejects_conflicting_file_selectors_for_database_queue(queue):
    manifest = enqueue(queue)
    with closing(OutboxJournal(queue, "cmip7", manifest["batch_id"])) as journal:
        with pytest.raises(ValueError, match="complete database batch"):
            HandlePublisher(Backend()).run(
                [], limit=1, record_source=journal.records(), journal=journal
            )
