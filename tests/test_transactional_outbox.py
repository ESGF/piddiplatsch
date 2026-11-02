"""SQLite owns queued Handle payloads and progress, independently of exports."""

import hashlib
import json
import shutil
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace

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


@pytest.mark.parametrize("failure", [None, "invalid", "commit"])
def test_waiting_acknowledgements_share_a_durable_atomic_commit(
    queue, monkeypatch, failure
):
    records = [dict(payloads()[0], handle=f"21.TEST/{index}") for index in range(4)]
    manifest = enqueue(queue, records)
    first_started = threading.Event()
    release_first = threading.Event()
    group_started = threading.Event()
    release_group = threading.Event()
    groups = []
    statements = []
    with closing(OutboxJournal(queue, "cmip7", manifest["batch_id"])) as journal:
        assert journal.connection.execute("PRAGMA synchronous").fetchone() == (2,)
        journal.connection.set_trace_callback(statements.append)
        original = journal._record_group

        def pause_commit(pending):
            groups.append(len(pending))
            started, release = (
                (first_started, release_first)
                if len(groups) == 1
                else (group_started, release_group)
            )
            started.set()
            assert release.wait(5), "test did not release acknowledgement group"
            return original(pending)

        monkeypatch.setattr(journal, "_record_group", pause_commit)
        outcomes = [
            SimpleNamespace(handle=row["handle"], line_number=index, error=None)
            for index, row in enumerate(records, start=1)
        ]
        if failure == "invalid":
            outcomes[-1].handle = "21.TEST/mismatched"
        with ThreadPoolExecutor(max_workers=4) as executor:
            try:
                first = executor.submit(journal.record, outcomes[0])
                assert first_started.wait(5)
                waiting = [
                    executor.submit(journal.record, outcome) for outcome in outcomes[1:]
                ]
                with journal._acknowledgements:
                    assert journal._acknowledgements.wait_for(
                        lambda: len(journal._pending) == 3, timeout=5
                    )
                assert not first.done()
                assert not any(future.done() for future in waiting)
                release_first.set()
                first.result(timeout=5)
                assert group_started.wait(5)
                assert not any(future.done() for future in waiting)
                with closing(sqlite3.connect(queue)) as reader:
                    assert reader.execute(
                        "SELECT published FROM handle_outbox ORDER BY position"
                    ).fetchall() == [(1,), (0,), (0,), (0,)]
                if failure == "commit":

                    def deny_commit(action, operation, *args):
                        if (
                            action == sqlite3.SQLITE_TRANSACTION
                            and operation == "COMMIT"
                        ):
                            return sqlite3.SQLITE_DENY
                        return sqlite3.SQLITE_OK

                    journal.connection.set_authorizer(deny_commit)
                release_group.set()
                for future in waiting:
                    if failure:
                        exception = (
                            ValueError
                            if failure == "invalid"
                            else sqlite3.DatabaseError
                        )
                        with pytest.raises(exception):
                            future.result(timeout=5)
                    else:
                        future.result(timeout=5)
            finally:
                release_first.set()
                release_group.set()
        assert groups == [1, 3]
        assert sum(sql.startswith("BEGIN") for sql in statements) == 2
        assert not journal.connection.in_transaction
        assert journal.successes == {
            row["handle"]: index
            for index, row in enumerate(records, start=1)
            if index == 1 or failure is None
        }
    # A new process sees exactly the acknowledged prefix after success/failure.
    with closing(OutboxJournal(queue, "cmip7", manifest["batch_id"])) as resumed:
        assert resumed.connection.execute(
            "SELECT published FROM handle_outbox ORDER BY position"
        ).fetchall() == [(1,), *((0,) if failure else (1,) for _ in range(3))]
        assert resumed.status(records[0]["handle"], 1) == "done"
        for index, row in enumerate(records[1:], start=2):
            assert resumed.status(row["handle"], index) == (
                "pending" if failure else "done"
            )
        # A failed group can be retried without losing or skipping any records.
        for index, row in enumerate(records, start=1):
            resumed.record(
                SimpleNamespace(handle=row["handle"], line_number=index, error=None)
            )
        assert resumed.connection.execute(
            "SELECT COUNT(*) FROM handle_outbox WHERE published=1"
        ).fetchone() == (4,)
