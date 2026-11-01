"""Project-scoped access to the four-table, shared SQLite work database."""

import json
import sqlite3
import zlib
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

from piddiplatsch.catalog.base import BaseCatalog, BaseCatalogTransaction
from piddiplatsch.catalog.models import (
    CatalogEntry,
    KafkaCheckpoint,
    PendingWork,
    WorkItem,
)
from piddiplatsch.exceptions import CatalogError

_SCHEMA_VERSION = 2
_SCHEMA = """
CREATE TABLE IF NOT EXISTS catalog_items (
    project TEXT NOT NULL, collection_id TEXT NOT NULL, item_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL, version_number INTEGER NOT NULL,
    stac_blob BLOB NOT NULL, last_update TEXT NOT NULL,
    PRIMARY KEY (project, collection_id, item_id)
);
CREATE INDEX IF NOT EXISTS catalog_versions
    ON catalog_items(project, collection_id, dataset_id, version_number);
CREATE TABLE IF NOT EXISTS work_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project TEXT NOT NULL, topic TEXT NOT NULL, partition INTEGER NOT NULL,
    offset INTEGER NOT NULL, stac_blob BLOB NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','mapped','invalid','published')),
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    batch_id TEXT, message TEXT, handle_count INTEGER NOT NULL DEFAULT 0,
    UNIQUE(topic, partition, offset)
);
CREATE INDEX IF NOT EXISTS work_selection ON work_items(project, state, id);
CREATE INDEX IF NOT EXISTS work_batches ON work_items(project, batch_id);
CREATE TABLE IF NOT EXISTS kafka_checkpoints (
    topic TEXT NOT NULL, partition INTEGER NOT NULL, next_offset INTEGER NOT NULL,
    PRIMARY KEY(topic, partition)
);
CREATE TABLE IF NOT EXISTS stats (
    date TEXT NOT NULL, project TEXT NOT NULL,
    items INTEGER NOT NULL DEFAULT 0, handles INTEGER NOT NULL DEFAULT 0,
    errors INTEGER NOT NULL DEFAULT 0, prepared_handles INTEGER NOT NULL DEFAULT 0,
    runtime TEXT NOT NULL DEFAULT '{}', last_publication TEXT, PRIMARY KEY(date, project)
);
"""


def _now():
    return datetime.now(UTC).isoformat()


def _encode(entry):
    # A snapshot envelope keeps local provenance out of the original STAC item.
    entry.__post_init__()
    return zlib.compress(
        json.dumps(
            {
                "collection_id": entry.collection_id,
                "item": entry.item,
                "dataset_id": entry.dataset_id,
                "version_number": entry.version_number,
                "publication_time": entry.publication_time,
                "remote_refresh": entry.remote_refresh,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode(),
        3,
    )


def _decode(blob):
    try:
        return CatalogEntry(**json.loads(zlib.decompress(blob)))
    except (ValueError, TypeError, zlib.error) as exc:
        raise CatalogError(f"Invalid stored STAC snapshot: {exc}") from exc


class SQLiteCatalog(BaseCatalog):
    """One project view of piddi.db. Connections belong to one thread.

    Read-only instances hold a stable snapshot. Writers use short explicit
    transactions. No legacy schema migration is performed.
    """

    def __init__(self, project, path, *, timeout=5.0, read_only=False):
        super().__init__(project)
        self.path = Path(path).resolve()
        self.read_only = read_only
        self._connection = None
        self._active_transaction = None
        self._read_connections = set()
        try:
            if read_only:
                self._connection = sqlite3.connect(
                    self.path.as_uri() + "?mode=ro",
                    uri=True,
                    timeout=timeout,
                    isolation_level=None,
                )
                self._execute("BEGIN")
                if (
                    self._execute("PRAGMA user_version").fetchone()[0]
                    != _SCHEMA_VERSION
                ):
                    raise CatalogError(
                        "Unsupported database schema; start with a fresh piddi.db"
                    )
                self._execute("SELECT project FROM catalog_items LIMIT 0")
            else:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                self._connection = sqlite3.connect(
                    self.path, timeout=timeout, isolation_level=None
                )
                tables = {
                    row[0]
                    for row in self._execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                if (
                    tables
                    and not {
                        "catalog_items",
                        "work_items",
                        "kafka_checkpoints",
                        "stats",
                    }
                    <= tables
                ):
                    raise CatalogError(
                        "Legacy database detected; start with a fresh piddi.db"
                    )
                version = self._execute("PRAGMA user_version").fetchone()[0]
                if version not in (0, _SCHEMA_VERSION):
                    raise CatalogError("Unsupported database schema")
                self._execute("PRAGMA journal_mode=WAL")
                self._execute("PRAGMA synchronous=FULL")
                self._require_connection().executescript(
                    "BEGIN IMMEDIATE;\n" + _SCHEMA + "\nPRAGMA user_version=2;\nCOMMIT;"
                )
        except (OSError, sqlite3.Error, CatalogError) as exc:
            self.close()
            raise CatalogError(f"Could not open database {self.path}: {exc}") from exc

    def _require_connection(self):
        if self._connection is None:
            raise CatalogError("Catalog is closed")
        return self._connection

    def _execute(self, sql, parameters=()):
        try:
            return self._require_connection().execute(sql, parameters)
        except (sqlite3.Error, OverflowError) as exc:
            if self._active_transaction is not None:
                self._active_transaction._failed = True
            raise CatalogError(f"SQLite operation failed: {exc}") from exc

    @contextmanager
    def transaction(self):
        if self.read_only:
            raise CatalogError("Catalog is read-only")
        if self._active_transaction is not None:
            raise CatalogError("Nested catalog transactions are unsupported")
        self._execute("BEGIN IMMEDIATE")
        transaction = SQLiteCatalogTransaction(self)
        self._active_transaction = transaction
        try:
            yield transaction
            transaction._require_active()
            if transaction._failed:
                raise CatalogError("Cannot commit a failed catalog transaction")
            self._execute("COMMIT")
        except BaseException:
            if self._connection is not None and self._connection.in_transaction:
                self._execute("ROLLBACK")
            raise
        finally:
            self._active_transaction = None

    def get_item(self, collection_id, item_id):
        row = self._execute(
            "SELECT stac_blob FROM catalog_items WHERE project=? AND collection_id=? AND item_id=?",
            (self.project, collection_id, item_id),
        ).fetchone()
        return _decode(row[0]) if row else None

    def find_previous_version(self, collection_id, dataset_id, version_number):
        row = self._execute(
            "SELECT item_id FROM catalog_items WHERE project=? AND collection_id=? AND dataset_id=? "
            "AND version_number<? ORDER BY version_number DESC, item_id DESC LIMIT 1",
            (self.project, collection_id, dataset_id, version_number),
        ).fetchone()
        return row[0] if row else None

    def find_versions(self, collection_id, dataset_id, limit=100):
        if type(limit) is not int or limit < 1:
            raise ValueError("limit must be a positive integer")
        return [
            row[0]
            for row in self._execute(
                "SELECT item_id FROM catalog_items WHERE project=? AND collection_id=? "
                "AND dataset_id=? ORDER BY version_number DESC, item_id DESC LIMIT ?",
                (self.project, collection_id, dataset_id, limit),
            )
        ]

    def get_checkpoint(self, topic, partition):
        row = self._execute(
            "SELECT topic, partition, next_offset FROM kafka_checkpoints WHERE topic=? AND partition=?",
            (topic, partition),
        ).fetchone()
        return KafkaCheckpoint(*row) if row else None

    def latest_event_date(self):
        # No source-date columns: the normal queue uses local insertion order.
        return max(
            (entry.event_date for entry in self.iter_items() if entry.event_date),
            default=None,
        )

    def iter_items(
        self,
        *,
        collection_id=None,
        from_date=None,
        to_date=None,
        remote_refresh_only=False,
    ):
        sql = "SELECT stac_blob FROM catalog_items WHERE project=?"
        params = [self.project]
        if collection_id is not None:
            sql += " AND collection_id=?"
            params.append(collection_id)
        reader = (
            self._require_connection()
            if self.read_only
            else sqlite3.connect(
                self.path.as_uri() + "?mode=ro", uri=True, isolation_level=None
            )
        )
        cursor = None
        try:
            if not self.read_only:
                self._read_connections.add(reader)
                reader.execute("BEGIN")
            cursor = reader.execute(sql + " ORDER BY collection_id, item_id", params)
            for row in cursor:
                self._require_connection()
                entry = _decode(row[0])
                if remote_refresh_only and entry.remote_refresh is None:
                    continue
                if from_date is not None and (
                    entry.event_date is None or entry.event_date < from_date
                ):
                    continue
                if to_date is not None and (
                    entry.event_date is None or entry.event_date > to_date
                ):
                    continue
                yield entry
        except sqlite3.Error as exc:
            raise CatalogError(f"Could not stream catalog: {exc}") from exc
        finally:
            if self.read_only:
                if self._connection is not None and cursor is not None:
                    cursor.close()
            else:
                self._read_connections.discard(reader)
                reader.close()

    def pending_work(self, max_id=None):
        frontier = self._execute(
            "SELECT COALESCE(MAX(id),0) FROM work_items WHERE project=?",
            (self.project,),
        ).fetchone()[0]
        count, oldest = self._execute(
            "SELECT COUNT(*), MIN(created_at) FROM work_items WHERE project=? AND state='pending' AND id<=?",
            (self.project, frontier if max_id is None else max_id),
        ).fetchone()
        return PendingWork(
            count,
            datetime.fromisoformat(oldest) if oldest else None,
            frontier,
        )

    def _work_selection(self, frontier, from_date, to_date, after_id):
        sql = "FROM work_items WHERE project=? AND state='pending' AND id>? AND id<=?"
        params = [self.project, after_id, frontier]
        if from_date is not None:
            sql += " AND created_at>=?"
            params.append(
                datetime.combine(from_date, datetime.min.time(), UTC).isoformat()
            )
        if to_date is not None:
            sql += " AND created_at<?"
            params.append(
                datetime.combine(
                    to_date + timedelta(days=1), datetime.min.time(), UTC
                ).isoformat()
            )
        return sql, params

    def iter_pending(
        self, limit, max_id=None, *, from_date=None, to_date=None, offset=0
    ):
        frontier = self.pending_work().last_id if max_id is None else max_id
        for identity in self.select_work_ids(
            limit, frontier, from_date=from_date, to_date=to_date, offset=offset
        ):
            yield self.get_work(identity)

    def select_work_ids(
        self, limit, frontier, *, from_date=None, to_date=None, offset=0, after_id=0
    ):
        if type(limit) is not int or limit < 1:
            raise ValueError("limit must be a positive integer")
        sql, params = self._work_selection(frontier, from_date, to_date, after_id)
        return [
            row[0]
            for row in self._execute(
                "SELECT id " + sql + " ORDER BY id LIMIT ? OFFSET ?",
                (*params, limit, offset),
            )
        ]

    def skip_work(self, limit, frontier, *, from_date=None, to_date=None):
        sql, params = self._work_selection(frontier, from_date, to_date, 0)
        # Aggregate in SQLite so even a large offset uses bounded Python memory.
        return self._execute(
            "SELECT COUNT(*), COALESCE(MAX(id),0) FROM (SELECT id "  # noqa: S608 - only fixed internal SQL fragments
            + sql
            + " ORDER BY id LIMIT ?)",
            (*params, limit),
        ).fetchone()

    def get_work(self, identity):
        row = self._execute(
            "SELECT stac_blob FROM work_items WHERE project=? AND id=?",
            (self.project, identity),
        ).fetchone()
        if row is None:
            raise CatalogError(f"Work entry {identity} is missing")
        return WorkItem(_decode(row[0]), identity)

    def _summary(
        self, items=0, handles=0, errors=0, prepared_handles=0, last_publication=None
    ):
        self._execute(
            "INSERT INTO stats(date,project,items,handles,errors,prepared_handles,last_publication) "
            "VALUES (?,?,?,?,?,?,?) ON CONFLICT(date,project) DO UPDATE SET "
            "items=items+excluded.items, handles=handles+excluded.handles, errors=errors+excluded.errors, "
            "prepared_handles=prepared_handles+excluded.prepared_handles, "
            "last_publication=COALESCE(excluded.last_publication,stats.last_publication)",
            (
                datetime.now(UTC).date().isoformat(),
                self.project,
                items,
                handles,
                errors,
                prepared_handles,
                last_publication,
            ),
        )

    def seal_batch(self, batch_id, outcomes):
        """Idempotent DB acknowledgement after the immutable files are durable."""
        with self.transaction():
            for outcome in outcomes:
                identity, error = outcome["id"], outcome.get("error")
                row = self._execute(
                    "SELECT state,batch_id FROM work_items WHERE project=? AND id=?",
                    (self.project, identity),
                ).fetchone()
                desired = "invalid" if error is not None else "mapped"
                if row is None:
                    raise CatalogError(
                        "Batch source work is missing; refusing stale batch"
                    )
                if row == (desired, batch_id) or row == ("published", batch_id):
                    continue
                if row != ("pending", None):
                    raise CatalogError("Work already belongs to another batch or state")
                handles = outcome.get("handle_count") if error is None else 0
                if type(handles) is not int or handles < 0:
                    raise CatalogError("Missing or invalid prepared Handle count")
                self._execute(
                    "UPDATE work_items SET state=?,batch_id=?,message=?,updated_at=?,handle_count=? WHERE id=?",
                    (
                        desired,
                        batch_id,
                        error,
                        _now(),
                        handles,
                        identity,
                    ),
                )
                self._summary(
                    items=int(error is None),
                    errors=int(error is not None),
                    prepared_handles=handles,
                )

    def publish_batch(self, batch_id, handle_count, *, error=None):
        with self.transaction():
            count = self._execute(
                "SELECT COUNT(*) FROM work_items WHERE project=? AND batch_id=? AND state='mapped'",
                (self.project, batch_id),
            ).fetchone()[0]
            if error is not None:
                self._execute(
                    "UPDATE work_items SET message=? WHERE project=? AND batch_id=? AND state='mapped'",
                    (error, self.project, batch_id),
                )
                self._summary(errors=1)
            elif count:
                self._execute(
                    "UPDATE work_items SET state='published',message=NULL,updated_at=? "
                    "WHERE project=? AND batch_id=? AND state='mapped'",
                    (_now(), self.project, batch_id),
                )
                self._summary(
                    handles=handle_count,
                    last_publication=json.dumps(
                        dict(
                            batch_id=batch_id,
                            published_at=_now(),
                            handle_count=handle_count,
                        )
                    ),
                )

    def retry_invalid(self, identities):
        with self.transaction():
            for identity in identities:
                row = self._execute(
                    "SELECT state FROM work_items WHERE project=? AND id=?",
                    (self.project, identity),
                ).fetchone()
                if row != ("invalid",):
                    raise CatalogError(f"Work entry {identity} is not invalid")
                if self._execute(
                    "SELECT 1 FROM stats WHERE project=? AND handles>0 LIMIT 1",
                    (self.project,),
                ).fetchone():
                    raise CatalogError(
                        "This project has published output; harvest a correction or reset the complete project for ordered replay"
                    )
                if self._execute(
                    "SELECT 1 FROM work_items WHERE project=? AND id>? AND state IN ('mapped','published') LIMIT 1",
                    (self.project, identity),
                ).fetchone():
                    raise CatalogError(
                        "A newer snapshot was mapped or published; retry would risk a stale overwrite. Harvest a correction instead"
                    )
                self._execute(
                    "UPDATE work_items SET state='pending',batch_id=NULL,message=NULL,updated_at=? WHERE id=?",
                    (_now(), identity),
                )

    def reset(self):
        with self.transaction():
            if self._execute(
                "SELECT 1 FROM catalog_items WHERE project<>? UNION ALL SELECT 1 FROM work_items WHERE project<>? LIMIT 1",
                (self.project, self.project),
            ).fetchone():
                raise CatalogError(
                    "Project-only harvest cleaning is unsafe with shared Kafka checkpoints; start with a fresh database"
                )
            self._execute("DELETE FROM catalog_items WHERE project=?", (self.project,))
            self._execute("DELETE FROM work_items WHERE project=?", (self.project,))
            self._execute("DELETE FROM kafka_checkpoints")

    def compact(self):
        if (
            self.read_only
            or self._active_transaction is not None
            or self._read_connections
        ):
            raise CatalogError("Cannot compact a read-only or active database")
        if self._execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0]:
            raise CatalogError(
                "Cannot compact while another connection holds a WAL snapshot"
            )
        self._execute("VACUUM")

    def close(self):
        for reader in self._read_connections:
            reader.close()
        self._read_connections.clear()
        if self._connection is not None:
            self._connection.close()
        self._connection = None
        self._active_transaction = None


class SQLiteCatalogTransaction(BaseCatalogTransaction):
    def __init__(self, catalog):
        self._catalog = catalog
        self._failed = False

    def _require_active(self):
        self._catalog._require_connection()
        if self._catalog._active_transaction is not self:
            raise CatalogError("Catalog transaction is no longer active")

    def get_item(self, *args):
        self._require_active()
        return self._catalog.get_item(*args)

    def find_previous_version(self, *args):
        self._require_active()
        return self._catalog.find_previous_version(*args)

    def find_versions(self, *args, **kwargs):
        self._require_active()
        return self._catalog.find_versions(*args, **kwargs)

    def get_checkpoint(self, *args):
        self._require_active()
        return self._catalog.get_checkpoint(*args)

    def latest_event_date(self):
        self._require_active()
        return self._catalog.latest_event_date()

    def pending_work(self, *args):
        self._require_active()
        return self._catalog.pending_work(*args)

    def iter_pending(self, *args, **kwargs):
        self._require_active()
        return self._catalog.iter_pending(*args, **kwargs)

    def put_item(self, entry):
        self._require_active()
        try:
            blob = _encode(entry)
            self._catalog._execute(
                "INSERT INTO catalog_items VALUES (?,?,?,?,?,?,?) "
                "ON CONFLICT(project,collection_id,item_id) DO UPDATE SET dataset_id=excluded.dataset_id,"
                "version_number=excluded.version_number,stac_blob=excluded.stac_blob,last_update=excluded.last_update",
                (
                    self._catalog.project,
                    entry.collection_id,
                    entry.item_id,
                    entry.dataset_id,
                    entry.version_number,
                    blob,
                    _now(),
                ),
            )
        except (
            ValueError,
            TypeError,
            OverflowError,
            UnicodeError,
            CatalogError,
        ) as exc:
            self._failed = True
            raise CatalogError(f"Could not store catalog item: {exc}") from exc

    def add_work(self, entry, source):
        self._require_active()
        try:
            timestamp = _now()
            self._catalog._execute(
                "INSERT INTO work_items "
                "(project,topic,partition,offset,stac_blob,created_at,updated_at) VALUES (?,?,?,?,?,?,?)",
                (
                    self._catalog.project,
                    source.topic,
                    source.partition,
                    source.offset,
                    _encode(entry),
                    timestamp,
                    timestamp,
                ),
            )
        except (ValueError, TypeError, OverflowError, CatalogError) as exc:
            self._failed = True
            raise CatalogError(f"Could not store work snapshot: {exc}") from exc

    def set_checkpoint(self, checkpoint):
        self._require_active()
        previous = self.get_checkpoint(checkpoint.topic, checkpoint.partition)
        if previous is not None and checkpoint.next_offset < previous.next_offset:
            self._failed = True
            raise CatalogError("Checkpoint cannot move backwards; reset before replay")
        self._catalog._execute(
            "INSERT INTO kafka_checkpoints VALUES (?,?,?) "
            "ON CONFLICT(topic,partition) DO UPDATE SET next_offset=excluded.next_offset",
            (checkpoint.topic, checkpoint.partition, checkpoint.next_offset),
        )
