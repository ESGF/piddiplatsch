"""SQLite storage for one project's complete, versioned STAC items."""

import json
import sqlite3
import zlib
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from pathlib import Path

from piddiplatsch.catalog.base import BaseCatalog, BaseCatalogTransaction
from piddiplatsch.catalog.models import CatalogEntry, KafkaCheckpoint
from piddiplatsch.exceptions import CatalogError

_SCHEMA_VERSION = 3


def _decode_entry(row: tuple) -> CatalogEntry:
    try:
        collection_id, dataset_id, version_number, payload, publication_time = row
        item = json.loads(zlib.decompress(payload))
        return CatalogEntry(
            collection_id, item, dataset_id, version_number, publication_time
        )
    except (ValueError, TypeError, zlib.error) as exc:
        raise CatalogError(f"Invalid stored catalog entry: {exc}") from exc


class SQLiteCatalog(BaseCatalog):
    """Persistent catalog backed by a dedicated project database file.

    Payloads use compact UTF-8 JSON compressed independently with zlib level 3,
    available in the standard library. Identity/version indexes remain outside
    the payload. Version 2 adds publication time; version 3 indexes UTC event days.
    Writer opens migrate older schemas and backfill dates once with bounded memory.
    Read-only opens support all versions without migrating or creating files;
    date selections require version 3.

    One instance belongs to one thread. WAL permits separate instances/processes
    to read while a writer holds a transaction; writers serialize with a bounded
    SQLite lock timeout. Streaming uses a separate read connection and a snapshot
    established on its first query, excluding uncommitted transaction writes.
    Long-lived iterators can delay WAL reclamation and must be closed promptly.
    Use local disk, not a network filesystem. Reset reuses space; it does not
    compact or remove the database. Read-only opens hold a snapshot shared by item
    iteration and predecessor queries until close.
    """

    def __init__(
        self,
        project: str,
        path: str | Path,
        *,
        timeout: float = 5.0,
        read_only: bool = False,
    ):
        super().__init__(project)
        if str(path) == ":memory:":
            raise ValueError("SQLiteCatalog requires a persistent database file")
        self.path = Path(path).resolve()
        self._connection: sqlite3.Connection | None = None
        self._active_transaction: SQLiteCatalogTransaction | None = None
        self._read_connections: set[sqlite3.Connection] = set()
        self._timeout = timeout
        self.read_only = read_only
        self._schema_version = _SCHEMA_VERSION
        try:
            if read_only:
                self._connection = sqlite3.connect(
                    self.path.as_uri() + "?mode=ro",
                    uri=True,
                    timeout=timeout,
                    isolation_level=None,
                )
                self._execute("BEGIN")
                metadata = self._execute(
                    "SELECT project, schema_version FROM catalog_metadata WHERE singleton = 1"
                ).fetchone()
                self._validate_metadata(metadata)
                self._schema_version = metadata[1]
            else:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                self._connection = sqlite3.connect(
                    self.path, timeout=timeout, isolation_level=None
                )
                self._initialize()
                self._execute("PRAGMA journal_mode=WAL")
                self._execute("PRAGMA synchronous=FULL")
        except (OSError, sqlite3.Error, CatalogError) as exc:
            self.close()
            raise CatalogError(f"Could not open catalog {self.path}: {exc}") from exc

    def _validate_metadata(self, metadata) -> None:
        if (
            metadata is None
            or metadata[0] != self.project
            or metadata[1] not in (1, 2, _SCHEMA_VERSION)
        ):
            raise CatalogError(
                "Catalog project or schema version does not match: "
                f"stored={metadata!r}, requested={(self.project, _SCHEMA_VERSION)!r}"
            )

    def _initialize(self) -> None:
        connection = self._require_connection()
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS catalog_metadata ("
                "singleton INTEGER PRIMARY KEY CHECK (singleton = 1), "
                "project TEXT NOT NULL, schema_version INTEGER NOT NULL)"
            )
            metadata = connection.execute(
                "SELECT project, schema_version FROM catalog_metadata WHERE singleton = 1"
            ).fetchone()
            if metadata is None:
                connection.execute(
                    "INSERT INTO catalog_metadata VALUES (1, ?, ?)",
                    (self.project, _SCHEMA_VERSION),
                )
            else:
                self._validate_metadata(metadata)
            connection.execute(
                "CREATE TABLE IF NOT EXISTS items ("
                "collection_id TEXT NOT NULL, item_id TEXT NOT NULL, "
                "dataset_id TEXT NOT NULL, version_number INTEGER NOT NULL, "
                "payload BLOB NOT NULL, publication_time TEXT, event_date TEXT, PRIMARY KEY (collection_id, item_id))"
            )
            if metadata is not None and metadata[1] == 1:
                connection.execute("ALTER TABLE items ADD COLUMN publication_time TEXT")
            if metadata is not None and metadata[1] < 3:
                connection.execute("ALTER TABLE items ADD COLUMN event_date TEXT")
                self._backfill_event_dates(connection)
                connection.execute(
                    "UPDATE catalog_metadata SET schema_version = ? WHERE singleton = 1",
                    (_SCHEMA_VERSION,),
                )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS items_event_dates ON items (event_date, collection_id, item_id)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS items_versions "
                "ON items (collection_id, dataset_id, version_number, item_id)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS checkpoints ("
                "topic TEXT NOT NULL, partition INTEGER NOT NULL, "
                "next_offset INTEGER NOT NULL, "
                "PRIMARY KEY (topic, partition)) WITHOUT ROWID"
            )
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise

    @staticmethod
    def _backfill_event_dates(connection: sqlite3.Connection) -> None:
        cursor = connection.execute(
            "SELECT collection_id, dataset_id, version_number, payload, publication_time, item_id FROM items"
        )
        try:
            while rows := cursor.fetchmany(256):
                updates = []
                for row in rows:
                    day = _decode_entry(row[:5]).event_date
                    updates.append((day.isoformat() if day else None, row[0], row[5]))
                connection.executemany(
                    "UPDATE items SET event_date = ? WHERE collection_id = ? AND item_id = ?",
                    updates,
                )
        finally:
            cursor.close()

    def latest_event_date(self) -> date | None:
        self._require_date_index()
        value = self._execute("SELECT MAX(event_date) FROM items").fetchone()[0]
        try:
            return date.fromisoformat(value) if value is not None else None
        except ValueError as exc:
            raise CatalogError(
                f"Invalid indexed catalog event date: {value!r}"
            ) from exc

    def _require_date_index(self) -> None:
        self._require_connection()
        if self._schema_version < 3:
            raise CatalogError(
                "Catalog date index is missing; run harvest once to upgrade before using date selectors"
            )

    def _require_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise CatalogError("Catalog is closed")
        return self._connection

    def _execute(self, sql: str, parameters: tuple = ()) -> sqlite3.Cursor:
        try:
            return self._require_connection().execute(sql, parameters)
        except (sqlite3.Error, OverflowError) as exc:
            raise CatalogError(f"SQLite catalog operation failed: {exc}") from exc

    def get_item(self, collection_id: str, item_id: str) -> CatalogEntry | None:
        sql = (
            (
                "SELECT collection_id, dataset_id, version_number, payload, publication_time "
                "FROM items WHERE collection_id = ? AND item_id = ?"
            )
            if self._schema_version >= 2
            else (
                "SELECT collection_id, dataset_id, version_number, payload, NULL "
                "FROM items WHERE collection_id = ? AND item_id = ?"
            )
        )
        row = self._execute(
            sql,
            (collection_id, item_id),
        ).fetchone()
        return None if row is None else _decode_entry(row)

    def find_previous_version(
        self, collection_id: str, dataset_id: str, version_number: int
    ) -> str | None:
        row = self._execute(
            "SELECT item_id FROM items "
            "WHERE collection_id = ? AND dataset_id = ? AND version_number < ? "
            "ORDER BY version_number DESC, item_id DESC LIMIT 1",
            (collection_id, dataset_id, version_number),
        ).fetchone()
        return None if row is None else row[0]

    def get_checkpoint(self, topic: str, partition: int) -> KafkaCheckpoint | None:
        row = self._execute(
            "SELECT topic, partition, next_offset FROM checkpoints "
            "WHERE topic = ? AND partition = ?",
            (topic, partition),
        ).fetchone()
        return None if row is None else KafkaCheckpoint(*row)

    @contextmanager
    def transaction(self) -> Iterator[BaseCatalogTransaction]:
        self._require_connection()
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

    def iter_items(
        self,
        *,
        collection_id: str | None = None,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> Iterator[CatalogEntry]:
        self._require_connection()
        dated = from_date is not None or to_date is not None
        if dated:
            self._require_date_index()
        reader = None
        cursor = None
        try:
            # URI mode prevents accidentally recreating a removed database.
            reader = (
                self._require_connection()
                if self.read_only
                else sqlite3.connect(
                    self.path.as_uri() + "?mode=ro",
                    uri=True,
                    timeout=self._timeout,
                    isolation_level=None,
                )
            )
            if not self.read_only:
                self._read_connections.add(reader)
                reader.execute("BEGIN")
            sql = (
                "SELECT collection_id, dataset_id, version_number, payload, publication_time FROM items"
                if self._schema_version >= 2
                else "SELECT collection_id, dataset_id, version_number, payload, NULL FROM items"
            )
            parameters = []
            conditions = []
            if collection_id is not None:
                conditions.append("collection_id = ?")
                parameters.append(collection_id)
            if from_date is not None:
                conditions.append("event_date >= ?")
                parameters.append(from_date.isoformat())
            if to_date is not None:
                conditions.append("event_date <= ?")
                parameters.append(to_date.isoformat())
            if conditions:
                sql += " WHERE " + " AND ".join(conditions)
            order = (
                "event_date, collection_id, item_id"
                if dated
                else "collection_id, item_id"
            )
            cursor = reader.execute(sql + " ORDER BY " + order, parameters)
            while True:
                self._require_connection()
                row = cursor.fetchone()
                if row is None:
                    break
                yield _decode_entry(row)
        except sqlite3.Error as exc:
            raise CatalogError(f"Could not stream SQLite catalog: {exc}") from exc
        finally:
            if reader is not None:
                if self.read_only:
                    if self._connection is not None and cursor is not None:
                        cursor.close()
                else:
                    self._read_connections.discard(reader)
                    reader.close()

    def reset(self) -> None:
        with self.transaction():
            self._execute("DELETE FROM items")
            self._execute("DELETE FROM checkpoints")

    def close(self) -> None:
        try:
            for reader in self._read_connections:
                reader.close()
            if self._connection is not None:
                # sqlite3.close rolls back any uncommitted transaction.
                self._connection.close()
        except sqlite3.Error as exc:
            raise CatalogError(f"Could not close SQLite catalog: {exc}") from exc
        finally:
            self._read_connections.clear()
            self._connection = None
            self._active_transaction = None

    def compact(self) -> None:
        self._require_connection()
        if self.read_only:
            raise CatalogError("Catalog is read-only")
        if self._active_transaction is not None or self._read_connections:
            raise CatalogError(
                "Cannot compact with an active transaction or read iterator"
            )
        if self._execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0]:
            raise CatalogError(
                "Cannot compact while another connection holds a WAL snapshot"
            )
        self._execute("VACUUM")
        if self._execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0]:
            raise CatalogError("Could not checkpoint compacted catalog")


class SQLiteCatalogTransaction(BaseCatalogTransaction):
    """Transaction handle owned by ``SQLiteCatalog.transaction()``."""

    def __init__(self, catalog: SQLiteCatalog):
        self._catalog = catalog
        self._failed = False

    def _require_active(self) -> None:
        self._catalog._require_connection()
        if self._catalog._active_transaction is not self:
            raise CatalogError("Catalog transaction is no longer active")

    def get_item(self, collection_id: str, item_id: str) -> CatalogEntry | None:
        self._require_active()
        return self._catalog.get_item(collection_id, item_id)

    def find_previous_version(
        self, collection_id: str, dataset_id: str, version_number: int
    ) -> str | None:
        self._require_active()
        return self._catalog.find_previous_version(
            collection_id, dataset_id, version_number
        )

    def get_checkpoint(self, topic: str, partition: int) -> KafkaCheckpoint | None:
        self._require_active()
        return self._catalog.get_checkpoint(topic, partition)

    def latest_event_date(self) -> date | None:
        self._require_active()
        return self._catalog.latest_event_date()

    def put_item(self, entry: CatalogEntry) -> None:
        self._require_active()
        try:
            # Revalidate because callers can mutate the entry's item dictionary.
            entry.__post_init__()
            payload = zlib.compress(
                json.dumps(
                    entry.item,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8"),
                level=3,
            )
            day = entry.event_date
            self._catalog._execute(
                "INSERT INTO items VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(collection_id, item_id) DO UPDATE SET "
                "dataset_id = excluded.dataset_id, version_number = excluded.version_number, "
                "payload = excluded.payload, publication_time = excluded.publication_time, event_date = excluded.event_date",
                (
                    entry.collection_id,
                    entry.item_id,
                    entry.dataset_id,
                    entry.version_number,
                    payload,
                    entry.publication_time,
                    day.isoformat() if day else None,
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

    def set_checkpoint(self, checkpoint: KafkaCheckpoint) -> None:
        self._require_active()
        try:
            previous = self.get_checkpoint(checkpoint.topic, checkpoint.partition)
            if previous is not None and checkpoint.next_offset < previous.next_offset:
                raise CatalogError(
                    "Checkpoint cannot move backwards; reset before replay"
                )
            self._catalog._execute(
                "INSERT INTO checkpoints VALUES (?, ?, ?) "
                "ON CONFLICT(topic, partition) DO UPDATE SET next_offset = excluded.next_offset",
                (checkpoint.topic, checkpoint.partition, checkpoint.next_offset),
            )
        except (OverflowError, CatalogError) as exc:
            self._failed = True
            raise CatalogError(f"Could not store catalog checkpoint: {exc}") from exc
