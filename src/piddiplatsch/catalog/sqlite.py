"""SQLite storage for one project's complete, versioned STAC items."""

import json
import sqlite3
import zlib
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from piddiplatsch.catalog.base import BaseCatalog, BaseCatalogTransaction
from piddiplatsch.catalog.models import CatalogEntry, KafkaCheckpoint
from piddiplatsch.exceptions import CatalogError

_SCHEMA_VERSION = 1


def _decode_entry(row: tuple) -> CatalogEntry:
    try:
        collection_id, dataset_id, version_number, payload = row
        item = json.loads(zlib.decompress(payload))
        return CatalogEntry(collection_id, item, dataset_id, version_number)
    except (ValueError, TypeError, zlib.error) as exc:
        raise CatalogError(f"Invalid stored catalog entry: {exc}") from exc


class SQLiteCatalog(BaseCatalog):
    """Persistent catalog backed by a dedicated project database file.

    Payloads use compact UTF-8 JSON compressed independently with zlib level 3,
    available in the standard library. Identity/version indexes remain outside
    the payload. Schema version 1 fixes this codec; future changes need migration.

    One instance belongs to one thread. WAL permits separate instances/processes
    to read while a writer holds a transaction; writers serialize with a bounded
    SQLite lock timeout. Streaming uses a separate read connection and a snapshot
    established on its first query, excluding uncommitted transaction writes.
    Long-lived iterators can delay WAL reclamation and must be closed promptly.
    Use local disk, not a network filesystem. Reset reuses space; it does not
    compact or remove the database. No harvest/map integration is enabled here.
    """

    def __init__(self, project: str, path: str | Path, *, timeout: float = 5.0):
        super().__init__(project)
        if str(path) == ":memory:":
            raise ValueError("SQLiteCatalog requires a persistent database file")
        self.path = Path(path).resolve()
        self._connection: sqlite3.Connection | None = None
        self._active_transaction: SQLiteCatalogTransaction | None = None
        self._read_connections: set[sqlite3.Connection] = set()
        self._timeout = timeout
        try:
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
            elif metadata != (self.project, _SCHEMA_VERSION):
                raise CatalogError(
                    "Catalog project or schema version does not match: "
                    f"stored={metadata!r}, requested={(self.project, _SCHEMA_VERSION)!r}"
                )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS items ("
                "collection_id TEXT NOT NULL, item_id TEXT NOT NULL, "
                "dataset_id TEXT NOT NULL, version_number INTEGER NOT NULL, "
                "payload BLOB NOT NULL, PRIMARY KEY (collection_id, item_id))"
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
        row = self._execute(
            "SELECT collection_id, dataset_id, version_number, payload "
            "FROM items WHERE collection_id = ? AND item_id = ?",
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

    def iter_items(self, *, collection_id: str | None = None) -> Iterator[CatalogEntry]:
        self._require_connection()
        reader = None
        try:
            # URI mode prevents accidentally recreating a removed database.
            reader = sqlite3.connect(
                self.path.as_uri() + "?mode=ro",
                uri=True,
                timeout=self._timeout,
                isolation_level=None,
            )
            self._read_connections.add(reader)
            reader.execute("BEGIN")
            sql = "SELECT collection_id, dataset_id, version_number, payload FROM items"
            parameters = ()
            if collection_id is not None:
                sql += " WHERE collection_id = ?"
                parameters = (collection_id,)
            cursor = reader.execute(
                sql + " ORDER BY collection_id, item_id", parameters
            )
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
            self._catalog._execute(
                "INSERT INTO items VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(collection_id, item_id) DO UPDATE SET "
                "dataset_id = excluded.dataset_id, version_number = excluded.version_number, "
                "payload = excluded.payload",
                (
                    entry.collection_id,
                    entry.item_id,
                    entry.dataset_id,
                    entry.version_number,
                    payload,
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
