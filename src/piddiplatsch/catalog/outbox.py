"""Stream immutable SQLite Handle payloads and commit publication progress."""

import json
import sqlite3
import threading
import zlib
from dataclasses import dataclass
from pathlib import Path


@dataclass
class _Acknowledgement:
    handle: str
    position: int
    done: bool = False
    error: BaseException | None = None


class OutboxJournal:
    """Short transactions acknowledge a write before a newer one can run.

    No database transaction or snapshot is held across an HTTP request. A crash
    between the remote write and its commit retries that same Handle payload.
    Acknowledgements waiting behind a commit share the next FULL transaction;
    every caller waits for its own commit before continuing its Handle chain.
    """

    def __init__(self, database, project, batch_id):
        self.database = Path(database)
        self.project = project
        self.batch_id = batch_id
        self.lock = threading.RLock()
        self._acknowledgements = threading.Condition()
        self._pending = []
        self._committing = False
        self.blocked = set()
        self.connection = sqlite3.connect(database, timeout=30, check_same_thread=False)
        try:
            self.connection.execute("PRAGMA synchronous=FULL")
            self.successes = dict(
                self.connection.execute(
                    "SELECT handle,MAX(position) FROM handle_outbox WHERE project=? AND batch_id=? AND published=1 GROUP BY handle",
                    (project, batch_id),
                )
            )
        except BaseException:
            self.connection.close()
            raise

    def records(self):
        position = 0
        while True:
            with self.lock:
                rows = self.connection.execute(
                    "SELECT position,payload FROM handle_outbox WHERE project=? AND batch_id=? AND position>? ORDER BY position LIMIT 256",
                    (self.project, self.batch_id, position),
                ).fetchall()
            if not rows:
                return
            for position, payload in rows:
                yield self.database, position, json.loads(zlib.decompress(payload))

    def status(self, handle, line):
        with self.lock:
            if line <= self.successes.get(handle, 0):
                return "done"
            return "blocked" if handle in self.blocked else "pending"

    def record(self, outcome):
        if outcome.error is not None:
            with self.lock:
                self.blocked.add(outcome.handle)
            return
        acknowledgement = _Acknowledgement(outcome.handle, outcome.line_number)
        # Queue independently of the connection lock so workers can join the
        # next transaction while another worker is waiting for disk sync.
        with self._acknowledgements:
            self._pending.append(acknowledgement)
            self._acknowledgements.notify_all()
            while self._committing and not acknowledgement.done:
                self._acknowledgements.wait()
            if acknowledgement.done:
                if acknowledgement.error is not None:
                    raise acknowledgement.error
                return
            self._committing = True
            pending, self._pending = self._pending, []
        error = None
        try:
            self._record_group(pending)
        except BaseException as exc:
            error = exc
            raise
        finally:
            with self._acknowledgements:
                for entry in pending:
                    entry.error = error
                    entry.done = True
                self._committing = False
                self._acknowledgements.notify_all()

    def _record_group(self, pending):
        with self.lock:
            confirmed = {}
            with self.connection:
                for entry in pending:
                    if entry.position <= confirmed.get(
                        entry.handle, self.successes.get(entry.handle, 0)
                    ):
                        continue
                    cursor = self.connection.execute(
                        "UPDATE handle_outbox SET published=1 WHERE project=? AND batch_id=? AND position=? AND handle=?",
                        (self.project, self.batch_id, entry.position, entry.handle),
                    )
                    if cursor.rowcount != 1:
                        raise ValueError(
                            "Publication acknowledgement does not match the outbox"
                        )
                    confirmed[entry.handle] = entry.position
            # Publish in-memory progress only after the entire group commits.
            self.successes.update(confirmed)

    def close(self):
        self.connection.close()
