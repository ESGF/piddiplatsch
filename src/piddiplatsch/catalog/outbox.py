"""Stream immutable SQLite Handle payloads and commit publication progress."""

import json
import sqlite3
import threading
import zlib
from pathlib import Path


class OutboxJournal:
    """Short transactions acknowledge a write before a newer one can run.

    No database transaction or snapshot is held across an HTTP request. A crash
    between the remote write and its commit retries that same Handle payload.
    """

    def __init__(self, database, project, batch_id):
        self.database = Path(database)
        self.project = project
        self.batch_id = batch_id
        self.lock = threading.RLock()
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
        with self.lock:
            if outcome.error is not None:
                self.blocked.add(outcome.handle)
                return
            if outcome.line_number <= self.successes.get(outcome.handle, 0):
                return
            with self.connection:
                cursor = self.connection.execute(
                    "UPDATE handle_outbox SET published=1 WHERE project=? AND batch_id=? AND position=? AND handle=?",
                    (self.project, self.batch_id, outcome.line_number, outcome.handle),
                )
                if cursor.rowcount != 1:
                    raise ValueError(
                        "Publication acknowledgement does not match the outbox"
                    )
            self.successes[outcome.handle] = outcome.line_number

    def close(self):
        self.connection.close()
