"""Slow, read-only metrics sampled without delaying the terminal refresh."""

import sqlite3
import time
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from queue import Empty, SimpleQueue
from threading import Thread

from piddiplatsch.monitoring.estimates import estimate_table_sizes
from piddiplatsch.monitoring.health import database_health
from piddiplatsch.monitoring.status import summarize_event_totals


class BackgroundMetrics:
    def __init__(self, path, project=None):
        self.path = Path(path)
        self.project = project
        self.results = SimpleQueue()
        self.worker = None
        self.completed_at = None
        self.latest = {}

    def update(self, status):
        while True:
            try:
                kind, value = self.results.get_nowait()
            except Empty:
                break
            self.latest[kind] = value
        if self.worker is not None and not self.worker.is_alive():
            self.completed_at = time.monotonic()
            self.worker = None
        if self.worker is None and (
            self.completed_at is None or time.monotonic() - self.completed_at >= 60
        ):
            self.worker = Thread(target=self._sample, daemon=True)
            self.worker.start()
        health = self.latest.get("health")
        if health is not None and "error" not in health:
            status["health"]["projects"] = health["projects"]
            status["event_totals"] = summarize_event_totals(
                health["projects"], status["handle_totals"]
            )
        status["table_sizes"] = self.latest.get("table_sizes")
        status["metrics"] = dict(
            sampling=self.worker is not None,
            counts_sampled_at=health.get("sampled_at") if health else None,
            error=health.get("error") if health else None,
        )

    def _sample(self):
        health = None
        for kind in ("health", "table_sizes"):
            try:
                # Separate snapshots let counts appear before the size scan ends.
                with closing(
                    sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True)
                ) as connection:
                    connection.execute("BEGIN")
                    sampled_at = datetime.now(UTC).isoformat()
                    if kind == "health":
                        health = database_health(connection, None, fast=True)
                        value = dict(health)
                        if self.project:
                            value["projects"] = [
                                row
                                for row in health["projects"]
                                if row["project"].casefold() == self.project.casefold()
                            ]
                    elif health is not None:
                        value = estimate_table_sizes(connection, health)
                    else:
                        value = dict(
                            available=False, tables=[], reason="row counts unavailable"
                        )
                    value["sampled_at"] = sampled_at
            except (sqlite3.Error, OSError) as exc:
                value = dict(
                    error=str(exc), available=False, tables=[], reason=str(exc)
                )
            self.results.put((kind, value))
