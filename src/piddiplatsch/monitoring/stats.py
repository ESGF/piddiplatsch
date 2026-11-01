import datetime
import json
import logging
import os
import re
import socket
import sqlite3
import threading
import time
import uuid
from collections import defaultdict
from contextlib import contextmanager
from enum import StrEnum

from piddiplatsch.config import config

logger = logging.getLogger(__name__)


def concise_error(message: str) -> str:
    """Keep operator context while excluding common credential forms and payloads."""
    first_line = message.splitlines()[0].strip()
    if first_line.startswith(("{", "[")):
        return "processing error (structured details omitted)"
    first_line = re.sub(r"(https?://)[^/@\s]+@", r"\1***@", first_line)
    first_line = re.sub(
        r"(?i)\b(password|token|authorization|secret)\s*[=:]\s*[^\s,;]+",
        r"\1=***",
        first_line,
    )
    return first_line[:500]


def to_iso(dt: float | datetime.datetime | None) -> str | None:
    """Convert a timestamp or datetime to a UTC ISO 8601 string."""
    if dt is None:
        return None
    if isinstance(dt, datetime.datetime):
        # If naive, assume UTC; otherwise convert to UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.UTC)
        else:
            dt = dt.astimezone(datetime.UTC)
        return dt.isoformat()
    # dt is a float timestamp
    return datetime.datetime.fromtimestamp(dt, tz=datetime.UTC).isoformat()


# -----------------------
# Enum for counters
# -----------------------
class CounterKey(StrEnum):
    MESSAGES = "messages"
    ERRORS = "errors"
    RETRIES = "retries"
    HANDLES = "handles"
    ALIAS_HANDLES = "alias_handles"
    CATALOG_WRITES = "catalog_writes"
    CATALOG_REFRESHES = "catalog_refreshes"
    RETRACTED = "retracted_messages"
    REPLICAS = "replicas"
    WARNINGS = "warnings"
    SKIPPED = "skipped_messages"
    FILTERED = "filtered_messages"
    PATCHED = "patched_messages"
    HANDLE_TIME = "total_handle_processing_time"  # float seconds
    EXTERNAL_FAILS = "external_failures"


# -----------------------
# Reporter base
# -----------------------
class StatsReporter:
    def log(self, summary: dict):
        raise NotImplementedError

    def close(self):
        return None


class ConsoleReporter(StatsReporter):
    def log(self, summary: dict):
        run = summary.get("run") or {}
        command = run.get("command", summary.get("command") or "stats")
        operation = "sync/map" if command == "sync" else command
        parts = [run.get("status", "running")]
        unit = "messages" if command in {"harvest", "consume"} else "items"
        parts.append(f"{summary.get('messages', 0):,} {unit}")
        if command in {"harvest", "consume"}:
            parts.append(f"{summary.get('catalog_writes', 0):,} catalog writes")
        else:
            parts.append(f"{summary.get('handles', 0):,} Handles prepared")
            if summary.get("alias_handles"):
                aliases = summary["alias_handles"]
                label = "alias" if aliases == 1 else "aliases"
                parts[-1] += f" ({aliases:,} {label})"
        parts.append(f"{summary.get('errors', 0):,} errors")
        for key, label in (
            ("catalog_refreshes", "refreshed"),
            ("retries", "retries"),
            ("warnings", "warnings"),
            ("skipped_messages", "skipped"),
            ("filtered_messages", "filtered"),
        ):
            if summary.get(key):
                parts.append(f"{summary[key]:,} {label}")
        parts.append(f"{summary.get('messages_per_sec', 0):,.0f} {unit}/s")
        parts.append(f"elapsed {summary.get('uptime', 0):,.0f}s")
        projects = [
            name
            for name, counters in summary.get("projects", {}).items()
            if counters.get("consumed", 0)
        ]
        if projects:
            parts.append(f"projects: {', '.join(sorted(projects))}")
        logger.info("[%s] %s", operation, " | ".join(parts))
        logger.debug("[%s] Stats snapshot: %s", operation, summary)


class StatusReporter(StatsReporter):
    """Live heartbeats and bounded rate histories in the shared stats table."""

    def __init__(
        self, db_path=None, sample_interval_seconds=15.0, sample_retention_days=30.0
    ):
        from pathlib import Path

        from piddiplatsch.catalog.api import get_catalog_directory
        from piddiplatsch.catalog.sqlite import SQLiteCatalog

        database = Path(db_path) if db_path else get_catalog_directory() / "piddi.db"
        with SQLiteCatalog("_monitor", database):
            pass
        self.connection = sqlite3.connect(database, timeout=5, check_same_thread=False)
        self._lock = threading.RLock()
        self._closed = False
        self.samples = []
        self.sample_interval_seconds = sample_interval_seconds
        self._last_sample = 0.0

    def log(self, summary):
        self.heartbeat(summary)

    def heartbeat(self, summary, *, force_sample=False):
        with self._lock:
            if self._closed or not summary.get("run"):
                return
            now = datetime.datetime.now(datetime.UTC)
            run = dict(summary["run"])
            run["heartbeat_at"] = now.isoformat()
            for key in ("last_message_at", "last_success_at", "last_error_at"):
                run[key] = summary.get(key.replace("_at", "_time"))
            projects = [
                dict(project=name, updated_at=now.isoformat(), **values)
                for name, values in summary.get("projects", {}).items()
            ]
            if (
                force_sample
                or time.monotonic() - self._last_sample >= self.sample_interval_seconds
            ):
                self.samples.extend(
                    dict(row, sampled_at=now.isoformat()) for row in projects
                )
                self._last_sample = time.monotonic()
            cutoff = (now - datetime.timedelta(hours=1)).isoformat()
            self.samples = [row for row in self.samples if row["sampled_at"] >= cutoff]
            from piddiplatsch.monitoring.disk import disk_status

            storage = disk_status()
            # Each run/project owns its row. Heartbeats never update batch totals.
            with self.connection:
                for row in projects or [{"project": ""}]:
                    name = row["project"]
                    payload = dict(
                        run=run,
                        storage=storage,
                        projects=[row] if name else [],
                        samples=[
                            sample
                            for sample in self.samples
                            if sample["project"] == name
                        ],
                    )
                    self.connection.execute(
                        "INSERT INTO stats(kind,project,id,status,created_at,updated_at,runtime) "
                        "VALUES ('run',?,?,?,?,?,?) ON CONFLICT(kind,project,id) DO UPDATE SET "
                        "status=excluded.status,updated_at=excluded.updated_at,runtime=excluded.runtime",
                        (
                            name,
                            run["run_id"],
                            run["status"],
                            run["started_at"],
                            now.isoformat(),
                            json.dumps(payload),
                        ),
                    )

    def close(self):
        with self._lock:
            if not self._closed:
                self._closed = True
                self.connection.close()


# -----------------------
# Stats singleton
# -----------------------
class Stats:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        log_interval_seconds: int | None = None,
        log_interval_messages: int | None = None,
        db_path: str | None = None,
        enable_db: bool = False,
    ):
        if getattr(self, "_initialized", False):
            return

        # Private timestamps as UTC datetime
        self._start_time = datetime.datetime.now(datetime.UTC)
        self._last_message_time: datetime.datetime | None = None
        self._last_error_time: datetime.datetime | None = None
        self._last_success_time: datetime.datetime | None = None

        # Initialize counters
        self._counters = dict.fromkeys(CounterKey, 0)
        self._counters[CounterKey.HANDLE_TIME] = 0.0
        self._project_counters: dict[str, dict[str, int]] = defaultdict(
            lambda: dict.fromkeys(
                (
                    "consumed",
                    "routed",
                    "filtered",
                    "succeeded",
                    "skipped",
                    "failed",
                    "handles",
                    "catalog_writes",
                    "catalog_refreshes",
                    "full_events",
                    "patch_events",
                ),
                0,
            )
        )
        self._run: dict | None = None
        self._command: str | None = None
        self._heartbeat_thread: threading.Thread | None = None
        self._heartbeat_stop = threading.Event()
        self._lock = threading.RLock()

        # Logging control
        self.log_interval_seconds = log_interval_seconds or 30
        self.log_interval_messages = log_interval_messages or 10000
        self._last_log_time = time.time()
        self._last_logged_messages = 0

        # Reporters
        self.reporters: list[StatsReporter] = [ConsoleReporter()]
        if enable_db:
            self.reporters.append(StatusReporter(db_path=db_path))

        self._closed = False
        self._initialized = True

    def reset(self):
        """
        Reset counters, timestamps, and logging state.
        Can be used for testing or to restart stats tracking.
        """
        self._stop_heartbeat()
        self._start_time = datetime.datetime.now(datetime.UTC)
        self._last_message_time = None
        self._last_error_time = None
        self._last_success_time = None

        # Reset counters
        self._counters = dict.fromkeys(CounterKey, 0)
        self._counters[CounterKey.HANDLE_TIME] = 0.0
        self._project_counters = defaultdict(
            lambda: dict.fromkeys(
                (
                    "consumed",
                    "routed",
                    "filtered",
                    "succeeded",
                    "skipped",
                    "failed",
                    "handles",
                    "catalog_writes",
                    "catalog_refreshes",
                    "full_events",
                    "patch_events",
                ),
                0,
            )
        )
        self._run = None
        self._command = None

        # Reset logging control
        self._last_log_time = time.time()
        self._last_logged_messages = 0

        # Close and reset reporters (optional: keep ConsoleReporter)
        for reporter in list(self.reporters):
            try:
                reporter.close()
            except Exception:
                pass

        self.reporters = [ConsoleReporter()]
        self._closed = False

    def configure_for_run(
        self,
        enable_db: bool = False,
        db_path: str | None = None,
        log_interval_seconds: int | None = None,
        log_interval_messages: int | None = None,
        command: str = "consume",
        topic: str | None = None,
        consumer_group: str | None = None,
        selected_projects: tuple[str, ...] | list[str] = (),
        heartbeat_interval_seconds: float = 5.0,
        sample_interval_seconds: float = 15.0,
        sample_retention_days: float = 30.0,
        expected_handles: int | None = None,
        batch_id: str | None = None,
    ):
        """Reset counters and reporters for a fresh run, optionally enabling DB.

        This keeps reporter setup encapsulated and avoids leaking lifecycle
        details into callers like the consumer.
        """
        if self._run and not self._closed:
            self.close(status="stopped")
        self.reset()
        self._command = command
        if log_interval_seconds is not None:
            self.log_interval_seconds = log_interval_seconds
        if log_interval_messages is not None:
            self.log_interval_messages = log_interval_messages
        if enable_db:
            try:
                self.reporters.append(
                    StatusReporter(
                        db_path=db_path,
                        sample_interval_seconds=sample_interval_seconds,
                        sample_retention_days=sample_retention_days,
                    )
                )
            except Exception:
                logger.exception(
                    "Failed to initialize StatusReporter; continuing without DB reporter"
                )
        if enable_db and any(isinstance(r, StatusReporter) for r in self.reporters):
            self._run = {
                "run_id": uuid.uuid4().hex,
                "command": command,
                "pid": os.getpid(),
                "hostname": socket.gethostname(),
                "started_at": to_iso(self._start_time),
                "status": "running",
                "topic": topic,
                "consumer_group": consumer_group,
                "selected_projects": list(selected_projects),
                "expected_handles": expected_handles,
                "batch_id": batch_id,
                "finished_at": None,
                "error_summary": None,
            }
            for project in selected_projects:
                self._project_counters[project]
            self._heartbeat_once(force_sample=True)
            self._start_heartbeat(heartbeat_interval_seconds)

    def _start_heartbeat(self, interval: float) -> None:
        self._heartbeat_stop.clear()

        def beat() -> None:
            while not self._heartbeat_stop.wait(max(0.1, interval)):
                self._heartbeat_once()

        self._heartbeat_thread = threading.Thread(
            target=beat, name="piddi-stats-heartbeat", daemon=True
        )
        self._heartbeat_thread.start()

    def _stop_heartbeat(self) -> None:
        self._heartbeat_stop.set()
        thread = self._heartbeat_thread
        if thread and thread is not threading.current_thread():
            thread.join(timeout=2)
        self._heartbeat_thread = None

    def _heartbeat_once(self, *, force_sample: bool = False) -> None:
        summary = self.summary()
        for reporter in list(self.reporters):
            if isinstance(reporter, StatusReporter):
                try:
                    reporter.heartbeat(summary, force_sample=force_sample)
                except Exception:
                    logger.exception("Failed to persist monitoring heartbeat")

    def record_result(self, result) -> None:
        """Record one processing result in aggregate and project-aware counters."""
        project = result.plugin or result.project or "unassigned"
        with self._lock:
            counters = self._project_counters[project]
            counters["consumed"] += 1
            event_type = getattr(result, "event_type", None)
            if event_type in {"full", "patch"}:
                counters[event_type + "_events"] += 1
            if result.filtered:
                counters["filtered"] += 1
                self.tick()
                self.filtered(message=f"message={result.key} project={project}")
            elif result.skipped:
                counters["routed"] += 1
                counters["skipped"] += 1
                self.tick()
            elif result.success:
                counters["routed"] += 1
                counters["succeeded"] += 1
                counters["handles"] += result.num_handles
                self._counters[CounterKey.CATALOG_WRITES] += int(
                    getattr(result, "catalog_written", False)
                )
                self._counters[CounterKey.CATALOG_REFRESHES] += int(
                    getattr(result, "catalog_refreshed", False)
                )
                counters["catalog_writes"] += int(
                    getattr(result, "catalog_written", False)
                )
                counters["catalog_refreshes"] += int(
                    getattr(result, "catalog_refreshed", False)
                )
                self._last_success_time = datetime.datetime.now(datetime.UTC)
                self.increment(
                    CounterKey.ALIAS_HANDLES, getattr(result, "num_alias_handles", 0)
                )
                self.handle(
                    n=result.num_handles,
                    handle_time_sec=result.handle_processing_time,
                )
                self.tick()
            else:
                counters["routed"] += 1
                counters["failed"] += 1
                self.error(message=result.error)

    def record_publication(
        self, project: str, *, success: bool, error: str | None = None
    ):
        """Count completed writes without adding per-Handle log messages."""
        with self._lock:
            counters = self._project_counters[project]
            counters["consumed"] += 1
            counters["routed"] += 1
            if success:
                counters["succeeded"] += 1
                counters["handles"] += 1
                self.handle()
                self._last_success_time = datetime.datetime.now(datetime.UTC)
            else:
                counters["failed"] += 1
                self.increment(CounterKey.ERRORS)
                self._last_error_time = datetime.datetime.now(datetime.UTC)
                if self._run and error:
                    self._run["error_summary"] = concise_error(error)
            self.tick()

    # --- Core increment ---
    def increment(self, key: CounterKey, n=1):
        if key == CounterKey.HANDLE_TIME:
            self._counters[key] += n
        else:
            self._counters[key] += n

        if key == CounterKey.MESSAGES:
            self._last_message_time = datetime.datetime.now(datetime.UTC)
            self._maybe_log()

    # --- Shortcuts ---
    def tick(self, n=1):
        self.increment(CounterKey.MESSAGES, n)

    def retry(self, n=1):
        self.increment(CounterKey.RETRIES, n)

    def handle(self, n=1, handle_time_sec: float = 0.0):
        self.increment(CounterKey.HANDLES, n)
        if handle_time_sec > 0:
            self.increment(CounterKey.HANDLE_TIME, handle_time_sec)

    def error(self, message: str | None = None, n=1):
        self.increment(CounterKey.ERRORS, n)
        self._last_error_time = datetime.datetime.now(datetime.UTC)
        if self._run and message:
            self._run["error_summary"] = concise_error(message)
        if message:
            logger.error(f"ERROR: {message}")

    def retracted(self, message: str | None = None, n=1):
        self.increment(CounterKey.RETRACTED, n)
        if message:
            logger.info(f"RETRACTED: {message}")

    def replica(self, message: str | None = None, n=1):
        self.increment(CounterKey.REPLICAS, n)
        if message:
            logger.info(f"REPLICA: {message}")

    def warn(self, message: str | None = None, n=1):
        self.increment(CounterKey.WARNINGS, n)
        if message:
            logger.warning(f"WARNING: {message}")

    def skip(self, message: str | None = None, n=1):
        self.increment(CounterKey.SKIPPED, n)
        if message:
            logger.info(f"SKIPPED: {message}")

    def filtered(self, message: str | None = None, n=1):
        self.increment(CounterKey.FILTERED, n)
        if message:
            logger.debug(f"FILTERED: {message}")

    def patch(self, message: str | None = None, n=1):
        self.increment(CounterKey.PATCHED, n)
        if message:
            logger.debug("PATCHED: %s", message)

    def external_fail(self, message: str | None = None, n=1):
        self.increment(CounterKey.EXTERNAL_FAILS, n)
        if message:
            logger.warning(f"EXTERNAL-FAIL: {message}")

    # --- Logging / persistence ---
    def _maybe_log(self):
        now = time.time()
        messages_since_last = (
            self._counters[CounterKey.MESSAGES] - self._last_logged_messages
        )

        if messages_since_last == 0:
            return

        if (now - self._last_log_time >= self.log_interval_seconds) or (
            messages_since_last >= self.log_interval_messages
        ):
            self._log_stats()
            self._last_log_time = now
            self._last_logged_messages = self._counters[CounterKey.MESSAGES]

    def _log_stats(self):
        summary = self.summary()
        for reporter in list(self.reporters):
            try:
                reporter.log(summary)
            except Exception:
                logger.exception("Failed to log stats with reporter %s", reporter)

    # --- Accessors ---
    def __getitem__(self, key: CounterKey):
        return self._counters[key]

    @property
    def messages(self) -> int:
        return self._counters[CounterKey.MESSAGES]

    @property
    def replicas(self) -> int:
        return self._counters[CounterKey.REPLICAS]

    @property
    def retracted_messages(self) -> int:
        return self._counters[CounterKey.RETRACTED]

    @property
    def skipped_messages(self) -> int:
        return self._counters[CounterKey.SKIPPED]

    @property
    def filtered_messages(self) -> int:
        return self._counters[CounterKey.FILTERED]

    @property
    def patched_messages(self) -> int:
        return self._counters[CounterKey.PATCHED]

    @property
    def errors(self) -> int:
        return self._counters[CounterKey.ERRORS]

    @property
    def warnings(self) -> int:
        return self._counters[CounterKey.WARNINGS]

    @property
    def retries(self) -> int:
        return self._counters[CounterKey.RETRIES]

    @property
    def catalog_writes(self) -> int:
        return self._counters[CounterKey.CATALOG_WRITES]

    @property
    def catalog_refreshes(self) -> int:
        return self._counters[CounterKey.CATALOG_REFRESHES]

    @property
    def handles(self) -> int:
        return self._counters[CounterKey.HANDLES]

    @property
    def handle_time_total(self) -> float:
        return self._counters[CounterKey.HANDLE_TIME]

    @property
    def start_time(self) -> datetime.datetime:
        return self._start_time

    @property
    def uptime(self) -> float:
        return (datetime.datetime.now(datetime.UTC) - self._start_time).total_seconds()

    @property
    def last_message_time(self) -> datetime.datetime | None:
        return self._last_message_time

    @property
    def last_error_time(self) -> datetime.datetime | None:
        return self._last_error_time

    @property
    def message_rate(self) -> float:
        return self.messages / self.uptime if self.uptime > 0 else 0.0

    @property
    def handle_rate(self) -> float:
        return self.handles / self.uptime if self.uptime > 0 else 0.0

    @property
    def messages_per_sec(self) -> float:
        interval = time.time() - self._last_log_time
        interval_messages = self.messages - self._last_logged_messages
        return interval_messages / interval if interval > 0 else 0.0

    # --- Summary ---
    def summary(self):
        with self._lock:
            summary = {key.value: self._counters[key] for key in CounterKey}
            projects = {
                name: dict(values) for name, values in self._project_counters.items()
            }
        summary.update(
            {
                "uptime": self.uptime,
                "message_rate": self.message_rate,
                "handle_rate": self.handle_rate,
                "messages_per_sec": self.messages_per_sec,
                "last_message_time": to_iso(self.last_message_time),
                "last_error_time": to_iso(self.last_error_time),
                "last_success_time": to_iso(self._last_success_time),
                "start_time": to_iso(self.start_time),
                "projects": projects,
                "run": dict(self._run) if self._run else None,
                "command": self._command,
            }
        )
        return summary

    # --- Cleanup ---
    def close(self, *, status: str = "stopped", error_summary: str | None = None):
        if getattr(self, "_closed", False):
            return
        self._stop_heartbeat()
        if self._run:
            self._run["status"] = status
            self._run["finished_at"] = to_iso(datetime.datetime.now(datetime.UTC))
            if error_summary is not None:
                self._run["error_summary"] = concise_error(error_summary)
            self._heartbeat_once(force_sample=True)
        if self._command == "map":
            summary = self.summary()
            if summary["run"] is None:
                summary["run"] = {"command": "map", "status": status}
            ConsoleReporter().log(summary)
        for reporter in list(self.reporters):
            try:
                reporter.close()
            except Exception:
                logger.exception("Error closing reporter %s", reporter)
        self._closed = True


# -----------------------
# Singleton instance
# -----------------------
stats_config = config.get("stats", {})

stats = Stats(
    log_interval_seconds=stats_config.get("interval_seconds"),
    log_interval_messages=stats_config.get("summary_interval"),
    # Reporters are opened only when a processing run starts. In particular,
    # importing the read-only top command must never create a database.
    enable_db=False,
)


@contextmanager
def publication_monitor(project: str | None, *, expected_handles=None, batch_id=None):
    """Give publication its own heartbeat and persistent counters."""
    settings = config.get("stats", {})
    if not settings.get("enable_db", False):
        yield None
        return
    stats.configure_for_run(
        enable_db=True,
        db_path=None,
        command="publish",
        expected_handles=expected_handles,
        batch_id=batch_id,
        selected_projects=(project,) if project else (),
        heartbeat_interval_seconds=settings.get("heartbeat_interval_seconds", 5),
        sample_interval_seconds=settings.get("sample_interval_seconds", 15),
        sample_retention_days=settings.get("sample_retention_days", 30),
    )
    # HandlePublisher already owns the INFO summaries and detailed error logs.
    stats.reporters = [
        reporter
        for reporter in stats.reporters
        if not isinstance(reporter, ConsoleReporter)
    ]
    try:
        yield stats
    except BaseException as exc:
        stats.close(status="failed", error_summary=str(exc))
        raise
    else:
        stats.close(status="failed" if stats.errors else "completed")
