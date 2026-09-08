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
        logger.info(f"Stats snapshot: {summary}")


class SQLiteReporter(StatsReporter):
    def __init__(self, db_path: str = "stats.db"):
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA busy_timeout = 5000")
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._cursor = self._conn.cursor()
        self._lock = threading.RLock()
        self._cursor.executescript("""
            CREATE TABLE IF NOT EXISTS monitor_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT OR IGNORE INTO monitor_meta (key, value)
            VALUES ('schema_version', '1');

            CREATE TABLE IF NOT EXISTS monitor_runs (
                run_id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                pid INTEGER NOT NULL,
                hostname TEXT NOT NULL,
                started_at TEXT NOT NULL,
                heartbeat_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                topic TEXT,
                consumer_group TEXT,
                selected_projects TEXT NOT NULL,
                last_message_at TEXT,
                last_success_at TEXT,
                last_error_at TEXT,
                error_summary TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_monitor_runs_heartbeat
                ON monitor_runs(heartbeat_at);

            CREATE TABLE IF NOT EXISTS monitor_project_stats (
                run_id TEXT NOT NULL,
                project TEXT NOT NULL,
                consumed INTEGER NOT NULL DEFAULT 0,
                routed INTEGER NOT NULL DEFAULT 0,
                filtered INTEGER NOT NULL DEFAULT 0,
                succeeded INTEGER NOT NULL DEFAULT 0,
                skipped INTEGER NOT NULL DEFAULT 0,
                failed INTEGER NOT NULL DEFAULT 0,
                handles INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (run_id, project),
                FOREIGN KEY (run_id) REFERENCES monitor_runs(run_id)
            );
            CREATE INDEX IF NOT EXISTS idx_monitor_project_stats_project
                ON monitor_project_stats(project);
            """)
        self._conn.commit()
        self._closed = False

    def log(self, summary: dict):
        if self._closed:
            raise RuntimeError("SQLiteReporter is closed")
        with self._lock:
            self._write_status(summary)
            self._conn.commit()

    def heartbeat(self, summary: dict) -> None:
        """Refresh run liveness without adding a historical stats snapshot."""
        if self._closed:
            return
        with self._lock:
            self._write_status(summary)
            self._conn.commit()

    def _write_status(self, summary: dict) -> None:
        run = summary.get("run")
        if not run:
            return
        now = datetime.datetime.now(datetime.UTC).isoformat()
        self._cursor.execute(
            """
            INSERT INTO monitor_runs (
                run_id, command, pid, hostname, started_at, heartbeat_at,
                finished_at, status, topic, consumer_group, selected_projects,
                last_message_at, last_success_at, last_error_at, error_summary
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                heartbeat_at=excluded.heartbeat_at,
                finished_at=excluded.finished_at,
                status=excluded.status,
                last_message_at=excluded.last_message_at,
                last_success_at=excluded.last_success_at,
                last_error_at=excluded.last_error_at,
                error_summary=excluded.error_summary
            """,
            (
                run["run_id"],
                run["command"],
                run["pid"],
                run["hostname"],
                run["started_at"],
                now,
                run.get("finished_at"),
                run["status"],
                run.get("topic"),
                run.get("consumer_group"),
                json.dumps(run.get("selected_projects", [])),
                summary.get("last_message_time"),
                summary.get("last_success_time"),
                summary.get("last_error_time"),
                run.get("error_summary"),
            ),
        )
        for project, counters in summary.get("projects", {}).items():
            self._cursor.execute(
                """
                INSERT INTO monitor_project_stats (
                    run_id, project, consumed, routed, filtered, succeeded,
                    skipped, failed, handles, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, project) DO UPDATE SET
                    consumed=excluded.consumed, routed=excluded.routed,
                    filtered=excluded.filtered, succeeded=excluded.succeeded,
                    skipped=excluded.skipped, failed=excluded.failed,
                    handles=excluded.handles, updated_at=excluded.updated_at
                """,
                (
                    run["run_id"],
                    project,
                    counters["consumed"],
                    counters["routed"],
                    counters["filtered"],
                    counters["succeeded"],
                    counters["skipped"],
                    counters["failed"],
                    counters["handles"],
                    now,
                ),
            )

    def close(self):
        if getattr(self, "_closed", False):
            return
        try:
            self._lock.acquire()
            try:
                self._cursor.close()
            except Exception:
                pass
            try:
                self._conn.close()
            except Exception:
                pass
        finally:
            self._closed = True
            self._lock.release()


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
            lambda: dict.fromkeys(("consumed", "routed", "filtered", "succeeded", "skipped", "failed", "handles"), 0)
        )
        self._run: dict | None = None
        self._heartbeat_thread: threading.Thread | None = None
        self._heartbeat_stop = threading.Event()
        self._lock = threading.RLock()

        # Logging control
        self.log_interval_seconds = log_interval_seconds or 10
        self.log_interval_messages = log_interval_messages or 100
        self._last_log_time = time.time()
        self._last_logged_messages = 0

        # Reporters
        self.reporters: list[StatsReporter] = [ConsoleReporter()]
        if enable_db and db_path:
            self.reporters.append(SQLiteReporter(db_path=db_path))

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
            lambda: dict.fromkeys(("consumed", "routed", "filtered", "succeeded", "skipped", "failed", "handles"), 0)
        )
        self._run = None

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
    ):
        """Reset counters and reporters for a fresh run, optionally enabling DB.

        This keeps reporter setup encapsulated and avoids leaking lifecycle
        details into callers like the consumer.
        """
        if self._run and not self._closed:
            self.close(status="stopped")
        self.reset()
        if log_interval_seconds is not None:
            self.log_interval_seconds = log_interval_seconds
        if log_interval_messages is not None:
            self.log_interval_messages = log_interval_messages
        if enable_db and db_path:
            try:
                self.reporters.append(SQLiteReporter(db_path=db_path))
            except Exception:
                logger.exception("Failed to initialize SQLiteReporter; continuing without DB reporter")
        if enable_db and any(isinstance(r, SQLiteReporter) for r in self.reporters):
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
                "finished_at": None,
                "error_summary": None,
            }
            for project in selected_projects:
                self._project_counters[project]
            self._heartbeat_once()
            self._start_heartbeat(heartbeat_interval_seconds)

    def _start_heartbeat(self, interval: float) -> None:
        self._heartbeat_stop.clear()

        def beat() -> None:
            while not self._heartbeat_stop.wait(max(0.1, interval)):
                self._heartbeat_once()

        self._heartbeat_thread = threading.Thread(target=beat, name="piddi-stats-heartbeat", daemon=True)
        self._heartbeat_thread.start()

    def _stop_heartbeat(self) -> None:
        self._heartbeat_stop.set()
        thread = self._heartbeat_thread
        if thread and thread is not threading.current_thread():
            thread.join(timeout=2)
        self._heartbeat_thread = None

    def _heartbeat_once(self) -> None:
        summary = self.summary()
        for reporter in list(self.reporters):
            if isinstance(reporter, SQLiteReporter):
                try:
                    reporter.heartbeat(summary)
                except Exception:
                    logger.exception("Failed to persist monitoring heartbeat")

    def record_result(self, result) -> None:
        """Record one processing result in aggregate and project-aware counters."""
        project = result.plugin or result.project or "unassigned"
        with self._lock:
            counters = self._project_counters[project]
            counters["consumed"] += 1
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
                self._last_success_time = datetime.datetime.now(datetime.UTC)
                self.tick()
                self.handle(
                    n=result.num_handles,
                    handle_time_sec=result.handle_processing_time,
                )
            else:
                counters["routed"] += 1
                counters["failed"] += 1
                self.error(message=result.error)

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
            logger.info(f"PATCHED: {message}")

    def external_fail(self, message: str | None = None, n=1):
        self.increment(CounterKey.EXTERNAL_FAILS, n)
        if message:
            logger.warning(f"EXTERNAL-FAIL: {message}")

    # --- Logging / persistence ---
    def _maybe_log(self):
        now = time.time()
        messages_since_last = self._counters[CounterKey.MESSAGES] - self._last_logged_messages

        if messages_since_last == 0:
            return

        if (now - self._last_log_time >= self.log_interval_seconds) or (messages_since_last >= self.log_interval_messages):
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
            projects = {name: dict(values) for name, values in self._project_counters.items()}
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
            self._heartbeat_once()
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
