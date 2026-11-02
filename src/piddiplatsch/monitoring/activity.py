"""Keep batch lifecycle activity visible independently of processing counters."""

import logging
import os
import socket
import threading
from contextlib import contextmanager
from datetime import UTC, datetime
from uuid import uuid4

from piddiplatsch.config import config
from piddiplatsch.monitoring.stats import StatusReporter, concise_error

LOGGER = logging.getLogger(__name__)


class BatchActivity:
    """A heartbeat spanning preparation, mapping, sealing, and publication.

    Mapping and publication own their counter runs. This separate activity run
    survives their transitions without counting records or changing their status.
    """

    def __init__(self, project, reporter, interval):
        self.reporter = reporter
        self.interval = max(0.1, interval)
        self.stop = threading.Event()
        self.lock = threading.RLock()
        self.closed = False
        self.run = {
            "run_id": uuid4().hex,
            "command": "batch",
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
            "started_at": datetime.now(UTC).isoformat(),
            "status": "running",
            "selected_projects": [project],
            "phase": "preparing",
            "finished_at": None,
            "error_summary": None,
        }
        self.thread = threading.Thread(
            target=self._beat, name="piddi-batch-heartbeat", daemon=True
        )

    def _heartbeat(self):
        try:
            # No counters: this record describes activity, not throughput.
            self.reporter.heartbeat({"run": dict(self.run), "projects": {}})
        except Exception:
            LOGGER.exception("Failed to persist batch activity heartbeat")

    def start(self):
        self._heartbeat()
        self.thread.start()

    def _beat(self):
        while not self.stop.wait(self.interval):
            with self.lock:
                if not self.closed:
                    self._heartbeat()

    def set_phase(self, phase):
        with self.lock:
            if not self.closed:
                self.run["phase"] = phase
                self._heartbeat()

    def close(self, error=None):
        self.stop.set()
        self.thread.join()
        with self.lock:
            self.run["status"] = "failed" if error is not None else "completed"
            self.run["finished_at"] = datetime.now(UTC).isoformat()
            self.run["error_summary"] = (
                concise_error(str(error) or type(error).__name__)
                if error is not None
                else None
            )
            self._heartbeat()
            self.closed = True
            self.reporter.close()


@contextmanager
def batch_activity(project):
    settings = config.get("stats", {})
    if not settings.get("enable_db", False):
        yield None
        return
    try:
        reporter = StatusReporter()
    except Exception:
        LOGGER.exception("Failed to initialize batch activity monitoring")
        yield None
        return
    activity = BatchActivity(
        project, reporter, settings.get("heartbeat_interval_seconds", 5)
    )
    activity.start()
    try:
        yield activity
    except BaseException as exc:
        activity.close(error=exc)
        raise
    else:
        activity.close()
