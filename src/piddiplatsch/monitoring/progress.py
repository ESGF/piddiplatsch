import time
from contextlib import AbstractContextManager
from datetime import UTC, datetime

import humanize
from millify import millify
from tqdm import tqdm

from piddiplatsch.helpers import utc_now
from piddiplatsch.monitoring.stats import stats


class BaseProgress(AbstractContextManager):
    """Base class for progress display."""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def refresh(self):
        raise NotImplementedError

    def close(self):
        pass


class NoOpProgress(BaseProgress):
    """Dummy progress display; does nothing."""

    def refresh(self):
        pass


class BoundedProgress(BaseProgress):
    """Track and optionally display progress for work with a known total."""

    def __init__(self, *, title: str, unit: str, enabled: bool, start: int = 0) -> None:
        self.title = title
        self.unit = unit
        self.enabled = enabled
        self.start = start
        self.position = start
        self.succeeded = 0
        self.failed = 0
        self.bar = None

    def update(self, *, total: int, position: int, ok: bool) -> None:
        """Record one completed item and refresh the display when enabled."""
        self.position = max(self.position, position)
        if ok:
            self.succeeded += 1
        else:
            self.failed += 1

        if not self.enabled:
            return
        if self.bar is None:
            self.bar = tqdm(
                total=total,
                desc=f"{self.title} {self.start + 1}-{self.start + total}",
                unit=self.unit,
                dynamic_ncols=True,
            )
        self.bar.set_postfix(position=self.position, ok=self.succeeded, failed=self.failed)
        self.bar.update(1)

    def refresh(self) -> None:
        if self.bar is not None:
            self.bar.refresh()

    def close(self) -> None:
        if self.bar is not None:
            self.bar.close()
            self.bar = None


class Progress(BaseProgress):
    """Displays concise message stats in the console (tqdm-based) with timestamps and total runtime."""

    def __init__(self, title="progress", update_interval=5):
        self.title = title
        self.update_interval = update_interval
        self.last_update = time.time()
        self.closed = False

        self.bar = tqdm(
            total=0,  # ticker mode, no total
            desc=self._format_desc(),
            bar_format="{desc}",
            dynamic_ncols=True,
        )

    def _to_utc_dt(self, ts):
        if ts is None:
            return None
        if isinstance(ts, float):
            return datetime.fromtimestamp(ts, tz=UTC)
        if ts.tzinfo is None:
            return ts.replace(tzinfo=UTC)
        return ts

    def _format_time(self, ts):
        dt = self._to_utc_dt(ts)
        return dt.strftime("%H:%M:%S") if dt else "--:--:--"

    def _time_ago(self, ts):
        dt = self._to_utc_dt(ts)
        if dt is None:
            return "--"
        return humanize.naturaltime(utc_now() - dt)

    def _format_elapsed(self, start_ts):
        start_dt = self._to_utc_dt(start_ts)
        if start_dt is None:
            return "--:--:--"
        elapsed = int((utc_now() - start_dt).total_seconds())
        h, rem = divmod(elapsed, 3600)
        m, s = divmod(rem, 60)
        return f"{h:02}:{m:02}:{s:02}"

    def _format_desc(self):
        # Short labels: msg=messages, hdl=handles, E=errors, F=filtered,
        # W=warn, D=retracted, replica=replicas, skip=skipped, patch=patched
        return (
            f"{self.title:<8}"
            f"| msg:{millify(stats.messages, precision=1)} ({stats.message_rate:.2f}/s)"
            f"| hdl:{millify(stats.handles, precision=1)} ({stats.handle_rate:.2f}/s)"
            f"| E:{millify(stats.errors, precision=1)}"
            f"| F:{millify(stats.filtered_messages, precision=1)}"
            f"| W:{millify(stats.warnings, precision=1)}"
            f"| D:{millify(stats.retracted_messages, precision=1)}"
            f"| replica:{millify(stats.replicas, precision=1)}"
            f"| skip:{millify(stats.skipped_messages, precision=1)}"
            f"| patch:{millify(stats.patched_messages, precision=1)}"
            f"| last_err:{self._time_ago(stats.last_error_time)} "
            f"| ⏱ {self._format_elapsed(stats.start_time)}"
        )

    def refresh(self):
        """Update the display from Stats."""
        if self.closed:
            return
        now = time.time()
        if now - self.last_update >= self.update_interval:
            self.bar.set_description(self._format_desc())
            self.last_update = now

    def close(self):
        if self.closed:
            return
        self.bar.set_description(self._format_desc())
        self.bar.close()
        self.closed = True


def get_progress(
    title="progress",
    use_tqdm=False,
    update_interval=5,
    *,
    stream: bool = True,
    unit: str = "item",
    start: int = 0,
) -> BaseProgress:
    """Create streaming or bounded progress, optionally without rendering."""
    if not stream:
        return BoundedProgress(
            title=title,
            unit=unit,
            enabled=use_tqdm,
            start=start,
        )
    if use_tqdm:
        return Progress(title=title, update_interval=update_interval)
    return NoOpProgress()
