import time
from contextlib import AbstractContextManager
from datetime import UTC, datetime

import humanize
from millify import millify
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from piddiplatsch.helpers import utc_now
from piddiplatsch.monitoring.stats import stats


class BaseProgress(AbstractContextManager):
    """Base class for progress display."""

    enabled = False

    def __enter__(self):
        self._logging_context = logging_redirect_tqdm() if self.enabled else None
        if self._logging_context is not None:
            self._logging_context.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self.close()
        finally:
            context = getattr(self, "_logging_context", None)
            if context is not None:
                context.__exit__(exc_type, exc_value, traceback)

    def set_phase(self, title: str, mode: str = "map") -> None:
        """Optional phase transition for multi-stage commands."""

    def refresh(self):
        raise NotImplementedError

    def close(self):
        pass


class NoOpProgress(BaseProgress):
    """Dummy progress display; does nothing."""

    def refresh(self):
        pass


class BoundedProgress(BaseProgress):
    """Track and optionally display progress for work with an optional total."""

    def __init__(self, *, title: str, unit: str, enabled: bool, start: int = 0) -> None:
        self.title = title
        self.unit = unit
        self.enabled = enabled
        self.start = start
        self.position = start
        self.succeeded = 0
        self.failed = 0
        self.bar = None

    def update(self, *, total: int | None, position: int, ok: bool) -> None:
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
                desc=(
                    f"{self.title} {self.start + 1}-{self.start + total}"
                    if total is not None
                    else self.title
                ),
                unit=self.unit,
                dynamic_ncols=True,
            )
        self.bar.set_postfix(
            position=self.position, ok=self.succeeded, failed=self.failed
        )
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

    enabled = True

    def __init__(self, title="progress", update_interval=5, *, mode=None):
        self.title = title
        self.mode = mode or (
            title if title in {"harvest", "map", "sync"} else "generic"
        )
        self.started_at = utc_now()
        self.update_interval = update_interval
        self.last_update = time.monotonic()
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
        if self.mode == "sync":
            return f"{self.title} | checking pending items and ready batches | elapsed:{self._format_elapsed(self.started_at)}"
        total = stats.messages + stats.errors
        rate = total / stats.uptime if stats.uptime > 0 else 0
        label = (
            "events"
            if self.mode.startswith("harvest")
            else "items" if self.mode == "map" else "messages"
        )
        fields = [self.title, f"{label}:{millify(total, precision=1)} ({rate:.2f}/s)"]
        if self.mode == "harvest":
            fields.append(f"writes:{millify(stats.catalog_writes, precision=1)}")
            optional = {
                "duplicates": stats.filtered_messages,
                "patches": stats.patched_messages,
                "refreshes": stats.catalog_refreshes,
            }
            fields.extend(
                f"{key}:{millify(value, precision=1)}"
                for key, value in optional.items()
                if value
            )
        elif not self.mode.startswith("harvest"):
            fields.append(
                f"handles:{millify(stats.handles, precision=1)} ({stats.handle_rate:.2f}/s)"
            )
            optional = {
                "filtered": stats.filtered_messages,
                "warnings": stats.warnings,
                "retracted": stats.retracted_messages,
                "replicas": stats.replicas,
                "skipped": stats.skipped_messages,
            }
            fields.extend(
                f"{key}:{millify(value, precision=1)}"
                for key, value in optional.items()
                if value
            )
        fields.append(f"errors:{millify(stats.errors, precision=1)}")
        if stats.errors:
            fields.append(f"last_error:{self._time_ago(stats.last_error_time)}")
        fields.append(f"elapsed:{self._format_elapsed(stats.start_time)}")
        return " | ".join(fields)

    def set_phase(self, title: str, mode: str = "map") -> None:
        self.title = title
        self.mode = mode
        self.last_update = time.monotonic()
        if self.closed:
            self.closed = False
            self.bar = tqdm(
                total=0,
                desc=self._format_desc(),
                bar_format="{desc}",
                dynamic_ncols=True,
            )
        else:
            self.bar.set_description_str(self._format_desc())

    def refresh(self):
        """Update the display from Stats."""
        if self.closed:
            return
        now = time.monotonic()
        if now - self.last_update >= self.update_interval:
            self.bar.set_description_str(self._format_desc())
            self.last_update = now

    def close(self):
        if self.closed:
            return
        self.bar.set_description_str(self._format_desc())
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
    mode: str | None = None,
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
        return Progress(title=title, update_interval=update_interval, mode=mode)
    return NoOpProgress()
