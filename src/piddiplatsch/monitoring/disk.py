"""Shared, throttled disk checks without creating output directories."""

import logging
import shutil
import threading
import time
from functools import lru_cache
from pathlib import Path

from piddiplatsch.config import config
from piddiplatsch.config.schema import DiskConfig
from piddiplatsch.exceptions import DiskSpaceError

LOGGER = logging.getLogger(__name__)
GIB = 1024**3


def storage_paths() -> tuple[Path, ...]:
    output = Path(config.get("consumer", "output_dir", "outputs"))
    paths = [
        output,
        Path(config.get("catalog", "directory") or output / "catalog"),
        Path(config.get("batch", "directory") or output / "batches"),
    ]
    value = config.get("stats", "db_path")
    if value and config.get("stats", "enable_db", True):
        paths.append(Path(value).parent)
    return tuple(sorted({path.resolve() for path in paths}))


def disk_status(paths=None, settings=None) -> list[dict]:
    settings = settings or DiskConfig.model_validate(config.get("disk", {}))
    rows = []
    filesystems = {}
    for path in storage_paths() if paths is None else paths:
        row = {"path": str(path), "paths": [str(path)], "state": "unknown"}
        try:
            existing = Path(path).resolve()
            while not existing.exists():
                existing = existing.parent
            device = existing.stat().st_dev
            if device in filesystems:
                filesystems[device]["paths"].append(str(path))
                continue
            usage = shutil.disk_usage(existing)
            state = (
                "stop"
                if usage.free < settings.stop_free_gib * GIB
                else (
                    "warning"
                    if usage.free < settings.warn_free_gib * GIB
                    else "healthy"
                )
            )
            row.update(total_bytes=usage.total, free_bytes=usage.free, state=state)
            filesystems[device] = row
        except OSError as exc:
            row["error"] = str(exc)
        rows.append(row)
    return rows


class DiskGuard:
    def __init__(self, paths, settings: DiskConfig):
        self.paths = paths
        self.settings = settings
        self._checked_at = float("-inf")
        self._error = None
        self._warned = set()
        self._lock = threading.Lock()

    def check(self, *, force=False):
        if not self.settings.enabled:
            return
        with self._lock:
            now = time.monotonic()
            if force or now - self._checked_at >= self.settings.check_interval_seconds:
                previous_error = self._error
                self._error = None
                for row in disk_status(self.paths, self.settings):
                    path = row["path"]
                    if row["state"] == "unknown":
                        self._error = (
                            f"Cannot check free disk space for {path}: {row['error']}"
                        )
                    elif row["state"] == "stop":
                        self._error = (
                            f"Disk space low at {path}: {row['free_bytes'] / GIB:.1f} GiB free, "
                            f"below the {self.settings.stop_free_gib:g} GiB stop threshold. "
                            "Processing stopped; free space before restarting."
                        )
                    elif row["state"] == "warning":
                        if path not in self._warned:
                            LOGGER.warning(
                                "Disk space warning at %s: %.1f GiB free (warning threshold %g GiB)",
                                path,
                                row["free_bytes"] / GIB,
                                self.settings.warn_free_gib,
                            )
                            self._warned.add(path)
                    elif path in self._warned:
                        LOGGER.info("Disk space recovered at %s", path)
                        self._warned.remove(path)
                if self._error and previous_error is None:
                    LOGGER.error("%s", self._error)
                self._checked_at = now
            if self._error:
                raise DiskSpaceError(self._error)


@lru_cache(maxsize=16)
def _guard(paths, settings_json):
    return DiskGuard(paths, DiskConfig.model_validate_json(settings_json))


def get_disk_guard(*, extra_paths=()):
    settings = DiskConfig.model_validate(config.get("disk", {}))
    paths = tuple(
        sorted({*storage_paths(), *(Path(path).resolve() for path in extra_paths)})
    )
    return _guard(paths, settings.model_dump_json())


def check_disk_space(*, extra_paths=(), force=False):
    get_disk_guard(extra_paths=extra_paths).check(force=force)
