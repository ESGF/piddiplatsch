import fcntl
import json
import logging
import os
import re
from pathlib import Path

from piddiplatsch.config import config
from piddiplatsch.helpers import DailyJsonlWriter, utc_now
from piddiplatsch.persist.base import RecorderBase
from piddiplatsch.result import PrepareResult


class _DumpWriter(DailyJsonlWriter):
    """Append numbered daily parts, resuming counts after a restart."""

    def __init__(self, root_dir: Path, max_events: int):
        super().__init__(root_dir)
        if max_events <= 0:
            raise ValueError("dump_max_events must be positive")
        self.max_events = max_events
        self._day = None
        self._part = 1
        self._size = None
        self._count = 0

    def write(self, prefix: str, data: dict, subdir: Path | None = None) -> Path:
        line = json.dumps(data) + "\n"
        # Serialize writers to the same archive, including separate processes.
        with self._lock, (self.root_dir / ".dump.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            day = utc_now().date()
            stem = f"{prefix}_{day}"
            if self._day != day:
                pattern = re.compile(rf"{re.escape(stem)}_(\d+)\.jsonl(?:\.gz)?")
                parts = [
                    int(match[1])
                    for path in self.root_dir.glob(f"{stem}_*.jsonl*")
                    if (match := pattern.fullmatch(path.name))
                ]
                self._day, self._part = day, max(parts, default=1)
                self._size = None
            while True:
                path = self.root_dir / f"{stem}_{self._part:06d}.jsonl"
                size = path.stat().st_size if path.exists() else 0
                if self._size != size:
                    self._count = 0
                    if size:
                        with path.open("rb") as stream:
                            for record in stream:
                                if not record.endswith(b"\n"):
                                    raise OSError(
                                        f"Incomplete last event in dump: {path}"
                                    )
                                self._count += 1
                if (
                    self._count < self.max_events
                    and not path.with_suffix(".jsonl.gz").exists()
                ):
                    break
                self._part += 1
                self._size = None
            with path.open("a", encoding="utf-8") as stream:
                stream.write(line)
                stream.flush()
                os.fsync(stream.fileno())
            descriptor = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            self._count += 1
            self._size = path.stat().st_size
            return path


class DumpRecorder(RecorderBase):
    LOG_KIND = "dump"
    LOG_LEVEL = logging.DEBUG
    DUMP_DIR: Path | None = None

    def __init__(
        self, root_dir: Path | None = None, *, prefix: str = "dump_messages"
    ) -> None:
        configured_dir = (
            Path(config.get("consumer", {}).get("output_dir", "outputs")) / "dump"
        )
        super().__init__(root_dir or self.DUMP_DIR or configured_dir, prefix)
        self.writer = _DumpWriter(
            self.root_dir, int(config.get("consumer", "dump_max_events", 100000))
        )

    def prepare(
        self,
        key: str,
        data: dict,
        reason: str | None,
        retries: int | None,
    ) -> PrepareResult:
        # No infos, just write raw payload
        return PrepareResult(payload=data)
