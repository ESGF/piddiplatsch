import json
import threading
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path


def utc_now() -> datetime:
    """Return current UTC time as a timezone-aware datetime."""
    return datetime.now(UTC)


class DailyJsonlWriter:
    """Utility for writing JSONL records to daily-rotated files.

    - Uses a directory (root) and a filename prefix (e.g., 'skipped_items').
    - Appends one JSON object per line.
    - Returns the path of the written file.
    """

    def __init__(self, root_dir: Path):
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    @staticmethod
    def wrap_with_infos(data: dict, infos: dict) -> dict:
        # Merge metadata under `__infos__` key
        wrapped = {**data, "__infos__": infos}
        return wrapped

    def write(
        self,
        prefix: str,
        data: dict,
        subdir: Path | None = None,
        *,
        filename: str | None = None,
    ) -> Path:
        now = utc_now()
        dated_filename = filename or f"{prefix}_{now.date()}.jsonl"
        target_dir = Path(subdir) if subdir else self.root_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / dated_filename
        line = json.dumps(data) + "\n"
        with self._lock, target_path.open("a", encoding="utf-8") as f:
            # Write a complete JSONL record in one operation so threads sharing
            # this writer cannot interleave json.dump() fragments.
            f.write(line)
        return target_path


def find_jsonl(paths: Iterable[Path]) -> list[Path]:
    """Resolve a sequence of files/dirs/globs to a sorted unique list of JSONL file paths."""
    files: set[Path] = set()
    for path in paths:
        if path.is_file():
            if path.suffix == ".jsonl":
                files.add(path)
        elif path.is_dir():
            files.update(p for p in path.glob("*.jsonl") if p.is_file())
        else:
            parent = path.parent
            pattern = path.name
            files.update(
                p for p in parent.glob(pattern) if p.is_file() and p.suffix == ".jsonl"
            )
    return sorted(files)
