import json
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

from piddiplatsch.exceptions import JsonlReadError


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

    @staticmethod
    def wrap_with_infos(data: dict, infos: dict) -> dict:
        # Merge metadata under `__infos__` key
        wrapped = {**data, "__infos__": infos}
        return wrapped

    def write(self, prefix: str, data: dict, subdir: Path | None = None) -> Path:
        now = utc_now()
        dated_filename = f"{prefix}_{now.date()}.jsonl"
        target_dir = Path(subdir) if subdir else self.root_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / dated_filename
        with target_path.open("a", encoding="utf-8") as f:
            json.dump(data, f)
            f.write("\n")
        return target_path


def read_jsonl(
    file_path: Path,
    limit: int | None = None,
    offset: int = 0,
) -> list[dict]:
    """Read a bounded window of JSONL records without dropping malformed input."""
    if limit is not None and limit < 1:
        raise ValueError("limit must be at least 1")
    if offset < 0:
        raise ValueError("offset cannot be negative")
    if not file_path.exists():
        return []
    records: list[dict] = []
    record_number = 0
    with file_path.open("r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise JsonlReadError(
                    f"Malformed JSON in {file_path} at line {line_number}: {exc.msg}"
                ) from exc
            if not isinstance(record, dict):
                raise JsonlReadError(
                    f"Expected a JSON object in {file_path} at line {line_number}"
                )
            record_number += 1
            if record_number <= offset:
                continue
            records.append(record)
            if limit is not None and len(records) >= limit:
                break
    return records


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
