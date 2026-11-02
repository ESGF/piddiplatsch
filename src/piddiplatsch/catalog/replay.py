"""Replay coordinate-bearing raw archives through the harvest processor."""

import gzip
import json
from pathlib import Path

from piddiplatsch.core.messages import KafkaSource, SourceMessage


def archive_paths(paths):
    """Expand backup directories without mixing in outputs or unknown-project logs."""
    files = set()
    for entry in paths:
        path = Path(entry)
        if path.is_dir():
            parts = [
                part
                for part in path.glob("dump_messages_*.jsonl*")
                if part.is_file()
                and (part.name.endswith(".jsonl") or part.name.endswith(".jsonl.gz"))
            ]
            if not parts:
                raise ValueError(
                    f"No dump_messages_*.jsonl or .jsonl.gz files in archive directory: {path}"
                )
            files.update(parts)
        else:
            if not path.is_file():
                raise ValueError(f"Archive file does not exist: {path}")
            files.add(path)
    return tuple(
        sorted(files, key=lambda path: (path.name.removesuffix(".gz"), str(path)))
    )


def archived_messages(paths):
    """Numbered daily parts sort deterministically; checkpoints deduplicate replay."""
    for path in archive_paths(paths):
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, 1):
                try:
                    record = json.loads(line)
                    source = KafkaSource(**record["source"])
                    payload = record["payload"]
                    if not isinstance(payload, dict):
                        raise ValueError("Archived payload must be an object")
                    yield SourceMessage(record.get("key"), payload, source)
                except (ValueError, KeyError, TypeError) as exc:
                    raise ValueError(
                        f"Invalid archive record {path}:{line_number}: {exc}"
                    ) from exc
