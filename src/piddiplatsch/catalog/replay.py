"""Replay coordinate-bearing raw archives through the harvest processor."""

import gzip
import json

from piddiplatsch.core.messages import KafkaSource, SourceMessage


def archived_messages(paths):
    """Numbered daily parts sort deterministically; checkpoints deduplicate replay."""
    for path in sorted(paths):
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
