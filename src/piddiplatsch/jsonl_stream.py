"""Single-pass JSONL readers shared by file-processing commands."""

import json
from collections.abc import Iterable, Iterator
from contextlib import closing
from itertools import islice
from pathlib import Path
from typing import Any

from piddiplatsch.exceptions import JsonlReadError

JsonlRecord = tuple[Path, int, dict[str, Any] | JsonlReadError]


def iter_jsonl_records(
    paths: Iterable[Path],
    *,
    offset: int = 0,
    limit: int | None = None,
    yield_errors: bool = False,
) -> Iterator[JsonlRecord]:
    """Yield a global window of records with their physical source locations.

    Paths are consumed in caller-provided order and each file is opened once.
    Blank lines do not count towards offset or limit. Selected malformed records
    raise by default; ``yield_errors`` lets publishers receipt them and continue.
    Close the iterator when stopping early to release the current input file.
    """
    if offset < 0:
        raise ValueError("offset cannot be negative")
    if limit is not None and limit < 1:
        raise ValueError("limit must be at least 1")
    with closing(_read_records(paths)) as source:
        for path, line_number, record in islice(
            source, offset, None if limit is None else offset + limit
        ):
            if isinstance(record, JsonlReadError) and not yield_errors:
                raise record
            yield path, line_number, record


def _read_records(paths: Iterable[Path]) -> Iterator[JsonlRecord]:
    for path in paths:
        line_number = 0
        try:
            with path.open(encoding="utf-8") as stream:
                for line_number, line in enumerate(stream, start=1):
                    if not line.strip():
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError as exc:
                        record = JsonlReadError(
                            f"Malformed JSON in {path} at line {line_number}: {exc.msg}"
                        )
                    if not isinstance(record, (dict, JsonlReadError)):
                        record = JsonlReadError(
                            f"Expected a JSON object in {path} at line {line_number}"
                        )
                    yield path, line_number, record
        except (OSError, UnicodeError) as exc:
            yield path, line_number, JsonlReadError(f"Could not read {path}: {exc}")
