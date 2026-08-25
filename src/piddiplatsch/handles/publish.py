from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, Protocol

from piddiplatsch.exceptions import JsonlReadError
from piddiplatsch.handles.rest_backend import RestHandleClient
from piddiplatsch.helpers import find_jsonl, read_jsonl
from piddiplatsch.result import PublishResult


class PreparedHandleBackend(Protocol):
    prefix: str

    def add(self, pid: str, record: dict[str, Any]) -> None: ...


ProgressCallback = Callable[[int, int, str, Exception | None], None]


class HandlePublisher:
    """Publish prepared Handle records from immutable JSONL files."""

    def __init__(self, backend: PreparedHandleBackend | None = None) -> None:
        # Publication is deliberately independent of [handle].backend. This allows
        # a consumer configured for JSONL output to use the same config file.
        self.backend = backend or RestHandleClient.from_config()
        self.logger = logging.getLogger(__name__)

    def run(
        self,
        paths: Iterable[Path],
        *,
        progress_callback: ProgressCallback | None = None,
    ) -> PublishResult:
        files = find_jsonl(paths)
        result = PublishResult()

        records: list[tuple[Path, int, dict[str, Any]]] = []
        for path in files:
            try:
                records.extend(
                    (path, line_number, record)
                    for line_number, record in enumerate(read_jsonl(path), start=1)
                )
            except (JsonlReadError, OSError) as exc:
                result.failed += 1
                result.total += 1
                result.errors.append(str(exc))

        total_records = len(records)
        for index, (path, line_number, record) in enumerate(records, start=1):
            result.total += 1
            handle = record.get("handle")
            error: Exception | None = None
            try:
                pid, handle_data = self._prepare_record(record)
                self.backend.add(pid, handle_data)
                result.succeeded += 1
            except Exception as exc:
                error = exc
                result.failed += 1
                location = f"{path}:{line_number}"
                result.errors.append(f"{location}: {exc}")
                self.logger.error("Could not publish %s: %s", location, exc)

            if progress_callback is not None:
                progress_callback(
                    index, total_records, str(handle or "<invalid>"), error
                )

        return result

    def _prepare_record(self, record: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        handle = record.get("handle")
        if not isinstance(handle, str) or "/" not in handle:
            raise ValueError("record has no valid handle")

        prefix, pid = handle.split("/", 1)
        if prefix != self.backend.prefix:
            raise ValueError(
                f"handle prefix {prefix!r} does not match configured prefix {self.backend.prefix!r}"
            )
        if not pid:
            raise ValueError("handle has an empty suffix")

        url = record.get("URL")
        data = record.get("data")
        if not isinstance(url, str) or not url:
            raise ValueError("record has no valid URL")
        if not isinstance(data, dict):
            raise ValueError("record data must be an object")

        return pid, {**data, "URL": url}
