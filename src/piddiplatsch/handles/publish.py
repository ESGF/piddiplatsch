from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, Protocol

import requests

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

    def __init__(
        self,
        backend: PreparedHandleBackend | None = None,
        *,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        # Publication is deliberately independent of [handle].backend. This allows
        # a consumer configured for JSONL output to use the same config file.
        self.backend = backend or RestHandleClient.from_config()
        self.sleep = sleep
        self.logger = logging.getLogger(__name__)

    def run(
        self,
        paths: Iterable[Path],
        *,
        limit: int | None = None,
        offset: int = 0,
        retries: int = 0,
        retry_delay: float = 1.0,
        progress_callback: ProgressCallback | None = None,
    ) -> PublishResult:
        if limit is not None and limit < 1:
            raise ValueError("limit must be at least 1")
        if offset < 0:
            raise ValueError("offset cannot be negative")
        if retries < 0:
            raise ValueError("retries cannot be negative")
        if retry_delay < 0:
            raise ValueError("retry delay cannot be negative")

        files = find_jsonl(paths)
        result = PublishResult()

        records: list[tuple[Path, int, dict[str, Any]]] = []
        offset_remaining = offset
        for path in files:
            remaining = None if limit is None else limit - len(records) - result.failed
            if remaining == 0:
                break
            try:
                file_offset = 0
                if offset_remaining:
                    skipped = read_jsonl(path, limit=offset_remaining)
                    file_offset = len(skipped)
                    offset_remaining -= file_offset
                    if offset_remaining:
                        continue

                records.extend(
                    (path, file_offset + record_number, record)
                    for record_number, record in enumerate(
                        read_jsonl(path, limit=remaining, offset=file_offset), start=1
                    )
                )
            except (JsonlReadError, OSError) as exc:
                result.failed += 1
                result.total += 1
                result.errors.append(str(exc))

        total_records = len(records)

        def count_retry() -> None:
            result.retry_attempts += 1

        for index, (path, line_number, record) in enumerate(records, start=1):
            result.total += 1
            handle = record.get("handle")
            error: Exception | None = None
            try:
                pid, handle_data = self._prepare_record(record)
                self._store_with_retries(
                    str(handle),
                    pid,
                    handle_data,
                    retries=retries,
                    retry_delay=retry_delay,
                    on_retry=count_retry,
                )
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

    def _store_with_retries(
        self,
        handle: str,
        pid: str,
        handle_data: dict[str, Any],
        *,
        retries: int,
        retry_delay: float,
        on_retry: Callable[[], None],
    ) -> None:
        for attempt in range(retries + 1):
            try:
                self.backend.add(pid, handle_data)
                return
            except Exception as exc:
                if attempt == retries or not self._is_retryable(exc):
                    raise
                on_retry()
                delay = min(retry_delay * (2**attempt), 60.0)
                self.logger.warning(
                    "Retrying handle %s after %s: attempt %d/%d in %.1fs",
                    handle,
                    exc,
                    attempt + 1,
                    retries,
                    delay,
                )
                self.sleep(delay)

    @staticmethod
    def _is_retryable(exc: Exception) -> bool:
        if isinstance(exc, requests.ConnectionError | requests.Timeout):
            return True
        if not isinstance(exc, requests.HTTPError):
            return False

        response = exc.response
        if response is None:
            return True
        status_code = response.status_code
        return status_code in (408, 425, 429) or (
            status_code is not None and status_code >= 500
        )

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
