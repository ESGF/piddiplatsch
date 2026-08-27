from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from typing import Any, Protocol

import requests

from piddiplatsch.exceptions import JsonlReadError
from piddiplatsch.handles.rest_backend import RestHandleClient
from piddiplatsch.helpers import find_jsonl, read_jsonl
from piddiplatsch.result import PublishResult


class PreparedHandleBackend(Protocol):
    prefix: str

    def add(self, pid: str, record: dict[str, Any]) -> Any: ...


ProgressCallback = Callable[[int, int, str, Exception | None], None]


@dataclass(frozen=True)
class _PublicationOutcome:
    index: int
    path: Path
    line_number: int
    handle: str
    error: Exception | None
    retry_attempts: int
    action: str = "published"
    url: str | None = None
    project: str | None = None
    dataset_id: str | None = None
    file_name: str | None = None


@dataclass(frozen=True)
class _PublicationContext:
    project: str | None
    dataset_id: str | None
    file_name: str | None


class HandlePublisher:
    """Publish prepared Handles from immutable JSONL files."""

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
        workers: int = 1,
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
        if workers < 1:
            raise ValueError("workers must be at least 1")

        files = find_jsonl(paths)
        result = PublishResult()
        self.logger.info(
            "Preparing Handle publication: files=%d offset=%d limit=%s retries=%d workers=%d",
            len(files),
            offset,
            limit if limit is not None else "none",
            retries,
            workers,
        )

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
                self.logger.error("Could not read Handle JSONL file %s: %s", path, exc)

        total_records = len(records)

        outcomes = self._publish_records(
            records,
            retries=retries,
            retry_delay=retry_delay,
            workers=workers,
            progress_callback=progress_callback,
        )
        for outcome in sorted(outcomes, key=lambda item: item.index):
            index = outcome.index
            result.total += 1
            result.retry_attempts += outcome.retry_attempts
            if outcome.error is None:
                result.succeeded += 1
                self._log_publication(
                    outcome,
                    position=offset + index,
                    batch_total=total_records,
                )
            else:
                result.failed += 1
                location = f"{outcome.path}:{outcome.line_number}"
                result.errors.append(f"{location}: {outcome.error}")
                self.logger.error(
                    "Could not publish %s (position=%d batch=%d/%d): %s",
                    location,
                    offset + index,
                    index,
                    total_records,
                    outcome.error,
                )

        self.logger.info(
            "Handle publication complete: published=%d total=%d failed=%d retries=%d offset=%d",
            result.succeeded,
            result.total,
            result.failed,
            result.retry_attempts,
            offset,
        )
        return result

    def _log_publication(
        self,
        outcome: _PublicationOutcome,
        *,
        position: int,
        batch_total: int,
    ) -> None:
        details = [
            f"handle={outcome.handle}",
            f"url={outcome.url or '-'}",
            f"project={outcome.project or '-'}",
            f"dataset_id={outcome.dataset_id or '-'}",
            f"file_name={outcome.file_name or '-'}",
            f"position={position}",
            f"batch={outcome.index}/{batch_total}",
        ]
        self.logger.info("%s handle %s", outcome.action.capitalize(), " ".join(details))

    def _publish_records(
        self,
        records: list[tuple[Path, int, dict[str, Any]]],
        *,
        retries: int,
        retry_delay: float,
        workers: int,
        progress_callback: ProgressCallback | None,
    ) -> list[_PublicationOutcome]:
        contexts = self._publication_contexts(records)
        indexed_records = [
            (index, path, line_number, record, contexts[index])
            for index, (path, line_number, record) in enumerate(records, start=1)
        ]
        if workers == 1 or len(indexed_records) < 2:

            def report(outcome: _PublicationOutcome) -> None:
                if progress_callback is not None:
                    progress_callback(
                        outcome.index,
                        len(indexed_records),
                        outcome.handle,
                        outcome.error,
                    )

            return self._publish_chain(
                indexed_records,
                retries=retries,
                retry_delay=retry_delay,
                on_outcome=report,
            )

        # Updates for one Handle form a chain and stay in source order. Separate
        # Handles can be sent concurrently without allowing an older state to
        # race past a newer state for the same PID.
        chains: dict[
            str,
            list[tuple[int, Path, int, dict[str, Any], _PublicationContext]],
        ] = {}
        for item in indexed_records:
            index, _, _, record, _ = item
            handle = record.get("handle")
            chain_key = handle if isinstance(handle, str) else f"<invalid:{index}>"
            chains.setdefault(chain_key, []).append(item)

        outcomes: list[_PublicationOutcome] = []
        outcome_queue: Queue[_PublicationOutcome | None] = Queue()

        def publish_chain(chain):
            try:
                self._publish_chain(
                    chain,
                    retries=retries,
                    retry_delay=retry_delay,
                    on_outcome=outcome_queue.put,
                )
            finally:
                outcome_queue.put(None)

        with ThreadPoolExecutor(
            max_workers=min(workers, len(chains)),
            thread_name_prefix="handle-publisher",
        ) as executor:
            futures = [
                executor.submit(publish_chain, chain) for chain in chains.values()
            ]
            completed_chains = 0
            while completed_chains < len(futures):
                outcome = outcome_queue.get()
                if outcome is None:
                    completed_chains += 1
                    continue
                outcomes.append(outcome)
                if progress_callback is not None:
                    progress_callback(
                        outcome.index,
                        len(indexed_records),
                        outcome.handle,
                        outcome.error,
                    )
            for future in as_completed(futures):
                future.result()
        return outcomes

    def _publish_chain(
        self,
        records: list[tuple[int, Path, int, dict[str, Any], _PublicationContext]],
        *,
        retries: int,
        retry_delay: float,
        on_outcome: Callable[[_PublicationOutcome], None] | None = None,
    ) -> list[_PublicationOutcome]:
        outcomes = []
        for index, path, line_number, record, context in records:
            handle = record.get("handle")
            retry_attempts = 0

            def count_retry() -> None:
                nonlocal retry_attempts
                retry_attempts += 1

            error: Exception | None = None
            write_result = None
            try:
                pid, handle_data = self._prepare_record(record)
                write_result = self._store_with_retries(
                    str(handle),
                    pid,
                    handle_data,
                    retries=retries,
                    retry_delay=retry_delay,
                    on_retry=count_retry,
                )
            except Exception as exc:
                error = exc
            outcome = _PublicationOutcome(
                index=index,
                path=path,
                line_number=line_number,
                handle=str(handle or "<invalid>"),
                error=error,
                retry_attempts=retry_attempts,
                action=getattr(write_result, "action", "published"),
                url=getattr(write_result, "url", self._record_url(str(handle))),
                project=context.project,
                dataset_id=context.dataset_id,
                file_name=context.file_name,
            )
            outcomes.append(outcome)
            if on_outcome is not None:
                on_outcome(outcome)
        return outcomes

    def _store_with_retries(
        self,
        handle: str,
        pid: str,
        handle_data: dict[str, Any],
        *,
        retries: int,
        retry_delay: float,
        on_retry: Callable[[], None],
    ) -> Any:
        for attempt in range(retries + 1):
            try:
                return self.backend.add(pid, handle_data)
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

    def _record_url(self, handle: str) -> str | None:
        record_url = getattr(self.backend, "record_url", None)
        return record_url(handle) if callable(record_url) else None

    @staticmethod
    def _publication_contexts(
        records: list[tuple[Path, int, dict[str, Any]]],
    ) -> dict[int, _PublicationContext]:
        dataset_by_handle: dict[str, str] = {}
        project_by_handle: dict[str, str] = {}

        for _, _, record in records:
            handle = record.get("handle")
            data = record.get("data")
            if not isinstance(handle, str) or not isinstance(data, dict):
                continue
            dataset_id = data.get("DATASET_ID")
            project = record.get("project")
            if isinstance(dataset_id, str) and dataset_id:
                dataset_by_handle[handle] = dataset_id
            if isinstance(project, str) and project:
                project_by_handle[handle] = project

        contexts: dict[int, _PublicationContext] = {}
        for index, (_, _, record) in enumerate(records, start=1):
            data = record.get("data")
            data = data if isinstance(data, dict) else {}
            parent = data.get("IS_PART_OF")
            if isinstance(parent, str) and parent.startswith("hdl:"):
                parent = parent.removeprefix("hdl:")
            if not isinstance(parent, str):
                parent = None

            dataset_id = data.get("DATASET_ID")
            if not isinstance(dataset_id, str) or not dataset_id:
                dataset_id = dataset_by_handle.get(parent or "")

            project = record.get("project")
            if not isinstance(project, str) or not project:
                project = project_by_handle.get(parent or "")
            if not project and dataset_id:
                project = dataset_id.split(".", 1)[0].lower()

            file_name = data.get("FILE_NAME")
            if not isinstance(file_name, str) or not file_name:
                file_name = None
            contexts[index] = _PublicationContext(
                project=project,
                dataset_id=dataset_id,
                file_name=file_name,
            )
        return contexts

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
            raise ValueError("Handle entry has no valid identifier")

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
            raise ValueError("Handle entry has no valid URL")
        if not isinstance(data, dict):
            raise ValueError("Handle data must be an object")

        return pid, {**data, "URL": url}
