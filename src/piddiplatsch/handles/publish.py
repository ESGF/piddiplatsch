from __future__ import annotations

import json
import logging
import re
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import AbstractContextManager, ExitStack, closing
from dataclasses import dataclass, replace
from pathlib import Path
from queue import Queue
from itertools import islice
from typing import Any, Protocol

import requests

from piddiplatsch.config import config
from piddiplatsch.core.plugin import normalize_project_id
from piddiplatsch.exceptions import JsonlReadError
from piddiplatsch.handles.rest_backend import RestHandleClient
from piddiplatsch.helpers import find_jsonl, utc_now
from piddiplatsch.result import ProjectPublishResult, PublishResult


class PreparedHandleBackend(Protocol):
    prefix: str

    def add(self, pid: str, record: dict[str, Any]) -> Any: ...


BATCH_SIZE = 256
InputRecord = tuple[Path, int, dict[str, Any] | Exception]
ProgressCallback = Callable[[int, int | None, str, Exception | None], None]
OutcomeCallback = Callable[["_PublicationOutcome"], None]


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


class _PublicationResultWriter(AbstractContextManager):
    """Append structured per-Handle outcomes to one run-scoped JSONL file."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._stream = self.path.open("a", encoding="utf-8")
        self._lock = threading.Lock()

    def write(self, record: dict[str, Any]) -> None:
        line = json.dumps(record) + "\n"
        with self._lock:
            self._stream.write(line)
            self._stream.flush()

    def close(self) -> None:
        self._stream.close()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()


class HandlePublisher:
    """Publish prepared Handles from immutable JSONL files."""

    def __init__(
        self,
        backend: PreparedHandleBackend | None = None,
        *,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        # Publication always uses REST, independently of the selected profile's
        # legacy backend setting. Configured clients are resolved lazily after
        # the input project is known.
        self.backend = backend
        self._uses_config = backend is None
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
        result_file: Path | None = None,
        project: str | None = None,
        handle_profile: str | None = None,
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

        selected_project = normalize_project_id(project or "") or None
        if project is not None and selected_project is None:
            raise ValueError("project must not be empty")
        validate_project = project is not None or self._uses_config
        backend_ready = not self._uses_config
        result.project = selected_project
        completed = 0
        with ExitStack() as stack:
            source = stack.enter_context(closing(self._read_records(files)))
            records = islice(source, offset, None if limit is None else offset + limit)
            writer = None
            while batch := list(islice(records, BATCH_SIZE)):
                contexts = self._publication_contexts(batch)
                if not backend_ready and any(
                    isinstance(record, dict) for _, _, record in batch
                ):
                    if selected_project is None:
                        selected_project = next(
                            (
                                contexts[index].project
                                for index, (_, _, record) in enumerate(batch, start=1)
                                if isinstance(record, dict)
                            ),
                            None,
                        )
                    self.backend = RestHandleClient.from_config(
                        project=selected_project, profile=handle_profile
                    )
                    result.project = selected_project
                    backend_ready = True
                if writer is None:
                    result.result_file = result_file or self._new_result_file(
                        result.project
                    )
                    self.logger.info(
                        "Writing publication results to %s", result.result_file
                    )
                    writer = stack.enter_context(
                        _PublicationResultWriter(result.result_file)
                    )

                def report(outcome, batch_start=completed, receipt_writer=writer):
                    outcome = replace(outcome, index=batch_start + outcome.index)
                    self._record_outcome(result, receipt_writer, outcome, offset=offset)
                    if progress_callback is not None:
                        progress_callback(
                            outcome.index, None, outcome.handle, outcome.error
                        )

                # Wait for all writes in a batch before reading the next batch.
                # Within each batch, updates for the same PID form an ordered chain.
                self._publish_records(
                    batch,
                    retries=retries,
                    retry_delay=retry_delay,
                    workers=workers,
                    contexts=contexts,
                    outcome_callback=report,
                    expected_project=selected_project,
                    validate_project=validate_project,
                )
                completed += len(batch)
        if not validate_project:
            names = set(result.projects)
            result.project = (
                next(iter(names))
                if len(names) == 1 and "unknown" not in names
                else None
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

    @staticmethod
    def _remember_error(result: PublishResult, error: str) -> None:
        # Full publication errors are streamed to receipts and logs. Avoid a
        # second unbounded collection when a service rejects a large run.
        if len(result.errors) < 100:
            result.errors.append(error)

    def _record_outcome(
        self,
        result: PublishResult,
        writer: _PublicationResultWriter,
        outcome: _PublicationOutcome,
        *,
        offset: int,
    ) -> None:
        writer.write(
            self._result_record(
                outcome, position=offset + outcome.index, batch_total=None
            )
        )
        result.total += 1
        result.retry_attempts += outcome.retry_attempts
        project_result = result.projects.setdefault(
            outcome.project or "unknown", ProjectPublishResult()
        )
        project_result.total += 1
        if outcome.error is None:
            result.succeeded += 1
            project_result.succeeded += 1
            self._log_publication(
                outcome, position=offset + outcome.index, batch_total=None
            )
        else:
            result.failed += 1
            project_result.failed += 1
            location = f"{outcome.path}:{outcome.line_number}"
            self._remember_error(result, f"{location}: {outcome.error}")
            self.logger.error(
                "Could not publish %s (position=%d): %s",
                location,
                offset + outcome.index,
                outcome.error,
            )

    @staticmethod
    def _read_records(files: Iterable[Path]) -> Iterator[InputRecord]:
        """Open each source once and retain physical line numbers for receipts."""
        for path in files:
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
                        if not isinstance(record, (dict, Exception)):
                            record = JsonlReadError(
                                f"Expected a JSON object in {path} at line {line_number}"
                            )
                        yield path, line_number, record
            except (OSError, UnicodeError) as exc:
                yield path, line_number, JsonlReadError(f"Could not read {path}: {exc}")

    def _log_publication(
        self,
        outcome: _PublicationOutcome,
        *,
        position: int,
        batch_total: int | None,
    ) -> None:
        details = [
            f"handle={outcome.handle}",
            f"url={outcome.url or '-'}",
            f"project={outcome.project or '-'}",
            f"dataset_id={outcome.dataset_id or '-'}",
            f"file_name={outcome.file_name or '-'}",
            f"position={position}",
            f"batch={outcome.index}/{batch_total if batch_total is not None else '?'}",
        ]
        self.logger.info("%s handle %s", outcome.action.capitalize(), " ".join(details))

    def _publish_records(
        self,
        records: list[InputRecord],
        *,
        retries: int,
        retry_delay: float,
        workers: int,
        contexts: dict[int, _PublicationContext],
        outcome_callback: OutcomeCallback,
        expected_project: str | None,
        validate_project: bool,
    ) -> None:
        indexed_records = [
            (index, path, line_number, record, contexts[index])
            for index, (path, line_number, record) in enumerate(records, start=1)
        ]
        if workers == 1 or len(indexed_records) < 2:
            return self._publish_chain(
                indexed_records,
                retries=retries,
                retry_delay=retry_delay,
                on_outcome=outcome_callback,
                expected_project=expected_project,
                validate_project=validate_project,
            )

        # Updates for one Handle form a chain and stay in source order. Separate
        # Handles can be sent concurrently without allowing an older state to
        # race past a newer state for the same PID.
        chains: dict[
            str,
            list[
                tuple[int, Path, int, dict[str, Any] | Exception, _PublicationContext]
            ],
        ] = {}
        for item in indexed_records:
            index, _, _, record, _ = item
            handle = record.get("handle") if isinstance(record, dict) else None
            chain_key = handle if isinstance(handle, str) else f"<invalid:{index}>"
            chains.setdefault(chain_key, []).append(item)

        outcome_queue: Queue[_PublicationOutcome | None] = Queue()

        def publish_chain(chain):
            try:
                self._publish_chain(
                    chain,
                    retries=retries,
                    retry_delay=retry_delay,
                    on_outcome=outcome_queue.put,
                    expected_project=expected_project,
                    validate_project=validate_project,
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
                outcome_callback(outcome)
            for future in as_completed(futures):
                future.result()

    @staticmethod
    def _result_record(
        outcome: _PublicationOutcome,
        *,
        position: int,
        batch_total: int | None,
    ) -> dict[str, Any]:
        succeeded = outcome.error is None
        return {
            "schema_version": 1,
            "timestamp": utc_now().isoformat(),
            "status": "succeeded" if succeeded else "failed",
            "action": outcome.action if succeeded else None,
            "handle": outcome.handle,
            "url": outcome.url,
            "project": outcome.project,
            "dataset_id": outcome.dataset_id,
            "file_name": outcome.file_name,
            "source_file": str(outcome.path.resolve()),
            "source_line": outcome.line_number,
            "position": position,
            "batch_index": outcome.index,
            "batch_total": batch_total,
            "retry_attempts": outcome.retry_attempts,
            "error": str(outcome.error) if outcome.error is not None else None,
        }

    @staticmethod
    def _new_result_file(project: str | None = None) -> Path:
        output_dir = Path(config.get("consumer", {}).get("output_dir", "outputs"))
        result_dir = output_dir / "published"
        result_dir.mkdir(parents=True, exist_ok=True)
        timestamp = utc_now().strftime("%Y-%m-%d_%H-%M-%S")
        project_slug = re.sub(r"[^a-z0-9-]+", "-", project or "").strip("-")
        stem = (
            f"published_{project_slug}_handles_{timestamp}"
            if project_slug
            else f"published_handles_{timestamp}"
        )
        counter = 1
        while True:
            suffix = "" if counter == 1 else f"_{counter}"
            candidate = result_dir / f"{stem}{suffix}.jsonl"
            try:
                # Reserve the name atomically so concurrent runs cannot select
                # the same human-readable filename.
                candidate.touch(exist_ok=False)
            except FileExistsError:
                counter += 1
                continue
            return candidate

    def _publish_chain(
        self,
        records: list[
            tuple[int, Path, int, dict[str, Any] | Exception, _PublicationContext]
        ],
        *,
        retries: int,
        retry_delay: float,
        on_outcome: OutcomeCallback,
        expected_project: str | None,
        validate_project: bool,
    ) -> None:
        for index, path, line_number, record, context in records:
            handle = record.get("handle") if isinstance(record, dict) else None
            retry_attempts = 0

            def count_retry() -> None:
                nonlocal retry_attempts
                retry_attempts += 1

            error: Exception | None = None
            write_result = None
            try:
                if isinstance(record, Exception):
                    raise record
                if validate_project and context.project != expected_project:
                    raise ValueError(
                        f"Handle project {context.project or 'missing'!r} does not match "
                        f"project {expected_project!r}"
                    )
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
                # Tracebacks retain this frame, its records, and its outcomes.
                # Receipts need the exception message, not that reference cycle.
                error = exc.with_traceback(None)
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
            on_outcome(outcome)

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
        records: list[InputRecord],
    ) -> dict[int, _PublicationContext]:
        contexts = {}
        for index, (_, _, record) in enumerate(records, start=1):
            record = record if isinstance(record, dict) else {}
            data = record.get("data")
            data = data if isinstance(data, dict) else {}
            dataset_id = data.get("DATASET_ID")
            dataset_id = (
                dataset_id if isinstance(dataset_id, str) and dataset_id else None
            )
            project = record.get("project")
            project = project if isinstance(project, str) and project else None
            if project is None and dataset_id:
                project = dataset_id.split(".", 1)[0]
            file_name = data.get("FILE_NAME")
            contexts[index] = _PublicationContext(
                project=normalize_project_id(project or "") or None,
                dataset_id=dataset_id,
                file_name=(
                    file_name if isinstance(file_name, str) and file_name else None
                ),
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
        handle = record.get("handle") if isinstance(record, dict) else None
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
