import logging
from collections.abc import Callable, Iterator
from contextlib import closing
from itertools import groupby, islice
from pathlib import Path

from piddiplatsch.config import config
from piddiplatsch.exceptions import JsonlReadError
from piddiplatsch.helpers import find_jsonl, utc_now
from piddiplatsch.jsonl_stream import iter_jsonl_records
from piddiplatsch.result import RetryResult

BATCH_SIZE = 256
_INPUT_ERROR = object()


def iter_failed_messages(
    jsonl_path: Path,
    *,
    yield_errors: bool = False,
) -> Iterator[tuple[str, dict] | JsonlReadError]:
    """Stream retry messages, incrementing persisted retry counts once."""
    with closing(
        iter_jsonl_records(
            [jsonl_path],
            yield_errors=yield_errors,
            snapshot=True,
        )
    ) as records:
        for path, line, record in records:
            if isinstance(record, JsonlReadError):
                yield record
                continue
            try:
                infos = record.get("__infos__")
                if infos is not None and not isinstance(infos, dict):
                    raise ValueError("__infos__ must be an object")
                if infos is None:
                    record.pop("__infos__", None)
                target = infos if infos is not None else record
                target["retries"] = int(target.get("retries", 0)) + 1
            except (TypeError, ValueError, OverflowError) as exc:
                error = JsonlReadError(
                    f"Invalid retry metadata in {path} at line {line}: {exc}"
                )
                if not yield_errors:
                    raise error from exc
                yield error
                continue
            key = str(record.get("key") or record.get("id") or "unknown")
            yield key, record


def find_retry_files(paths: tuple[Path, ...]) -> list[Path]:
    """
    Find all JSONL files from the given paths.

    Supports files, directories, and glob patterns. Returns sorted unique paths.
    """
    return find_jsonl(paths)


class RetryRunner:
    """Encapsulates retry policy and execution for processing failed items.

    Configure once per run to avoid repeating arguments across functions.

    Example:
        from pathlib import Path
        from piddiplatsch.persist.retry import RetryRunner

        runner = RetryRunner(
            projects=["cmip6"],
            failure_dir=Path("outputs/failures"),  # legacy/unresolved records
            delete_after=False,
            publish=False,
        )
        # Single file
        result = runner.run_file(
            Path("outputs/cmip6/failures/r0/failed_items.jsonl")
        )
        # Batch
        overall = runner.run_batch((Path("outputs/cmip6/failures/r0"),))
    """

    def __init__(
        self,
        *,
        projects: list[str] | tuple[str, ...] | str,
        failure_dir: Path,
        delete_after: bool = False,
        publish: bool = False,
        handle_profile: str | None = None,
        handle_output_filename: str | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.projects = projects
        self.failure_dir = failure_dir
        self.delete_after = delete_after
        self.publish = publish
        self.handle_profile = handle_profile
        self.handle_output_filename = handle_output_filename or (
            f"retry_handles_{utc_now():%Y-%m-%d_%H-%M-%S-%f}.jsonl"
        )
        self.output_dir = Path(config.get("consumer", {}).get("output_dir", "outputs"))
        self.logger = logger or logging.getLogger(__name__)

    def run_file(self, jsonl_path: Path) -> RetryResult:
        """Retry failed items from a JSONL file by reprocessing them through the pipeline."""
        from piddiplatsch.consumer import feed_messages_direct

        result = RetryResult()
        self.logger.info(
            "Retrying messages from %s using %s", jsonl_path, self.projects
        )
        try:
            source_stat = jsonl_path.stat()
        except OSError:
            source_stat = None  # The shared reader will report the read failure.
        failure_files_before = {
            path: path.stat().st_size for path in self._failure_files()
        }
        with closing(iter_failed_messages(jsonl_path, yield_errors=True)) as messages:
            while batch := list(islice(messages, BATCH_SIZE)):
                # Group only adjacent selections so interleaved projects retain
                # source order. Each list and processing call is bounded.
                for project, group in groupby(batch, key=self._message_project):
                    entries = list(group)
                    if project is _INPUT_ERROR:
                        for error in entries:
                            result.total += 1
                            result.failed += 1
                            self.logger.error("%s", error)
                            if len(result.errors) < 100:
                                result.errors.append(str(error))
                        continue
                    partial = feed_messages_direct(
                        entries,
                        projects=[project] if project else self.projects,
                        publish=self.publish,
                        handle_profile=self.handle_profile,
                        handle_output_filename=self.handle_output_filename,
                        force=True,
                    )
                    result.total += partial.total
                    result.succeeded += partial.succeeded
                    result.skipped += partial.skipped
                    result.filtered += partial.filtered
                    result.failed += partial.failed + partial.skipped + partial.filtered

        # Find new failure files created during retry
        failure_files_after = self._failure_files()
        result.failure_files = {
            path
            for path in failure_files_after
            if path not in failure_files_before
            or path.stat().st_size != failure_files_before[path]
        }
        result.handle_files = set(
            self.output_dir.glob(f"*/handles/{self.handle_output_filename}")
        )

        if result.total == 0:
            self.logger.warning("No messages to retry.")
            return result

        if self.delete_after and result.failed == 0:
            try:
                current_stat = jsonl_path.stat()
                unchanged = source_stat is not None and all(
                    getattr(current_stat, name) == getattr(source_stat, name)
                    for name in ("st_dev", "st_ino", "st_size", "st_mtime_ns")
                )
                if not unchanged:
                    self.logger.warning("Keeping changed retry input: %s", jsonl_path)
                else:
                    jsonl_path.unlink()
                    self.logger.info(f"Deleted retry file: {jsonl_path}")
            except Exception as e:
                self.logger.warning(f"Could not delete {jsonl_path}: {e}")
        elif self.delete_after and result.failed > 0:
            self.logger.info(
                f"Skipping deletion of {jsonl_path} because {result.failed} items failed again"
            )

        return result

    def _failure_files(self) -> set[Path]:
        files = set(self.failure_dir.rglob("*.jsonl"))
        files.update(self.output_dir.rglob("failed_items_*.jsonl"))
        return files

    @staticmethod
    def _message_project(message: tuple[str, dict] | JsonlReadError):
        if isinstance(message, JsonlReadError):
            return _INPUT_ERROR
        infos = message[1].get("__infos__", {}) or {}
        project = infos.get("project")
        return project if isinstance(project, str) and project.strip() else None

    def run_batch(
        self,
        paths: tuple[Path, ...],
        *,
        verbose: bool = False,
        progress_callback: Callable[[Path, int, int, RetryResult], None] | None = None,
    ) -> RetryResult:
        """Retry failed items from multiple files/directories and aggregate results."""
        files = find_retry_files(paths)

        if not files:
            self.logger.warning("No retry files found.")
            return RetryResult()

        self.logger.info(f"Found {len(files)} file(s) to retry.")

        overall = RetryResult()
        total_files = len(files)

        for idx, file in enumerate(files, 1):
            result = self.run_file(file)
            overall.total += result.total
            overall.succeeded += result.succeeded
            overall.failed += result.failed
            overall.skipped += result.skipped
            overall.filtered += result.filtered
            overall.failure_files.update(result.failure_files)
            overall.handle_files.update(result.handle_files)
            overall.errors.extend(result.errors[: max(0, 100 - len(overall.errors))])

            if progress_callback:
                progress_callback(file, idx, total_files, result)

            if verbose:
                self.logger.info(
                    f"[{idx}/{total_files}] {file.name}: total={result.total}, succeeded={result.succeeded}, failed={result.failed}"
                )

        return overall
