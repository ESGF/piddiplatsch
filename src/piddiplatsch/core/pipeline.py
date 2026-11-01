"""Shared record processing, independent of Kafka and file input readers."""

import logging
from collections.abc import Iterable
from pathlib import Path

from piddiplatsch.config import config
from piddiplatsch.core.messages import SourceMessage
from piddiplatsch.core.routing import ProjectRouter
from piddiplatsch.exceptions import MaxErrorsExceededError
from piddiplatsch.monitoring.disk import get_disk_guard
from piddiplatsch.monitoring.progress import BaseProgress, get_progress
from piddiplatsch.monitoring.stats import stats
from piddiplatsch.persist.dump import DumpRecorder
from piddiplatsch.persist.recovery import FailureRecorder
from piddiplatsch.persist.skipped import SkipRecorder
from piddiplatsch.result import FeedResult, ProcessingResult

logger = logging.getLogger(__name__)


def configured_projects() -> list[str] | str:
    """Return the configured project plugin selection."""
    consumer_cfg = config.get("consumer", {})
    projects = consumer_cfg.get("projects")
    if projects is None:
        raise ValueError("No projects configured; set [consumer].projects")
    return projects


def build_processing_target(
    *,
    processor=None,
    projects: list[str] | tuple[str, ...] | str | None = None,
    publish: bool = False,
    handle_profile: str | None = None,
    handle_output_filename: str | None = None,
    catalogs=None,
    version_history: bool = False,
    require_all_assets: bool = False,
):
    """Build a project router or use an explicitly supplied processing object."""
    if processor is not None and projects is not None:
        raise ValueError("Specify either processor or projects, not both")
    if processor is not None:
        if isinstance(processor, str):
            raise TypeError(
                "String processor selection is not supported; use projects or a processing object"
            )
        return processor
    selection = configured_projects() if projects is None else projects
    return ProjectRouter(
        selection,
        publish=publish,
        handle_profile=handle_profile,
        processor_kwargs={
            "handle_output_filename": handle_output_filename,
            **({"require_all_assets": True} if require_all_assets else {}),
        },
        catalogs=catalogs,
        version_history=version_history,
    )


class ProcessingPipeline:
    """Process a lazy iterable of keyed records with shared policies and counters."""

    def __init__(
        self,
        messages: Iterable[tuple[str | None, dict] | SourceMessage],
        processor,
        *,
        dump_messages=False,
        verbose=False,
        progress: BaseProgress | None = None,
        max_errors: int | None = None,
        project_max_errors: dict[str, int] | None = None,
        publish: bool = False,
        failure_dir: Path | None = None,
        limit: int | None = None,
    ):
        self.messages = messages
        self.processor = build_processing_target(
            processor=processor,
            publish=publish,
        )
        self.dump_messages = dump_messages
        self.dump_recorder = DumpRecorder() if dump_messages else None
        self.disk_guard = get_disk_guard()
        self.max_errors = 1 if max_errors is None else int(max_errors)
        if limit is not None and limit <= 0:
            raise ValueError("limit must be positive")
        self.limit = limit
        self.project_max_errors = project_max_errors
        self.failure_dir = failure_dir
        self.stats = stats
        self._owns_progress = progress is None
        self.progress = progress or get_progress(f"{self.processor}", use_tqdm=verbose)

    def run(self) -> FeedResult:
        """Process in source order; the caller owns the input iterable's lifetime."""
        logger.info("Starting processing pipeline...")
        messages_before = self.stats.messages
        errors_before = self.stats.errors
        skipped_before = self.stats.skipped_messages
        filtered_before = self.stats.filtered_messages
        processed = 0
        self._project_errors = {}
        for message in self.messages:
            self.disk_guard.check()
            source_message = message if isinstance(message, SourceMessage) else None
            key, value = (message.key, message.value) if source_message else message
            result = self._safe_process_message(
                key, value, source_message=source_message
            )

            if source_message and source_message.acknowledge and result.success:
                source_message.acknowledge()

            self.stats.record_result(result)
            project = result.plugin or result.project or "unassigned"
            if not result.success and not result.skipped and not result.filtered:
                self._project_errors[project] = self._project_errors.get(project, 0) + 1

            if result.skipped:
                self.stats.skip(message=f"message={key}")
                try:
                    SkipRecorder(project=result.plugin).record(
                        key, value, reason=result.skip_reason
                    )
                except Exception:
                    logger.exception(f"Failed to persist skipped message {key}")

            if result.patched:
                self.stats.patch(message=f"message={key}")

            if self.progress:
                self.progress.refresh()

            self._check_success(project)
            processed += 1
            if self.limit is not None and processed >= self.limit:
                logger.info("Processing limit reached (%d messages)", self.limit)
                break

        skipped = self.stats.skipped_messages - skipped_before
        filtered = self.stats.filtered_messages - filtered_before
        return FeedResult(
            total=processed,
            succeeded=self.stats.messages - messages_before - skipped - filtered,
            failed=self.stats.errors - errors_before,
            skipped=skipped,
            filtered=filtered,
        )

    def _check_success(self, project):
        limit = self.max_errors
        errors = self.stats.errors
        suffix = ""
        if self.project_max_errors is not None:
            limit = self.project_max_errors.get(project, self.max_errors)
            errors = self._project_errors.get(project, 0)
            suffix = f" for project {project}"
        # Zero errors tolerated means stop on the first actual error, never
        # after a successful record. Keep 1 as the conventional default.
        if errors > 0 and limit >= 0 and errors >= max(1, limit):
            raise MaxErrorsExceededError(
                f"Max error limit reached ({errors}/{limit}){suffix}"
            )

    def _safe_process_message(self, key, value, *, source_message=None):
        try:
            logger.debug(f"Processing message: {key}")
            if self.dump_recorder is not None:
                self.dump_recorder.record(key, value)
            if source_message is not None:
                process = getattr(self.processor, "process_with_source", None)
                if callable(process):
                    return process(key, value, source_message.source)
            return self.processor.process(key, value)
        except Exception as e:
            logger.exception(f"Error processing message {key}")
            failure_value = (
                value if isinstance(value, dict) else {"invalid_message": value}
            )
            infos = failure_value.get("__infos__", {}) or {}
            if not isinstance(infos, dict):
                infos = {}
            retries = infos.get("retries", failure_value.get("retries", 0))
            source_options = {"source": source_message.source} if source_message else {}
            reason = str(e)
            project = self._project_for_message(value)
            FailureRecorder(
                root_dir=self.failure_dir, project=project, **source_options
            ).record(key, failure_value, retries=retries, reason=reason)
            result = ProcessingResult(
                key=key,
                success=False,
                error=reason,
                project=project,
                plugin=project,
            )
            if getattr(self.processor, "stop_on_error", False):
                self.stats.record_result(result)
                raise
            return result

    def _project_for_message(self, value: dict) -> str | None:
        resolver = getattr(self.processor, "plugin_name_for", None)
        if not callable(resolver):
            return None
        try:
            return resolver(value)
        except Exception:
            logger.debug("Could not resolve project for failed message", exc_info=True)
            return None

    def close_progress(self) -> None:
        """Close progress created by this pipeline, leaving injected progress to its owner."""
        if self._owns_progress:
            self.progress.close()


def process_messages(
    messages: Iterable[tuple[str | None, dict]],
    processor=None,
    projects: list[str] | tuple[str, ...] | str | None = None,
    publish=False,
    failure_dir: Path | None = None,
    verbose: bool = False,
    progress: BaseProgress | None = None,
    handle_profile: str | None = None,
    handle_output_filename: str | None = None,
    max_errors: int | None = None,
    project_max_errors: dict[str, int] | None = None,
) -> FeedResult:
    target = build_processing_target(
        processor=processor,
        projects=projects,
        publish=publish,
        handle_profile=handle_profile,
        handle_output_filename=handle_output_filename,
    )
    pipeline = ProcessingPipeline(
        messages,
        processor=target,
        publish=publish,
        failure_dir=failure_dir,
        verbose=verbose,
        progress=progress,
        max_errors=max_errors,
        project_max_errors=project_max_errors,
    )

    try:
        return pipeline.run()
    finally:
        pipeline.close_progress()
