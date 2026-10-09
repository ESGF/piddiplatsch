"""Shared record processing, independent of Kafka and file input readers."""

import logging
from collections.abc import Iterable
from pathlib import Path

from piddiplatsch.config import config
from piddiplatsch.core.routing import ProjectRouter
from piddiplatsch.exceptions import MaxErrorsExceededError, StopOnTransientSkipError
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
        processor_kwargs={"handle_output_filename": handle_output_filename},
    )


class ProcessingPipeline:
    """Process a lazy iterable of keyed records with shared policies and counters."""

    def __init__(
        self,
        messages: Iterable[tuple[str | None, dict]],
        processor,
        *,
        dump_messages=False,
        verbose=False,
        progress: BaseProgress | None = None,
        max_errors=-1,
        publish: bool = False,
        force: bool = False,
        failure_dir: Path | None = None,
        limit: int | None = None,
    ):
        self.messages = messages
        self.processor = build_processing_target(
            processor=processor,
            publish=publish,
        )
        self.dump_messages = dump_messages
        self.max_errors = int(max_errors)
        if limit is not None and limit <= 0:
            raise ValueError("limit must be positive")
        self.limit = limit
        self.force = force
        self.failure_dir = failure_dir
        consumer_cfg = config.get("consumer", {})
        transient_cfg = consumer_cfg.get("transient", {})
        # Prefer new key `stop_on_skip`, fallback to legacy `stop_on_transient_skip`
        self.stop_on_transient_skip = bool(
            transient_cfg.get(
                "stop_on_skip",
                transient_cfg.get(
                    "stop_on_transient_skip",
                    consumer_cfg.get("stop_on_transient_skip", True),
                ),
            )
        )
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
        for key, value in self.messages:
            result = self._safe_process_message(key, value)

            self.stats.record_result(result)

            if result.skipped:
                self.stats.skip(message=f"message={key}")
                try:
                    SkipRecorder(project=result.plugin).record(
                        key, value, reason=result.skip_reason
                    )
                except Exception:
                    logger.exception(f"Failed to persist skipped message {key}")

                if result.transient_skip:
                    self.stats.external_fail(message=f"message={key}")
                    if self.stop_on_transient_skip and not self.force:
                        raise StopOnTransientSkipError(
                            f"Transient external failure encountered (key={key}); stopping as per policy"
                        )
            if result.patched:
                self.stats.patch(message=f"message={key}")

            if self.progress:
                self.progress.refresh()

            self._check_success()
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

    def _check_success(self):
        if self.max_errors >= 0 and self.stats.errors >= self.max_errors:
            raise MaxErrorsExceededError(
                f"Max error limit reached ({self.stats.errors}/{self.max_errors})"
            )

    def _safe_process_message(self, key, value):
        try:
            logger.debug(f"Processing message: {key}")
            if self.dump_messages:
                DumpRecorder().record(key, value)
            return self.processor.process(key, value)
        except Exception as e:
            logger.exception(f"Error processing message {key}")
            infos = value.get("__infos__", {}) or {}
            retries = infos.get("retries", value.get("retries", 0))
            reason = str(e)
            project = self._project_for_message(value)
            FailureRecorder(root_dir=self.failure_dir, project=project).record(
                key, value, retries=retries, reason=reason
            )
            return ProcessingResult(
                key=key,
                success=False,
                error=reason,
                project=project,
                plugin=project,
            )

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
    force: bool = False,
    verbose: bool = False,
    progress: BaseProgress | None = None,
    handle_profile: str | None = None,
    handle_output_filename: str | None = None,
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
        force=force,
        verbose=verbose,
        progress=progress,
    )

    try:
        return pipeline.run()
    finally:
        pipeline.close_progress()
