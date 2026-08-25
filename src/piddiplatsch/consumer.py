import json
import logging
import signal
import sys
from enum import StrEnum
from pathlib import Path

from confluent_kafka import Consumer as ConfluentConsumer
from confluent_kafka import KafkaException

from piddiplatsch.config import config
from piddiplatsch.core.routing import ProjectRouter
from piddiplatsch.exceptions import MaxErrorsExceededError, StopOnTransientSkipError
from piddiplatsch.monitoring.stats import CounterKey, stats
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
    dry_run: bool = False,
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
    return ProjectRouter(selection, dry_run=dry_run)


class StopCause(StrEnum):
    MANUAL = "manual"
    SIGINT = "sigint"
    KEYBOARD_INTERRUPT = "keyboard_interrupt"
    MAX_ERRORS = "max_errors_exceeded"
    TRANSIENT_EXTERNAL = "transient_external_failure"


# ----------------------------
# Base Consumer
# ----------------------------


class BaseConsumer:
    """Abstract base consumer interface."""

    def consume(self):
        """
        Yield tuples of (key, value) messages.
        Must be implemented by subclasses.
        """
        raise NotImplementedError


# ----------------------------
# Kafka Consumer
# ----------------------------


class KafkaConsumer(BaseConsumer):
    """Kafka consumer wrapper."""

    def __init__(self, topic: str, kafka_cfg: dict):
        self.topic = topic
        self.consumer = ConfluentConsumer(kafka_cfg)
        self.consumer.subscribe([self.topic])

    def consume(self):
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                key = msg.key().decode("utf-8") if msg.key() else None
                try:
                    value = json.loads(msg.value().decode("utf-8"))
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
                    continue

                yield key, value
        finally:
            self.consumer.close()


# ----------------------------
# Direct Consumer (for tests / recovery)
# ----------------------------


class DirectConsumer(BaseConsumer):
    """Feed messages directly without Kafka."""

    def __init__(self, messages):
        """
        messages: iterable of (key, value) tuples
        """
        self.messages = list(messages)

    def consume(self):
        yield from self.messages


# ----------------------------
# Consumer Pipeline
# ----------------------------


class ConsumerPipeline:
    """Coordinates consumption, message processing, and stats."""

    def __init__(
        self,
        consumer: BaseConsumer,
        processor,
        *,
        dump_messages=False,
        verbose=False,
        max_errors=-1,
        dry_run: bool = False,
        force: bool = False,
        failure_dir: Path | None = None,
    ):
        """
        consumer: instance of BaseConsumer (KafkaConsumer or DirectConsumer)
        processor: processor name
        """
        self.consumer = consumer
        # String names remain supported for direct/test callers. Runtime
        # ingestion passes a ProjectRouter or another processing object.
        self.processor = build_processing_target(
            processor=processor,
            dry_run=dry_run,
        )
        self.dump_messages = dump_messages
        self.max_errors = int(max_errors)
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
        self.progress = None
        try:
            from piddiplatsch.monitoring import get_progress

            self.progress = get_progress(f"{self.processor}", use_tqdm=verbose)
        except ImportError:
            pass

    def run(self):
        logger.info("Starting consumer pipeline...")
        for key, value in self.consumer.consume():
            result = self._safe_process_message(key, value)

            # Track metrics
            if result.filtered:
                self.stats.tick()
                self.stats.filtered(message=f"message={key} project={result.project}")
            elif result.skipped:
                # A skipped message was consumed, but it did not complete successfully.
                self.stats.tick()
            elif result.success:
                self.stats.tick()
                self.stats.handle(
                    n=result.num_handles,
                    handle_time_sec=result.handle_processing_time,
                )
            else:
                self.stats.error(message=result.error)

            if result.skipped:
                self.stats.skip(message=f"message={key}")
                try:
                    SkipRecorder().record(key, value, reason=result.skip_reason)
                except Exception:
                    logger.exception(f"Failed to persist skipped message {key}")

                if result.transient_skip:
                    self.stats.external_fail(message=f"message={key}")
                    if self.stop_on_transient_skip and not self.force:
                        raise StopOnTransientSkipError(f"Transient external failure encountered (key={key}); stopping as per policy")
            if result.patched:
                self.stats.patch(message=f"message={key}")

            if self.progress:
                self.progress.refresh()

            self._check_success()

    def _check_success(self):
        if self.max_errors >= 0 and self.stats.errors >= self.max_errors:
            raise MaxErrorsExceededError(f"Max error limit reached ({self.stats.errors}/{self.max_errors})")

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
            FailureRecorder(root_dir=self.failure_dir).record(key, value, retries=retries, reason=reason)
            return ProcessingResult(key=key, success=False, error=reason)

    def stop(self, cause: StopCause = StopCause.MANUAL):
        logger.warning(f"Stopping consumer (cause: {cause.value})...")
        self.stats._log_stats()
        if self.progress:
            self.progress.close()
        logger.info(
            f"Total messages: {self.stats.messages}, total errors: {self.stats.errors}, "
            f"handles: {self.stats[CounterKey.HANDLES]}, skipped: {self.stats.skipped_messages}, "
            f"filtered: {self.stats.filtered_messages}"
        )
        self.stats.close()


# ----------------------------
# Direct helpers for testing / recovery
# ----------------------------


def feed_messages_direct(
    messages,
    processor=None,
    projects: list[str] | tuple[str, ...] | str | None = None,
    dry_run=False,
    failure_dir: Path | None = None,
    force: bool = False,
) -> FeedResult:
    consumer = DirectConsumer(messages)
    target = build_processing_target(
        processor=processor,
        projects=projects,
        dry_run=dry_run,
    )
    pipeline = ConsumerPipeline(
        consumer,
        processor=target,
        dry_run=dry_run,
        failure_dir=failure_dir,
        force=force,
    )

    # Track stats before run
    messages_before = pipeline.stats.messages
    errors_before = pipeline.stats.errors
    skipped_before = pipeline.stats.skipped_messages
    filtered_before = pipeline.stats.filtered_messages

    pipeline.run()

    # Calculate delta from pipeline stats
    processed = pipeline.stats.messages - messages_before
    failed = pipeline.stats.errors - errors_before
    skipped = pipeline.stats.skipped_messages - skipped_before
    filtered = pipeline.stats.filtered_messages - filtered_before
    succeeded = processed - skipped - filtered

    return FeedResult(
        total=len(messages),
        succeeded=succeeded,
        failed=failed,
        skipped=skipped,
        filtered=filtered,
    )


def feed_test_files(
    testfile_paths,
    projects: list[str] | tuple[str, ...] | str = ("cmip6",),
):
    messages = []
    for path in testfile_paths:
        if isinstance(path, str):
            path = Path(path)
        with path.open("r", encoding="utf-8") as f:
            messages.append((path.name, json.load(f)))
    feed_messages_direct(messages, projects=projects)


# ----------------------------
# CLI / Dispatcher entrypoint
# ----------------------------


def start_consumer(
    topic=None,
    kafka_cfg=None,
    processor=None,
    *,
    projects: list[str] | tuple[str, ...] | str | None = None,
    dump_messages=False,
    verbose=False,
    enable_db: bool | None = None,
    db_path: str | None = None,
    direct_messages=None,
    dry_run: bool = False,
    force: bool = False,
):
    # Initialize stats from the fully loaded configuration for this run.
    stats_config = config.get("stats", {})
    stats.configure_for_run(
        enable_db=(stats_config.get("enable_db", False) if enable_db is None else enable_db),
        db_path=db_path or stats_config.get("db_path"),
        log_interval_seconds=stats_config.get("interval_seconds"),
        log_interval_messages=stats_config.get("summary_interval"),
    )

    max_errors = config.get("consumer", {}).get("max_errors", -1)
    # Build processor instance to run preflight and pass into pipeline
    proc_instance = build_processing_target(
        processor=processor,
        projects=projects,
        dry_run=dry_run,
    )
    # Optional STAC preflight
    try:
        if not force:
            consumer_cfg = config.get("consumer", {})
            transient_cfg = consumer_cfg.get("transient", {})
            stop_on_transient_skip = bool(
                transient_cfg.get(
                    "stop_on_skip",
                    transient_cfg.get(
                        "stop_on_transient_skip",
                        consumer_cfg.get("stop_on_transient_skip", True),
                    ),
                )
            )
            proc_instance.preflight_check(stop_on_transient_skip=stop_on_transient_skip)
    except Exception as e:
        logger.error(str(e))
        # Stop as transient external failure
        sys.exit(1)

    # Subscribe only after project selection, plugin construction, and preflight
    # have succeeded. Invalid selections must not create an orphaned consumer.
    if direct_messages is not None:
        consumer = DirectConsumer(direct_messages)
    elif topic and kafka_cfg:
        consumer = KafkaConsumer(topic, kafka_cfg)
    else:
        raise ValueError("Either Kafka config or direct_messages must be provided")

    pipeline = ConsumerPipeline(
        consumer,
        proc_instance,
        dump_messages=dump_messages,
        verbose=verbose,
        max_errors=max_errors,
        dry_run=dry_run,
        force=force,
    )

    def sigint_handler(sig, frame):
        logger.warning("Received SIGINT. Gracefully shutting down.")
        pipeline.stop(cause=StopCause.SIGINT)
        sys.exit(0)

    signal.signal(signal.SIGINT, sigint_handler)

    try:
        pipeline.run()
    except MaxErrorsExceededError as e:
        logger.error(str(e))
        pipeline.stop(cause=StopCause.MAX_ERRORS)
        sys.exit(1)
    except StopOnTransientSkipError as e:
        logger.error(str(e))
        pipeline.stop(cause=StopCause.TRANSIENT_EXTERNAL)
        sys.exit(1)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted.")
        pipeline.stop(cause=StopCause.KEYBOARD_INTERRUPT)
        sys.exit(0)
