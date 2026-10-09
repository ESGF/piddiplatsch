"""Kafka polling and lifecycle for the consume and harvest commands."""

import json
import logging
import signal
import sys
import time
from contextlib import closing, nullcontext
from enum import StrEnum

from piddiplatsch.config import config
from piddiplatsch.core.pipeline import ProcessingPipeline, build_processing_target
from piddiplatsch.exceptions import MaxErrorsExceededError, StopOnTransientSkipError
from piddiplatsch.monitoring.progress import BaseProgress
from piddiplatsch.monitoring.stats import CounterKey, stats
from piddiplatsch.result import ProcessingResult

logger = logging.getLogger(__name__)


class StopCause(StrEnum):
    MANUAL = "manual"
    SIGINT = "sigint"
    KEYBOARD_INTERRUPT = "keyboard_interrupt"
    MAX_ERRORS = "max_errors_exceeded"
    TRANSIENT_EXTERNAL = "transient_external_failure"


class KafkaConsumer:
    """Kafka consumer wrapper."""

    def __init__(
        self,
        topic: str,
        kafka_cfg: dict,
        *,
        idle_timeout: float | None = None,
        clock=time.monotonic,
    ):
        # Configuration and file-only commands must not load Kafka's native
        # extension (which can enable the GIL on free-threaded Python).
        from confluent_kafka import Consumer as ConfluentConsumer

        if idle_timeout is not None and idle_timeout <= 0:
            raise ValueError("idle timeout must be positive")
        self.topic = topic
        self.idle_timeout = idle_timeout
        self.clock = clock
        self.consumer = ConfluentConsumer(kafka_cfg)
        self.consumer.subscribe([self.topic])

    def consume(self):
        last_message_at = self.clock()
        try:
            while True:
                poll_timeout = 1.0
                if self.idle_timeout is not None:
                    remaining = self.idle_timeout - (self.clock() - last_message_at)
                    if remaining <= 0:
                        return
                    poll_timeout = min(poll_timeout, remaining)

                msg = self.consumer.poll(timeout=poll_timeout)
                if msg is None:
                    continue
                last_message_at = self.clock()
                if msg.error():
                    from confluent_kafka import KafkaException

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


class HarvestProcessor:
    """Accept raw messages after they have been dumped."""

    def __str__(self) -> str:
        return "harvest"

    def preflight_check(self, stop_on_transient_skip: bool = True) -> None:
        return None

    def process(self, key: str, value: dict) -> ProcessingResult:
        return ProcessingResult(key=key, success=True)


def _stop_pipeline(pipeline: ProcessingPipeline, cause: StopCause = StopCause.MANUAL):
    logger.warning(f"Stopping consumer (cause: {cause.value})...")
    pipeline.stats._log_stats()
    pipeline.close_progress()
    logger.info(
        f"Total messages: {pipeline.stats.messages}, total errors: {pipeline.stats.errors}, "
        f"handles: {pipeline.stats[CounterKey.HANDLES]}, skipped: {pipeline.stats.skipped_messages}, "
        f"filtered: {pipeline.stats.filtered_messages}"
    )
    failed = cause in {StopCause.MAX_ERRORS, StopCause.TRANSIENT_EXTERNAL}
    pipeline.stats.close(
        status="failed" if failed else "stopped",
        error_summary=cause.value if failed else None,
    )


def start_consumer(
    topic=None,
    kafka_cfg=None,
    processor=None,
    *,
    projects: list[str] | tuple[str, ...] | str | None = None,
    dump_messages=False,
    verbose=False,
    direct_messages=None,
    publish: bool = False,
    force: bool = False,
    progress: BaseProgress | None = None,
    idle_timeout: float | None = None,
    limit: int | None = None,
    monitor_db: bool = False,
    handle_profile: str | None = None,
):
    max_errors = config.get("consumer", {}).get("max_errors", -1)
    # Build processor instance to run preflight and pass into pipeline
    proc_instance = build_processing_target(
        processor=processor,
        projects=projects,
        publish=publish,
        handle_profile=handle_profile,
    )
    stats_config = config.get("stats", {})
    selected_projects = getattr(proc_instance, "project_names", ())
    stats.configure_for_run(
        enable_db=monitor_db and stats_config.get("enable_db", False),
        db_path=stats_config.get("db_path"),
        log_interval_seconds=stats_config.get("interval_seconds"),
        log_interval_messages=stats_config.get("summary_interval"),
        command="consume",
        topic=topic,
        consumer_group=(kafka_cfg or {}).get("group.id"),
        selected_projects=selected_projects,
        heartbeat_interval_seconds=stats_config.get("heartbeat_interval_seconds", 5),
        sample_interval_seconds=stats_config.get("sample_interval_seconds", 15),
        sample_retention_days=stats_config.get("sample_retention_days", 30),
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
        stats.close(status="failed", error_summary=str(e))
        # Stop as transient external failure
        sys.exit(1)

    # Subscribe only after project selection, plugin construction, and preflight
    # have succeeded. Invalid selections must not create an orphaned consumer.
    if direct_messages is not None:
        input_context = nullcontext(direct_messages)
    elif topic and kafka_cfg:
        consumer = KafkaConsumer(topic, kafka_cfg, idle_timeout=idle_timeout)
        input_context = closing(consumer.consume())
    else:
        raise ValueError("Either Kafka config or direct_messages must be provided")

    with input_context as messages:
        pipeline = ProcessingPipeline(
            messages,
            proc_instance,
            dump_messages=dump_messages,
            verbose=verbose,
            progress=progress,
            max_errors=max_errors,
            publish=publish,
            force=force,
            limit=limit,
        )

        def sigint_handler(sig, frame):
            logger.warning("Received SIGINT. Gracefully shutting down.")
            _stop_pipeline(pipeline, cause=StopCause.SIGINT)
            sys.exit(0)

        signal.signal(signal.SIGINT, sigint_handler)

        try:
            pipeline.run()
        except MaxErrorsExceededError as e:
            logger.error(str(e))
            _stop_pipeline(pipeline, cause=StopCause.MAX_ERRORS)
            sys.exit(1)
        except StopOnTransientSkipError as e:
            logger.error(str(e))
            _stop_pipeline(pipeline, cause=StopCause.TRANSIENT_EXTERNAL)
            sys.exit(1)
        except KeyboardInterrupt:
            logger.warning("Consumer interrupted.")
            _stop_pipeline(pipeline, cause=StopCause.KEYBOARD_INTERRUPT)
            sys.exit(0)
        else:
            pipeline.close_progress()
            stats.close(status="completed")
