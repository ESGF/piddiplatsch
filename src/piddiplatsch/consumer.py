"""Kafka polling and lifecycle for catalog harvest and reusable input adapters."""

import json
import logging
import signal
import sys
import time
from contextlib import closing, nullcontext
from enum import StrEnum

from piddiplatsch.config import config
from piddiplatsch.core.messages import KafkaSource, SourceMessage
from piddiplatsch.core.pipeline import ProcessingPipeline, build_processing_target
from piddiplatsch.exceptions import MaxErrorsExceededError, StopOnTransientSkipError
from piddiplatsch.monitoring.progress import BaseProgress
from piddiplatsch.monitoring.stats import CounterKey, stats
from piddiplatsch.result import ProcessingResult

logger = logging.getLogger(__name__)


def prepare_kafka_runtime():
    """Load the native extension before rendering Kafka command progress.

    Keep this import lazy so configuration and file-only commands do not load
    an extension that can enable the GIL on free-threaded Python.
    """
    from confluent_kafka import Consumer

    return Consumer


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
        with_source: bool = False,
        commit_batch_size: int = 100,
        commit_interval: float = 1.0,
        on_idle=None,
    ):
        consumer_class = prepare_kafka_runtime()

        if idle_timeout is not None and idle_timeout <= 0:
            raise ValueError("idle timeout must be positive")
        if type(commit_batch_size) is not int or commit_batch_size < 1:
            raise ValueError("commit batch size must be a positive integer")
        if commit_interval <= 0:
            raise ValueError("commit interval must be positive")
        self.topic = topic
        self.idle_timeout = idle_timeout
        self.clock = clock
        self.with_source = with_source
        self.on_idle = on_idle
        self.commit_batch_size = commit_batch_size
        self.commit_interval = commit_interval
        self._pending_offsets: dict[tuple[str, int], int] = {}
        self._pending_messages = 0
        self._last_commit_at = self.clock() if with_source else None
        self._commit_failed = False
        if with_source:
            kafka_cfg = {
                **kafka_cfg,
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
            }
        self.consumer = consumer_class(kafka_cfg, logger=logger)
        if with_source:
            self.consumer.subscribe(
                [self.topic], on_revoke=self._on_revoke, on_lost=self._on_lost
            )
        else:
            self.consumer.subscribe([self.topic])

    def consume(self):
        last_message_at = self.clock()
        try:
            while True:
                poll_timeout = 1.0
                if self.with_source:
                    self._maybe_commit()
                    poll_timeout = min(poll_timeout, self.commit_interval)
                if self.idle_timeout is not None:
                    remaining = self.idle_timeout - (self.clock() - last_message_at)
                    if remaining <= 0:
                        return
                    poll_timeout = min(poll_timeout, remaining)

                msg = self.consumer.poll(timeout=poll_timeout)
                if msg is None:
                    if self.on_idle is not None:
                        self.on_idle()
                    continue
                last_message_at = self.clock()
                if msg.error():
                    from confluent_kafka import KafkaException

                    raise KafkaException(msg.error())

                key = msg.key().decode("utf-8") if msg.key() else None
                try:
                    value = json.loads(msg.value().decode("utf-8"))
                except (json.JSONDecodeError, UnicodeError) as e:
                    if self.with_source:
                        raise ValueError(
                            f"Invalid Kafka JSON at {msg.topic()}/{msg.partition()}/{msg.offset()}: {e}"
                        ) from e
                    logger.error(f"Failed to decode message: {e}")
                    continue

                if self.with_source:
                    yield SourceMessage(
                        key,
                        value,
                        KafkaSource(msg.topic(), msg.partition(), msg.offset()),
                        acknowledge=lambda msg=msg: self._acknowledge(msg),
                    )
                else:
                    yield key, value
        finally:
            try:
                if self.with_source and not self._commit_failed:
                    self.flush_acknowledgements()
            finally:
                self.consumer.close()

    def _acknowledge(self, message) -> None:
        """Queue only an offset whose catalog changes have already committed."""
        identity = (message.topic(), message.partition())
        next_offset = message.offset() + 1
        self._pending_offsets[identity] = max(
            next_offset, self._pending_offsets.get(identity, next_offset)
        )
        self._pending_messages += 1
        self._maybe_commit()

    def _maybe_commit(self) -> None:
        if self._pending_offsets and (
            self._pending_messages >= self.commit_batch_size
            or self.clock() - self._last_commit_at >= self.commit_interval
        ):
            self.flush_acknowledgements()

    def flush_acknowledgements(self, partitions=None) -> None:
        """Commit explicit durable offsets, never the consumer's polled position."""
        from confluent_kafka import KafkaException, TopicPartition

        identities = set(self._pending_offsets)
        if partitions is not None:
            identities &= {
                (partition.topic, partition.partition) for partition in partitions
            }
        if not identities:
            return
        offsets = [
            TopicPartition(topic, partition, self._pending_offsets[topic, partition])
            for topic, partition in sorted(identities)
        ]
        started = time.perf_counter()
        try:
            committed = self.consumer.commit(offsets=offsets, asynchronous=False)
            for partition in committed or ():
                if partition.error is not None:
                    raise KafkaException(partition.error)
        except BaseException:
            self._commit_failed = True
            raise
        logger.debug(
            "Kafka commit: %d partition offsets, %d durable messages, %.3fs",
            len(offsets),
            self._pending_messages,
            time.perf_counter() - started,
        )
        for identity in identities:
            del self._pending_offsets[identity]
        if not self._pending_offsets:
            self._pending_messages = 0
        self._last_commit_at = self.clock()

    def _on_revoke(self, consumer, partitions) -> None:
        self.flush_acknowledgements(partitions)

    def _on_lost(self, consumer, partitions) -> None:
        # Another owner may already be processing these partitions. Its replay
        # will consult durable catalog checkpoints; do not commit lost ownership.
        for partition in partitions:
            self._pending_offsets.pop((partition.topic, partition.partition), None)
        if not self._pending_offsets:
            self._pending_messages = 0


class HarvestProcessor:
    """Harvest raw dumps, optionally maintaining complete project catalogs."""

    def __init__(self, *, catalog_dir=None, backend="sqlite", clean_projects=()):
        self._harvester = None
        if catalog_dir is not None:
            from piddiplatsch.catalog.harvest import CatalogHarvester

            self._harvester = CatalogHarvester(
                catalog_dir, backend=backend, clean_projects=clean_projects
            )
        self.requires_kafka_source = self._harvester is not None
        self.stop_on_error = self.requires_kafka_source

    @property
    def kafka_group_suffix(self) -> str:
        return self._harvester.kafka_group_suffix if self._harvester else ".harvest"

    @property
    def project_names(self) -> tuple[str, ...]:
        return self._harvester.project_names if self._harvester else ()

    def plugin_name_for(self, value: dict) -> str | None:
        return self._harvester.plugin_name_for(value) if self._harvester else None

    def __str__(self) -> str:
        return "harvest"

    def preflight_check(self, stop_on_transient_skip: bool = True) -> None:
        return None

    def process(self, key: str, value: dict) -> ProcessingResult:
        if self._harvester:
            return self._harvester.process(key, value)
        return ProcessingResult(key=key, success=True)

    def process_with_source(self, key, value, source) -> ProcessingResult:
        if self._harvester:
            return self._harvester.process_with_source(key, value, source)
        return self.process(key, value)

    def close(self) -> None:
        if self._harvester:
            self._harvester.close()


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
    max_errors = config.get("consumer", {}).get("max_errors", 1)
    # Build processor instance to run preflight and pass into pipeline
    proc_instance = build_processing_target(
        processor=processor,
        projects=projects,
        publish=publish,
        handle_profile=handle_profile,
    )
    with_source = bool(getattr(proc_instance, "requires_kafka_source", False))
    if with_source and kafka_cfg is not None:
        kafka_cfg = {
            **kafka_cfg,
            "group.id": str(kafka_cfg.get("group.id", "piddiplatsch"))
            + getattr(proc_instance, "kafka_group_suffix", ".harvest"),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    stats_config = config.get("stats", {})
    selected_projects = getattr(proc_instance, "project_names", ())
    stats.configure_for_run(
        enable_db=monitor_db and stats_config.get("enable_db", False),
        db_path=stats_config.get("db_path"),
        log_interval_seconds=stats_config.get("interval_seconds"),
        log_interval_messages=stats_config.get("summary_interval"),
        command="harvest" if isinstance(proc_instance, HarvestProcessor) else "consume",
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
    consumer = None
    if direct_messages is not None:
        input_context = nullcontext(direct_messages)
    elif topic and kafka_cfg:
        catalog_cfg = config.get("catalog", {})
        options = (
            {
                "with_source": True,
                "commit_batch_size": catalog_cfg.get("kafka_commit_batch_size", 100),
                "commit_interval": catalog_cfg.get(
                    "kafka_commit_interval_seconds", 1.0
                ),
            }
            if with_source
            else {}
        )
        consumer = KafkaConsumer(
            topic,
            kafka_cfg,
            idle_timeout=idle_timeout,
            on_idle=progress.refresh if progress is not None else None,
            **options,
        )
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
            if with_source and consumer is not None:
                consumer.flush_acknowledgements()
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
        except Exception as exc:
            if stats.errors == 0:
                stats.error(message=str(exc))
            pipeline.close_progress()
            stats.close(status="failed", error_summary=str(exc))
            raise
        else:
            pipeline.close_progress()
            stats.close(status="completed")
