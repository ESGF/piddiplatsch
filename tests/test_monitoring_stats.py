import datetime
import logging
import sqlite3
from types import SimpleNamespace

import pytest

from piddiplatsch.config import config
from piddiplatsch.consumer import HarvestProcessor, start_consumer
from piddiplatsch.monitoring.stats import (
    ConsoleReporter,
    CounterKey,
    Stats,
    StatusReporter,
    concise_error,
)
from piddiplatsch.monitoring.status import read_status
from piddiplatsch.result import ProcessingResult


@pytest.mark.parametrize("command", ["harvest", "map", "sync"])
def test_console_summary_labels_operation_and_keeps_raw_details_at_debug(
    command, caplog
):
    summary = {
        "messages": 30000,
        "catalog_writes": 30000,
        "handles": 11573,
        "errors": 0,
        "warnings": 2,
        "messages_per_sec": 293.9,
        "uptime": 106.25,
        "projects": {"cmip6": {"consumed": 30000}, "cmip7": {"consumed": 0}},
        "run": {"command": command, "status": "running", "run_id": "details-only"},
    }
    operation = "sync/map" if command == "sync" else command
    with caplog.at_level(logging.INFO, logger="piddiplatsch.monitoring.stats"):
        ConsoleReporter().log(summary)

    assert len(caplog.messages) == 1
    line = caplog.messages[0]
    assert line.startswith(f"[{operation}] running | 30,000 ")
    assert "0 errors | 2 warnings" in line
    assert "elapsed 106s | projects: cmip6" in line
    if command == "harvest":
        assert "30,000 catalog writes" in line
        assert "294 messages/s" in line
        assert "Handles" not in line
    else:
        assert "11,573 Handles prepared" in line
        assert "294 items/s" in line
        assert "catalog writes" not in line
    assert "details-only" not in caplog.text

    with caplog.at_level(logging.DEBUG, logger="piddiplatsch.monitoring.stats"):
        ConsoleReporter().log(summary)
    assert f"[{operation}] Stats snapshot:" in caplog.text
    assert "details-only" in caplog.text


@pytest.mark.parametrize("enable_db", [False, True])
@pytest.mark.parametrize("aliases", [0, 1])
def test_map_final_summary_reports_aliases_without_per_item_info(
    enable_db, aliases, tmp_path, caplog
):
    stats = Stats(enable_db=False)
    stats.configure_for_run(
        enable_db=enable_db,
        db_path=str(tmp_path / "monitor.db"),
        command="map",
        log_interval_messages=10000,
        log_interval_seconds=3600,
    )
    with caplog.at_level(logging.INFO, logger="piddiplatsch.monitoring.stats"):
        stats.record_result(
            ProcessingResult(
                key="dataset-details",
                success=True,
                plugin="cmip7",
                num_handles=2 + aliases,
                num_alias_handles=aliases,
            )
        )
        assert not caplog.messages
        stats.close(status="completed")
        stats.close(status="completed")

    assert len(caplog.messages) == 1
    assert f"[map] completed | 1 items | {2 + aliases} Handles prepared" in caplog.text
    if aliases:
        assert "(1 alias)" in caplog.text
    else:
        assert "aliases" not in caplog.text
    assert "dataset-details" not in caplog.text
    assert stats.summary()["alias_handles"] == aliases


def test_periodic_map_summary_includes_current_handles_and_aliases(caplog):
    stats = Stats(enable_db=False)
    stats.configure_for_run(command="map", log_interval_messages=1)
    with caplog.at_level(logging.INFO, logger="piddiplatsch.monitoring.stats"):
        stats.record_result(
            ProcessingResult(
                key="item", success=True, num_handles=3, num_alias_handles=1
            )
        )
    assert "3 Handles prepared (1 alias)" in caplog.text


class _MappingProcessor:
    def preflight_check(self, **_kwargs):
        return None

    def process(self, key, _value):
        return ProcessingResult(key=key, success=True)


def test_counters_increment():
    s = Stats(enable_db=False)
    s.reset()  # fresh state for test

    # initial counts
    assert s.messages == 0
    assert s.errors == 0
    assert s.handles == 0

    # increment messages
    s.tick(3)
    assert s.messages == 3

    # increment errors
    s.error(n=2)
    assert s.errors == 2

    # increment handles
    s.handle(n=5, handle_time_sec=0.5)
    assert s.handles == 5
    assert s.handle_time_total == 0.5

    # other counters
    s.retry(n=1)
    assert s.retries == 1
    s.skip(n=2)
    assert s.skipped_messages == 2
    s.warn(n=1)
    assert s.warnings == 1
    s.retracted(n=1)
    assert s.retracted_messages == 1
    s.replica(n=2)
    assert s.replicas == 2


def test_summary_returns_expected_keys():
    s = Stats(enable_db=False)
    s.reset()
    s.tick(1)
    s.error(n=1)

    summary = s.summary()
    # all CounterKey values present
    for key in CounterKey:
        assert key.value in summary

    # computed keys
    for key in [
        "uptime",
        "message_rate",
        "handle_rate",
        "messages_per_sec",
        "last_message_time",
        "last_error_time",
        "start_time",
    ]:
        assert key in summary


def test_timestamps_are_utc():
    s = Stats(enable_db=False)
    s.reset()
    s.tick(1)
    s.error(n=1)

    # all timestamps must be UTC-aware
    assert s.start_time.tzinfo is datetime.UTC
    assert s.last_message_time.tzinfo is datetime.UTC
    assert s.last_error_time.tzinfo is datetime.UTC

    # uptime should be positive float
    assert isinstance(s.uptime, float)
    assert s.uptime >= 0


def test_project_status_is_persisted_with_run_heartbeat(tmp_path):
    db_path = tmp_path / "piddi.db"
    config._set("catalog", "directory", str(tmp_path))
    s = Stats(enable_db=False)
    s.configure_for_run(
        enable_db=True,
        db_path=str(db_path),
        command="consume",
        topic="publication",
        consumer_group="piddi-cmip",
        selected_projects=("cmip6", "cmip7"),
        heartbeat_interval_seconds=60,
    )
    s.record_result(
        SimpleNamespace(
            key="one",
            plugin="cmip6",
            project="cmip6",
            filtered=False,
            skipped=False,
            success=True,
            num_handles=3,
            handle_processing_time=0.2,
            error=None,
        )
    )
    s.record_result(
        SimpleNamespace(
            key="two",
            plugin=None,
            project="cmip7",
            filtered=True,
            skipped=False,
            success=True,
            num_handles=0,
            handle_processing_time=0,
            error=None,
        )
    )
    s.close(status="completed")

    status = read_status(db_path)
    assert status["schema_version"] == 1
    assert status["runs"][0]["state"] == "completed"
    assert status["runs"][0]["selected_projects"] == ["cmip6", "cmip7"]
    projects = {row["project"]: row for row in status["runs"][0]["projects"]}
    assert projects["cmip6"]["succeeded"] == 1
    assert projects["cmip6"]["handles"] == 3
    assert projects["cmip7"]["filtered"] == 1
    history = {row["project"]: row for row in status["runs"][0]["history"]}
    assert history["cmip6"]["sample_count"] == 2
    assert history["cmip6"]["deltas"]["consumed"] == 1
    assert history["cmip6"]["deltas"]["handles"] == 3
    assert history["cmip7"]["deltas"]["filtered"] == 1


def test_read_status_can_filter_project(tmp_path):
    db_path = tmp_path / "piddi.db"
    config._set("catalog", "directory", str(tmp_path))
    reporter = StatusReporter(str(db_path))
    reporter.close()

    status = read_status(db_path, project="cmip7")
    assert status["runs"] == []


def test_fresh_database_contains_only_current_monitoring_tables(tmp_path):
    db_path = tmp_path / "piddi.db"
    config._set("catalog", "directory", str(tmp_path))
    reporter = StatusReporter(str(db_path))
    reporter.close()

    with sqlite3.connect(db_path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }

    assert tables - {"sqlite_sequence"} == {
        "catalog_items",
        "work_items",
        "kafka_checkpoints",
        "stats",
    }


def test_monitoring_error_summary_redacts_credentials_and_payloads():
    assert (
        concise_error("request failed password=hunter2")
        == "request failed password=***"
    )
    assert (
        concise_error("https://alice:secret@example.test failed")
        == "https://***@example.test failed"
    )
    assert (
        concise_error('{"raw": "message"}')
        == "processing error (structured details omitted)"
    )


@pytest.mark.parametrize("processor", [HarvestProcessor(), _MappingProcessor()])
def test_consumer_entrypoint_without_monitoring_opt_in_does_not_create_database(
    tmp_path, processor
):
    db_path = tmp_path / "piddi.db"
    config._set("catalog", "directory", str(tmp_path))
    config._set("stats", "enable_db", True)

    start_consumer(processor=processor, direct_messages=[("one", {"value": 1})])

    assert not db_path.exists()


def test_consumer_entrypoint_creates_configured_monitoring_database(tmp_path):
    db_path = tmp_path / "piddi.db"
    config._set("catalog", "directory", str(tmp_path))
    config._set("stats", "enable_db", True)

    start_consumer(
        "publications",
        {"group.id": "piddi-test"},
        projects=["cmip6"],
        direct_messages=[("one", {"value": 1})],
        monitor_db=True,
    )

    status = read_status(db_path)
    run = status["runs"][0]
    assert run["command"] == "consume"
    assert run["topic"] == "publications"
    assert run["consumer_group"] == "piddi-test"
    assert run["selected_projects"] == ["cmip6"]
    assert run["state"] == "completed"


def test_consumer_monitoring_respects_disabled_database_config(tmp_path):
    db_path = tmp_path / "piddi.db"
    config._set("catalog", "directory", str(tmp_path))
    config._set("stats", "enable_db", False)

    start_consumer(
        processor=_MappingProcessor(),
        direct_messages=[("one", {"value": 1})],
        monitor_db=True,
    )

    assert not db_path.exists()


def test_concurrent_heartbeats_preserve_runs_and_durable_totals(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor

    from piddiplatsch.catalog.sqlite import SQLiteCatalog
    from piddiplatsch.maintenance import clean_status_history

    database = tmp_path / "piddi.db"
    config._set("catalog", "directory", str(tmp_path))
    now = datetime.datetime.now(datetime.UTC)
    started = (now - datetime.timedelta(days=40)).isoformat()
    reporters = [StatusReporter(database), StatusReporter(database)]
    counters = dict.fromkeys(
        (
            "consumed",
            "routed",
            "filtered",
            "succeeded",
            "skipped",
            "failed",
            "handles",
            "catalog_writes",
            "catalog_refreshes",
        ),
        0,
    )
    summaries = [
        dict(
            run=dict(
                run_id=str(index),
                command=command,
                status="completed",
                started_at=started,
                finished_at=started,
                selected_projects=["cmip7"],
            ),
            projects={"cmip7": dict(counters, consumed=index)},
        )
        for index, command in enumerate(("harvest", "publish"))
    ]
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(reporter.heartbeat, summary, force_sample=True)
            for reporter, summary in zip(reporters, summaries, strict=True)
        ]
        for future in futures:
            future.result()
    for reporter in reporters:
        reporter.close()
    with SQLiteCatalog("cmip7", database) as catalog:
        from piddiplatsch.catalog import CatalogEntry
        from piddiplatsch.core.messages import KafkaSource

        with catalog.transaction() as transaction:
            transaction.add_work(
                CatalogEntry("CMIP7", {"id": "one"}, "dataset", 1),
                KafkaSource("topic", 0, 0),
            )
        catalog.seal_batch(
            "batch", [{"id": 1, "error": None, "handle_count": 3, "alias_count": 1}]
        )
        catalog.publish_batch("batch", 3)
    # Reading a complete snapshot needs no status, batch, or disk inspection.
    monkeypatch.setattr(
        "piddiplatsch.monitoring.disk.disk_status",
        lambda: pytest.fail("top must only read its database"),
    )
    status = read_status(database)
    assert {run["run_id"] for run in status["runs"]} == {"0", "1"}
    assert status["handle_totals"] == [
        dict(
            project="cmip7",
            mapped_items=1,
            prepared=3,
            published=3,
            errors=0,
            full_events=1,
            patch_events=0,
        )
    ]
    assert not (tmp_path / "status").exists()
    assert clean_status_history(dry_run=True) == 2
    assert len(read_status(database)["runs"]) == 2
    assert clean_status_history() == 2
    assert read_status(database)["runs"] == []
    assert read_status(database)["handle_totals"] == status["handle_totals"]
