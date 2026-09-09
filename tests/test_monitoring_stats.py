import datetime
import sqlite3
from types import SimpleNamespace

import pytest

from piddiplatsch.config import config
from piddiplatsch.consumer import HarvestProcessor, start_consumer
from piddiplatsch.monitoring.stats import (
    CounterKey,
    SQLiteReporter,
    Stats,
    concise_error,
)
from piddiplatsch.monitoring.status import read_status
from piddiplatsch.result import ProcessingResult


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
    assert status["schema_version"] == 2
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
    reporter = SQLiteReporter(str(db_path))
    reporter.close()

    status = read_status(db_path, project="cmip7")
    assert status["runs"] == []


def test_fresh_database_contains_only_current_monitoring_tables(tmp_path):
    db_path = tmp_path / "piddi.db"
    reporter = SQLiteReporter(str(db_path))
    reporter.close()

    with sqlite3.connect(db_path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }

    assert tables == {
        "monitor_meta",
        "monitor_runs",
        "monitor_project_stats",
        "monitor_samples",
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
def test_consumer_entrypoint_never_creates_monitoring_database(tmp_path, processor):
    db_path = tmp_path / "piddi.db"
    config._set("stats", "enable_db", True)
    config._set("stats", "db_path", str(db_path))

    start_consumer(
        processor=processor,
        direct_messages=[("one", {"value": 1})],
        force=True,
    )

    assert not db_path.exists()
