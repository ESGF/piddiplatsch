import datetime
import sqlite3

from piddiplatsch.monitoring.stats import CounterKey, SQLiteReporter, Stats


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


def test_sqlite_reporter_persists_filtered_counter(tmp_path):
    db_path = tmp_path / "stats.db"
    reporter = SQLiteReporter(str(db_path))
    summary = Stats(enable_db=False).summary()
    summary[CounterKey.FILTERED.value] = 3

    reporter.log(summary)
    reporter.close()

    with sqlite3.connect(db_path) as connection:
        row = connection.execute(
            "SELECT filtered_messages FROM message_stats"
        ).fetchone()
    assert row == (3,)
