# tests/test_dump.py
import json
from datetime import UTC, datetime

import pytest

from piddiplatsch.config import config
from piddiplatsch.persist.dump import DumpRecorder


@pytest.fixture
def temp_dump_dir(tmp_path, monkeypatch):
    """
    Provide a temporary dump directory and patch DumpRecorder.DUMP_DIR.
    Ensures tests do not affect the real filesystem.
    """
    dump_dir = tmp_path / "dump"
    dump_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(DumpRecorder, "DUMP_DIR", dump_dir)
    return dump_dir


def test_record_single_item(temp_dump_dir):
    """Test that a single record is correctly written to a JSONL file."""
    key = "testkey"
    data = {"foo": "bar"}

    DumpRecorder().record(key, data)

    files = list(temp_dump_dir.glob("*.jsonl"))
    assert len(files) == 1

    with files[0].open("r", encoding="utf-8") as f:
        lines = f.readlines()

    assert len(lines) == 1
    assert json.loads(lines[0]) == data


def test_record_multiple_items_append(temp_dump_dir):
    """Test that multiple records append to the same JSONL file."""
    records = [
        ("key1", {"foo": "bar"}),
        ("key2", {"baz": 123}),
        ("key3", {"nested": {"a": 1}}),
    ]

    for key, data in records:
        DumpRecorder().record(key, data)

    files = list(temp_dump_dir.glob("*.jsonl"))
    assert len(files) == 1
    dump_file = files[0]

    with dump_file.open("r", encoding="utf-8") as f:
        lines = f.readlines()

    assert len(lines) == len(records)
    for line, (_, data) in zip(lines, records, strict=False):
        assert json.loads(line) == data


def test_dump_filename_contains_today(temp_dump_dir):
    """Test that the dump filename includes today's date."""
    key = "datekey"
    data = {"hello": "world"}

    DumpRecorder().record(key, data)

    files = list(temp_dump_dir.glob("*.jsonl"))
    assert len(files) == 1

    today = datetime.now(UTC).date()
    expected_filename = f"dump_messages_{today}_000001.jsonl"
    actual_filename = files[0].name

    assert actual_filename == expected_filename


def test_rotation_and_restart_preserve_order(temp_dump_dir):
    config._set("consumer", "dump_max_events", 2)
    recorder = DumpRecorder()
    for number in range(3):
        recorder.record(str(number), {"event": number})
    # A restart resumes the partly filled second file.
    recorder = DumpRecorder()
    for number in range(3, 6):
        recorder.record(str(number), {"event": number})
    files = sorted(temp_dump_dir.glob("*.jsonl"))
    assert len(files) == 3
    assert [len(path.read_text().splitlines()) for path in files] == [2, 2, 2]
    assert [
        json.loads(line)["event"]
        for path in files
        for line in path.read_text().splitlines()
    ] == list(range(6))


def test_multiple_recorders_respect_part_limit(temp_dump_dir):
    config._set("consumer", "dump_max_events", 2)
    first, second = DumpRecorder(), DumpRecorder()
    for number in range(7):
        (first if number % 2 else second).record(str(number), {"event": number})
    files = sorted(temp_dump_dir.glob("*.jsonl"))
    assert [len(path.read_text().splitlines()) for path in files] == [2, 2, 2, 1]
    assert [
        json.loads(line)["event"]
        for path in files
        for line in path.read_text().splitlines()
    ] == list(range(7))


def test_rotation_at_utc_day_boundary(temp_dump_dir, monkeypatch):
    now = datetime(2026, 11, 1, 23, 59, tzinfo=UTC)
    monkeypatch.setattr("piddiplatsch.persist.dump.utc_now", lambda: now)
    recorder = DumpRecorder()
    first = recorder.record("first", {"event": 1})
    now = datetime(2026, 11, 2, tzinfo=UTC)
    second = recorder.record("second", {"event": 2})
    assert first.name == "dump_messages_2026-11-01_000001.jsonl"
    assert second.name == "dump_messages_2026-11-02_000001.jsonl"


def test_existing_daily_and_compressed_parts_are_preserved(temp_dump_dir):
    day = datetime.now(UTC).date()
    original = temp_dump_dir / f"dump_messages_{day}.jsonl"
    original.write_text('{"old": true}\n')
    archived = temp_dump_dir / f"dump_messages_{day}_000001.jsonl.gz"
    archived.write_bytes(b"existing archive")
    path = DumpRecorder().record("new", {"event": 1})
    assert path.name == f"dump_messages_{day}_000002.jsonl"
    assert original.read_text() == '{"old": true}\n'
    assert archived.read_bytes() == b"existing archive"


def test_incomplete_event_is_not_appended_to(temp_dump_dir):
    day = datetime.now(UTC).date()
    path = temp_dump_dir / f"dump_messages_{day}_000001.jsonl"
    path.write_bytes(b'{"incomplete":')
    with pytest.raises(OSError, match="Incomplete last event"):
        DumpRecorder().record("new", {"event": 1})
    assert path.read_bytes() == b'{"incomplete":'
