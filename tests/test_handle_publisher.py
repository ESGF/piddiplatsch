import json

from piddiplatsch.config import config
from piddiplatsch.handles.jsonl_backend import JsonlHandleBackend
from piddiplatsch.handles.publish import HandlePublisher


class FakeBackend:
    prefix = "21.TEST"

    def __init__(self, failing_pid=None):
        self.failing_pid = failing_pid
        self.published = []

    def add(self, pid, record):
        if pid == self.failing_pid:
            raise RuntimeError("server unavailable")
        self.published.append((pid, record))


def write_jsonl(path, records):
    path.write_text(
        "".join(f"{json.dumps(record)}\n" for record in records), encoding="utf-8"
    )


def handle_record(pid, **overrides):
    record = {
        "handle": f"21.TEST/{pid}",
        "URL": f"https://example.test/{pid}",
        "data": {"AGGREGATION_LEVEL": "DATASET"},
        "timestamp": "2026-08-24T12:00:00+00:00",
        "project": "cmip6",
    }
    record.update(overrides)
    return record


def test_publishes_prepared_records_without_changing_source(tmp_path):
    source = tmp_path / "handles_2026-08-24.jsonl"
    write_jsonl(source, [handle_record("abc"), handle_record("nested/def")])
    original = source.read_bytes()
    backend = FakeBackend()

    result = HandlePublisher(backend).run([source])

    assert result.total == 2
    assert result.succeeded == 2
    assert result.failed == 0
    assert backend.published == [
        (
            "abc",
            {
                "URL": "https://example.test/abc",
                "AGGREGATION_LEVEL": "DATASET",
            },
        ),
        (
            "nested/def",
            {
                "URL": "https://example.test/nested/def",
                "AGGREGATION_LEVEL": "DATASET",
            },
        ),
    ]
    assert source.read_bytes() == original


def test_continues_after_invalid_record_and_backend_failure(tmp_path):
    source = tmp_path / "handles.jsonl"
    write_jsonl(
        source,
        [
            handle_record("wrong", handle="20.WRONG/wrong"),
            handle_record("failed"),
            handle_record("good"),
        ],
    )
    backend = FakeBackend(failing_pid="failed")

    result = HandlePublisher(backend).run([source])

    assert result.total == 3
    assert result.succeeded == 1
    assert result.failed == 2
    assert backend.published == [
        (
            "good",
            {
                "URL": "https://example.test/good",
                "AGGREGATION_LEVEL": "DATASET",
            },
        )
    ]
    assert "does not match configured prefix" in result.errors[0]
    assert "server unavailable" in result.errors[1]


def test_rejects_malformed_jsonl_without_publishing_partial_file(tmp_path):
    source = tmp_path / "handles.jsonl"
    source.write_text(
        f'{json.dumps(handle_record("first"))}\nnot-json\n', encoding="utf-8"
    )
    backend = FakeBackend()

    result = HandlePublisher(backend).run([source])

    assert result.total == 1
    assert result.failed == 1
    assert "line 2" in result.errors[0]
    assert backend.published == []


def test_reads_all_jsonl_files_in_directory(tmp_path):
    write_jsonl(tmp_path / "handles_2026-08-23.jsonl", [handle_record("one")])
    write_jsonl(tmp_path / "handles_2026-08-24.jsonl", [handle_record("two")])
    (tmp_path / "notes.txt").write_text("ignored", encoding="utf-8")
    backend = FakeBackend()

    result = HandlePublisher(backend).run([tmp_path])

    assert result.succeeded == 2
    assert [pid for pid, _ in backend.published] == ["one", "two"]


def test_publishes_jsonl_backend_output(tmp_path):
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("handle", "prefix", "21.TEST")
    JsonlHandleBackend(project="cmip6").add(
        "abc",
        {
            "URL": "https://example.test/abc",
            "AGGREGATION_LEVEL": "DATASET",
        },
    )
    source = next((tmp_path / "cmip6" / "handles").glob("handles_*.jsonl"))
    backend = FakeBackend()

    result = HandlePublisher(backend).run([source])

    assert result.succeeded == 1
    assert backend.published == [
        (
            "abc",
            {
                "URL": "https://example.test/abc",
                "AGGREGATION_LEVEL": "DATASET",
            },
        )
    ]


def test_limit_caps_records_across_files_and_stops_reading(tmp_path):
    first = tmp_path / "handles_1.jsonl"
    second = tmp_path / "handles_2.jsonl"
    write_jsonl(first, [handle_record("one"), handle_record("two")])
    second.write_text(
        f'{json.dumps(handle_record("three"))}\nmalformed-tail', encoding="utf-8"
    )
    backend = FakeBackend()

    result = HandlePublisher(backend).run([tmp_path], limit=3)

    assert result.total == 3
    assert result.succeeded == 3
    assert [pid for pid, _ in backend.published] == ["one", "two", "three"]
