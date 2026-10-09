import json
import logging
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest
import requests

from piddiplatsch.config import config
from piddiplatsch.handles.jsonl_backend import JsonlHandleBackend
from piddiplatsch.handles.rest_backend import HandleWriteResult
from piddiplatsch.runners.publish import HandlePublisher


class FakeBackend:
    prefix = "21.TEST"

    def __init__(self, failing_pid=None):
        self.failing_pid = failing_pid
        self.published = []

    def add(self, pid, record):
        if pid == self.failing_pid:
            raise RuntimeError("server unavailable")
        self.published.append((pid, record))


class TransientBackend(FakeBackend):
    def __init__(self, failures, exception=None):
        super().__init__()
        self.failures = failures
        self.exception = exception or requests.ConnectionError("connection lost")
        self.calls = 0

    def add(self, pid, record):
        self.calls += 1
        if self.calls <= self.failures:
            raise self.exception
        super().add(pid, record)


class ConcurrentBackend(FakeBackend):
    def __init__(self):
        super().__init__()
        self.lock = threading.Lock()
        self.active = 0
        self.max_active = 0

    def add(self, pid, record):
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            time.sleep(0.02)
            with self.lock:
                self.published.append((pid, record))
        finally:
            with self.lock:
                self.active -= 1


class ReportingBackend(FakeBackend):
    def add(self, pid, record):
        super().add(pid, record)
        return HandleWriteResult(
            action="updated",
            url=f"https://handles.example.test/api/handles/{self.prefix}/{pid}",
        )


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


def test_configured_publisher_resolves_backend_from_input_project(
    tmp_path, monkeypatch
):
    source = tmp_path / "handles.jsonl"
    write_jsonl(source, [handle_record("abc", project="cmip6")])
    backend = FakeBackend()
    selected = []

    def from_config(*, project=None, profile=None):
        selected.append((project, profile))
        return backend

    monkeypatch.setattr(
        "piddiplatsch.runners.publish.RestHandleClient.from_config", from_config
    )

    result = HandlePublisher().run([source], handle_profile="dkrz-test")

    assert result.succeeded == 1
    assert selected == [("cmip6", "dkrz-test")]


def test_configured_publisher_rejects_other_projects_per_record(tmp_path, monkeypatch):
    source = tmp_path / "handles.jsonl"
    write_jsonl(
        source,
        [
            handle_record("one", project="cmip6"),
            handle_record("two", project="cmip7"),
        ],
    )

    backend = FakeBackend()
    monkeypatch.setattr(
        "piddiplatsch.runners.publish.RestHandleClient.from_config", lambda **_: backend
    )
    result = HandlePublisher().run([source])
    assert result.succeeded == 1
    assert result.failed == 1
    assert [pid for pid, _ in backend.published] == ["one"]
    assert "does not match project 'cmip6'" in result.errors[0]


def test_logs_each_publication_and_summary(tmp_path, caplog):
    source = tmp_path / "handles.jsonl"
    write_jsonl(
        source,
        [handle_record(str(index)) for index in range(10)] + [handle_record("abc")],
    )

    with caplog.at_level(logging.INFO, logger="piddiplatsch.runners.publish"):
        HandlePublisher(ReportingBackend()).run([source], offset=10, limit=1)

    assert "Updated handle handle=21.TEST/abc" in caplog.text
    assert "url=https://handles.example.test/api/handles/21.TEST/abc" in caplog.text
    assert "project=cmip6" in caplog.text
    assert "dataset_id=-" in caplog.text
    assert "file_name=-" in caplog.text
    assert "position=11 batch=1/?" in caplog.text
    assert "Handle publication complete: published=1 total=1" in caplog.text


def test_logs_dataset_context_for_file_asset(tmp_path, caplog):
    source = tmp_path / "handles.jsonl"
    dataset = handle_record(
        "dataset",
        project=None,
        data={
            "AGGREGATION_LEVEL": "DATASET",
            "DATASET_ID": "CMIP6.CMIP.example",
        },
    )
    file = handle_record(
        "file",
        data={
            "DATASET_ID": "CMIP6.CMIP.example",
            "AGGREGATION_LEVEL": "FILE",
            "FILE_NAME": "example.nc",
            "IS_PART_OF": "hdl:21.TEST/dataset",
        },
    )
    write_jsonl(source, [dataset, file])

    with caplog.at_level(logging.INFO, logger="piddiplatsch.runners.publish"):
        HandlePublisher(ReportingBackend()).run([source], workers=2)

    file_line = next(line for line in caplog.messages if "handle=21.TEST/file" in line)
    assert "project=cmip6" in file_line
    assert "dataset_id=CMIP6.CMIP.example" in file_line
    assert "file_name=example.nc" in file_line


def test_writes_structured_result_jsonl_for_successes_and_failures(tmp_path):
    source = tmp_path / "handles.jsonl"
    result_file = tmp_path / "receipts" / "publication.jsonl"
    write_jsonl(
        source,
        [
            handle_record(
                "good",
                data={
                    "AGGREGATION_LEVEL": "FILE",
                    "DATASET_ID": "CMIP6.CMIP.example",
                    "FILE_NAME": "example.nc",
                },
            ),
            handle_record("failed"),
        ],
    )

    result = HandlePublisher(FakeBackend(failing_pid="failed")).run(
        [source], workers=2, result_file=result_file
    )

    receipts = [json.loads(line) for line in result_file.read_text().splitlines()]
    receipts_by_handle = {receipt["handle"]: receipt for receipt in receipts}
    success = receipts_by_handle["21.TEST/good"]
    failure = receipts_by_handle["21.TEST/failed"]
    assert result.result_file == result_file
    assert len(receipts) == 2
    assert success["schema_version"] == 1
    assert success["status"] == "succeeded"
    assert success["action"] == "published"
    assert success["project"] == "cmip6"
    assert success["dataset_id"] == "CMIP6.CMIP.example"
    assert success["file_name"] == "example.nc"
    assert success["source_file"] == str(source.resolve())
    assert success["source_line"] == 1
    assert success["position"] == 1
    assert success["batch_index"] == 1
    assert success["batch_total"] is None
    assert success["retry_attempts"] == 0
    assert success["error"] is None
    assert failure["status"] == "failed"
    assert failure["action"] is None
    assert failure["error"] == "server unavailable"


def test_default_result_jsonl_has_readable_unique_name(tmp_path, monkeypatch):
    config._set("consumer", "output_dir", str(tmp_path / "outputs"))
    monkeypatch.setattr(
        "piddiplatsch.runners.publish.utc_now",
        lambda: datetime(2026, 8, 27, 15, 33, 53, tzinfo=UTC),
    )
    source = tmp_path / "handles.jsonl"
    write_jsonl(source, [handle_record("abc")])

    first = HandlePublisher(FakeBackend()).run([source], project="cmip6")
    second = HandlePublisher(FakeBackend()).run([source], project="cmip6")

    assert first.result_file is not None
    assert second.result_file is not None
    assert first.result_file.parent == tmp_path / "outputs" / "published"
    assert first.result_file.name == "published_cmip6_handles_2026-08-27_15-33-53.jsonl"
    assert (
        second.result_file.name == "published_cmip6_handles_2026-08-27_15-33-53_2.jsonl"
    )
    assert first.result_file.read_text().count("\n") == 1


def test_project_selection_validates_each_record_before_publication(tmp_path):
    source = tmp_path / "handles.jsonl"
    write_jsonl(
        source,
        [handle_record("cmip6"), handle_record("cmip7", project="cmip7")],
    )
    backend = FakeBackend()

    result = HandlePublisher(backend).run([source], project="CMIP6")
    assert result.succeeded == 1
    assert result.failed == 1
    assert [pid for pid, _ in backend.published] == ["cmip6"]
    assert "handles.jsonl:2" in result.errors[0]
    assert "does not match project 'cmip6'" in result.errors[0]


def test_project_selection_reports_unreadable_record_and_publishes_valid_input(
    tmp_path,
):
    good = tmp_path / "good.jsonl"
    malformed = tmp_path / "malformed.jsonl"
    write_jsonl(good, [handle_record("good")])
    malformed.write_text("not-json\n", encoding="utf-8")
    backend = FakeBackend()

    result = HandlePublisher(backend).run([good, malformed], project="cmip6")
    assert result.succeeded == 1
    assert result.failed == 1
    assert [pid for pid, _ in backend.published] == ["good"]


def test_generic_publication_keeps_mixed_projects_and_reports_each(tmp_path):
    config._set("consumer", "output_dir", str(tmp_path / "outputs"))
    source = tmp_path / "handles.jsonl"
    write_jsonl(
        source,
        [handle_record("cmip6"), handle_record("cmip7", project="cmip7")],
    )

    result = HandlePublisher(FakeBackend()).run([source])

    assert result.project is None
    assert result.result_file is not None
    assert result.result_file.name.startswith("published_handles_")
    assert result.projects["cmip6"].succeeded == 1
    assert result.projects["cmip7"].succeeded == 1


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


def test_reports_malformed_line_without_discarding_valid_records(tmp_path):
    source = tmp_path / "handles.jsonl"
    source.write_text(
        f"{json.dumps(handle_record('first'))}\nnot-json\n", encoding="utf-8"
    )
    backend = FakeBackend()

    result = HandlePublisher(backend).run([source])

    assert result.total == 2
    assert result.succeeded == 1
    assert result.failed == 1
    assert "line 2" in result.errors[0]
    assert [pid for pid, _ in backend.published] == ["first"]


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
        f"{json.dumps(handle_record('three'))}\nmalformed-tail", encoding="utf-8"
    )
    backend = FakeBackend()

    result = HandlePublisher(backend).run([tmp_path], limit=3)

    assert result.total == 3
    assert result.succeeded == 3
    assert [pid for pid, _ in backend.published] == ["one", "two", "three"]


def test_offset_skips_records_across_files(tmp_path):
    write_jsonl(
        tmp_path / "handles_1.jsonl",
        [handle_record("one"), handle_record("two")],
    )
    write_jsonl(
        tmp_path / "handles_2.jsonl",
        [handle_record("three"), handle_record("four")],
    )
    backend = FakeBackend()

    result = HandlePublisher(backend).run([tmp_path], offset=2, limit=1)

    assert result.total == 1
    assert result.succeeded == 1
    assert [pid for pid, _ in backend.published] == ["three"]


def test_offset_and_limit_select_requested_window(tmp_path):
    source = tmp_path / "handles.jsonl"
    write_jsonl(source, [handle_record(str(index)) for index in range(5)])
    backend = FakeBackend()

    result = HandlePublisher(backend).run([source], offset=2, limit=2)

    assert result.succeeded == 2
    assert [pid for pid, _ in backend.published] == ["2", "3"]


def test_retries_transient_failures_with_exponential_backoff(tmp_path):
    source = tmp_path / "handles.jsonl"
    write_jsonl(source, [handle_record("abc")])
    backend = TransientBackend(failures=2)
    delays = []

    result = HandlePublisher(backend, sleep=delays.append).run(
        [source], retries=3, retry_delay=0.5
    )

    assert result.succeeded == 1
    assert result.failed == 0
    assert result.retry_attempts == 2
    assert backend.calls == 3
    assert delays == [0.5, 1.0]


def test_stops_retrying_after_configured_attempts(tmp_path):
    source = tmp_path / "handles.jsonl"
    write_jsonl(source, [handle_record("abc")])
    backend = TransientBackend(failures=3)
    delays = []

    result = HandlePublisher(backend, sleep=delays.append).run(
        [source], retries=2, retry_delay=1.0
    )

    assert result.succeeded == 0
    assert result.failed == 1
    assert result.retry_attempts == 2
    assert backend.calls == 3
    assert delays == [1.0, 2.0]


def test_does_not_retry_permanent_http_error(tmp_path):
    source = tmp_path / "handles.jsonl"
    write_jsonl(source, [handle_record("abc")])
    response = requests.Response()
    response.status_code = 401
    backend = TransientBackend(
        failures=1,
        exception=requests.HTTPError("authentication failed", response=response),
    )
    delays = []

    result = HandlePublisher(backend, sleep=delays.append).run([source], retries=3)

    assert result.failed == 1
    assert result.retry_attempts == 0
    assert backend.calls == 1
    assert delays == []


def test_parallel_publication_keeps_updates_for_one_pid_in_order(tmp_path):
    source = tmp_path / "handles.jsonl"
    first = handle_record("same")
    first["data"]["VERSION"] = "1"
    second = handle_record("other")
    second["data"]["VERSION"] = "1"
    third = handle_record("same")
    third["data"]["VERSION"] = "2"
    write_jsonl(source, [first, second, third])
    backend = ConcurrentBackend()

    result = HandlePublisher(backend).run([source], workers=3)

    assert result.succeeded == 3
    assert backend.max_active >= 2
    same_versions = [
        record["VERSION"] for pid, record in backend.published if pid == "same"
    ]
    assert same_versions == ["1", "2"]


def test_rejects_invalid_worker_count(tmp_path):
    source = tmp_path / "handles.jsonl"
    write_jsonl(source, [handle_record("abc")])

    with pytest.raises(ValueError, match="workers must be at least 1"):
        HandlePublisher(FakeBackend()).run([source], workers=0)


@pytest.mark.parametrize("workers", [1, 3])
def test_batches_preserve_order_context_and_global_positions(
    tmp_path, monkeypatch, workers
):
    monkeypatch.setattr("piddiplatsch.runners.publish.BATCH_SIZE", 2)
    source = tmp_path / "handles.jsonl"
    write_jsonl(
        source,
        [
            handle_record("skip"),
            handle_record(
                "same",
                data={
                    "DATASET_ID": "CMIP6.example",
                    "IS_PART_OF": "hdl:21.TEST/parent",
                    "VERSION": "1",
                },
            ),
            handle_record("other"),
            handle_record(
                "same",
                data={
                    "DATASET_ID": "CMIP6.example",
                    "IS_PART_OF": "hdl:21.TEST/parent",
                    "VERSION": "2",
                },
            ),
            handle_record("parent", data={"DATASET_ID": "CMIP6.example"}),
        ],
    )
    backend = ConcurrentBackend()
    progress = []
    result = HandlePublisher(backend).run(
        [source],
        offset=1,
        project="cmip6",
        workers=workers,
        progress_callback=lambda index, total, *_: progress.append((index, total)),
    )
    assert result.succeeded == 4
    assert [data["VERSION"] for pid, data in backend.published if pid == "same"] == [
        "1",
        "2",
    ]
    receipts = [
        json.loads(line) for line in result.result_file.read_text().splitlines()
    ]
    assert sorted(
        (r["position"], r["batch_index"], r["batch_total"]) for r in receipts
    ) == [
        (2, 1, None),
        (3, 2, None),
        (4, 3, None),
        (5, 4, None),
    ]
    assert all(
        r["dataset_id"] == "CMIP6.example"
        for r in receipts
        if r["handle"] == "21.TEST/same"
    )
    assert sorted(progress) == [(1, None), (2, None), (3, None), (4, None)]
    assert not list((tmp_path / "outputs").glob(".piddi-publish-*"))


@pytest.mark.parametrize(
    "bad_tail", ["not-json", json.dumps(handle_record("wrong", project="cmip7"))]
)
def test_rejects_bad_input_beyond_first_batch_without_undoing_writes(
    tmp_path, monkeypatch, bad_tail
):
    monkeypatch.setattr("piddiplatsch.runners.publish.BATCH_SIZE", 2)
    source = tmp_path / "handles.jsonl"
    write_jsonl(source, [handle_record(str(i)) for i in range(5)])
    with source.open("a") as stream:
        stream.write(bad_tail + "\n")
    backend = FakeBackend()
    result = HandlePublisher(backend).run([source], project="cmip6", workers=3)
    assert result.succeeded == 5
    assert result.failed == 1
    assert sorted(pid for pid, _ in backend.published) == [str(i) for i in range(5)]
    assert not list((tmp_path / "outputs").glob(".piddi-publish-*"))


def test_receipts_report_physical_lines_with_blanks_and_offset(tmp_path):
    source = tmp_path / "handles.jsonl"
    source.write_text(
        "\n"
        + json.dumps(handle_record("skip"))
        + "\n\n"
        + json.dumps(handle_record("good"))
        + "\n"
    )
    result = HandlePublisher(FakeBackend()).run([source], offset=1, limit=1)
    receipt = json.loads(result.result_file.read_text())
    assert receipt["source_line"] == 4
    assert receipt["position"] == 2


def test_parent_reference_does_not_require_parent_record(tmp_path):
    source = tmp_path / "handles.jsonl"
    write_jsonl(
        source,
        [handle_record("child", data={"IS_PART_OF": "hdl:21.TEST/absent-parent"})],
    )
    backend = FakeBackend()
    result = HandlePublisher(backend).run([source], project="cmip6")
    assert result.succeeded == 1
    assert backend.published[0][1]["IS_PART_OF"] == "hdl:21.TEST/absent-parent"
    receipt = json.loads(result.result_file.read_text())
    assert receipt["project"] == "cmip6"
    assert receipt["dataset_id"] is None


@pytest.mark.parametrize("workers", [1, 3])
def test_large_publication_memory_is_bounded_even_on_failure(tmp_path, workers):
    import tracemalloc

    class RejectingBackend:
        prefix = "21.TEST"

        def add(self, pid, record):
            raise RuntimeError("rejected")

    def peak_for(count):
        source = tmp_path / "handles.jsonl"
        with source.open("w") as stream:
            for i in range(count):
                stream.write(
                    json.dumps(handle_record(str(i), data={"payload": "x" * 4096}))
                    + "\n"
                )
        tracemalloc.start()
        try:
            # Disable captured logs: pytest would otherwise retain every failure.
            logger = logging.getLogger("piddiplatsch.runners.publish")
            previous = logger.disabled
            logger.disabled = True
            try:
                result = HandlePublisher(RejectingBackend()).run(
                    [source], workers=workers
                )
            finally:
                logger.disabled = previous
            peak = tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()
        assert result.failed == count
        assert len(result.errors) == 100
        with result.result_file.open() as stream:
            assert sum(1 for _ in stream) == count
        return peak

    small = peak_for(512)
    large = peak_for(4096)
    # Eight times as many unique handles must not retain eight times the input,
    # parent index, futures, outcomes, or exception tracebacks on the heap.
    assert large < small * 2 + 2_000_000


@pytest.mark.parametrize("workers", [1, 3])
def test_reads_each_source_once_and_publishes_before_reading_next_batch(
    tmp_path, monkeypatch, workers
):
    monkeypatch.setattr("piddiplatsch.runners.publish.BATCH_SIZE", 2)
    source = tmp_path / "handles.jsonl"
    write_jsonl(source, [handle_record(str(i)) for i in range(6)])
    original_open = Path.open
    opened = []
    consumed = []
    backend = FakeBackend()

    class WatchedStream:
        def __init__(self, stream):
            self.stream = stream

        def __enter__(self):
            self.stream.__enter__()
            return self

        def __exit__(self, *args):
            return self.stream.__exit__(*args)

        def __iter__(self):
            for i, line in enumerate(self.stream):
                if i >= 2:
                    assert len(backend.published) >= (i // 2) * 2
                consumed.append(i)
                yield line

    def watched_open(path, *args, **kwargs):
        stream = original_open(path, *args, **kwargs)
        if path == source:
            opened.append(stream)
            return WatchedStream(stream)
        return stream

    monkeypatch.setattr(Path, "open", watched_open)
    result = HandlePublisher(backend).run(
        [source], project="cmip6", workers=workers, limit=5
    )
    assert result.succeeded == 5
    assert len(opened) == 1
    assert opened[0].closed
    assert consumed == list(range(5))


@pytest.mark.parametrize(
    "bad_record",
    [[], None, 42, "bad", {"project": "cmip6"}, handle_record("missing", project=None)],
)
def test_invalid_records_are_receipted_and_next_valid_record_is_published(
    tmp_path, bad_record
):
    source = tmp_path / "handles.jsonl"
    write_jsonl(source, [bad_record, handle_record("good")])
    backend = FakeBackend()
    result = HandlePublisher(backend).run([source], project="cmip6")
    assert result.failed == 1
    assert result.succeeded == 1
    assert [pid for pid, _ in backend.published] == ["good"]
    receipts = [
        json.loads(line) for line in result.result_file.read_text().splitlines()
    ]
    assert receipts[0]["status"] == "failed"
    assert receipts[0]["source_line"] == 1
    assert receipts[1]["status"] == "succeeded"
