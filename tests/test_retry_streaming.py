import json
import logging
import tracemalloc
from pathlib import Path

import pytest

from piddiplatsch.result import FeedResult
from piddiplatsch.runners.retry import RetryRunner


def write_records(path, records):
    with path.open("w") as stream:
        for record in records:
            stream.write(json.dumps(record) + "\n")


def runner(tmp_path, **kwargs):
    return RetryRunner(projects=["cmip7"], failure_dir=tmp_path / "failures", **kwargs)


def test_retry_streams_bounded_batches_in_source_order(tmp_path, monkeypatch):
    monkeypatch.setattr("piddiplatsch.runners.retry.BATCH_SIZE", 2)
    source = tmp_path / "retry.jsonl"
    write_records(
        source,
        [
            {"key": str(i), "__infos__": {"project": project, "retries": 2}}
            for i, project in enumerate(["cmip6", "cmip7", "cmip6", "cmip6", "cmip7"])
        ],
    )
    processed = []
    opened = []
    original_open = Path.open

    class WatchedStream:
        def __init__(self, stream):
            self.stream = stream
            self.lines = 0

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.stream.close()

        def fileno(self):
            return self.stream.fileno()

        def readline(self, size):
            assert len(processed) >= (self.lines // 2) * 2
            self.lines += 1
            return self.stream.readline(size)

    def tracked_open(path, *args, **kwargs):
        stream = original_open(path, *args, **kwargs)
        if path == source:
            opened.append(stream)
            return WatchedStream(stream)
        return stream

    def feed(messages, **kwargs):
        assert len(messages) <= 2
        for key, record in messages:
            assert kwargs["projects"] == [record["__infos__"]["project"]]
            assert record["__infos__"]["retries"] == 3
            processed.append(key)
        return FeedResult(total=len(messages), succeeded=len(messages))

    monkeypatch.setattr(Path, "open", tracked_open)
    monkeypatch.setattr("piddiplatsch.runners.retry.process_messages", feed)
    result = runner(tmp_path).run_file(source)
    assert result.succeeded == result.total == 5
    assert processed == [str(i) for i in range(5)]
    assert len(opened) == 1
    assert opened[0].closed


def test_retry_continues_after_bad_input_and_keeps_source(tmp_path, monkeypatch):
    source = tmp_path / "retry.jsonl"
    source.write_text(
        '{"key":"one"}\nnot-json\n{"__infos__": []}\n{"retries":"bad"}\n{"key":"two"}\n'
    )
    seen = []

    def feed(messages, **kwargs):
        seen.extend(key for key, _ in messages)
        return FeedResult(total=len(messages), succeeded=len(messages))

    monkeypatch.setattr("piddiplatsch.runners.retry.process_messages", feed)
    result = runner(tmp_path, delete_after=True).run_file(source)
    assert seen == ["one", "two"]
    assert result.total == 5
    assert result.failed == 3
    assert result.succeeded == 2
    assert source.exists()
    assert all(
        f"line {i}" in error for i, error in zip([2, 3, 4], result.errors, strict=True)
    )


@pytest.mark.parametrize("count", [1, 3])
def test_retry_does_not_read_or_delete_appended_records(tmp_path, monkeypatch, count):
    monkeypatch.setattr("piddiplatsch.runners.retry.BATCH_SIZE", 1)
    source = tmp_path / "retry.jsonl"
    # No trailing newline also exercises the byte boundary of the snapshot.
    source.write_text(
        "\n".join(json.dumps({"key": f"original-{i}"}) for i in range(count))
    )
    seen = []

    def feed(messages, **kwargs):
        seen.extend(key for key, _ in messages)
        with source.open("a") as stream:
            stream.write('\n{"key":"appended"}\n')
        return FeedResult(total=len(messages), succeeded=len(messages))

    monkeypatch.setattr("piddiplatsch.runners.retry.process_messages", feed)
    result = runner(tmp_path, delete_after=True).run_file(source)
    assert result.total == result.succeeded == count
    assert seen == [f"original-{i}" for i in range(count)]
    assert source.exists()
    assert "appended" in source.read_text()


def test_retry_closes_input_when_processing_stops(tmp_path, monkeypatch):
    monkeypatch.setattr("piddiplatsch.runners.retry.BATCH_SIZE", 1)
    source = tmp_path / "retry.jsonl"
    write_records(source, [{"key": "one"}, {"key": "two"}])
    opened = []
    original_open = Path.open

    def tracked_open(path, *args, **kwargs):
        stream = original_open(path, *args, **kwargs)
        if path == source:
            opened.append(stream)
        return stream

    def stop(*args, **kwargs):
        raise RuntimeError("stop processing")

    monkeypatch.setattr(Path, "open", tracked_open)
    monkeypatch.setattr("piddiplatsch.runners.retry.process_messages", stop)
    with pytest.raises(RuntimeError, match="stop processing"):
        runner(tmp_path, delete_after=True).run_file(source)
    assert source.exists()
    assert len(opened) == 1
    assert opened[0].closed


def test_retry_memory_is_bounded(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "piddiplatsch.runners.retry.process_messages",
        lambda messages, **kwargs: FeedResult(
            total=len(messages), succeeded=len(messages)
        ),
    )

    def peak_for(count):
        source = tmp_path / "retry.jsonl"
        write_records(
            source, ({"key": str(i), "payload": "x" * 4096} for i in range(count))
        )
        tracemalloc.start()
        try:
            result = runner(tmp_path).run_file(source)
            peak = tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()
        assert result.total == result.succeeded == count
        return peak

    small = peak_for(512)
    large = peak_for(4096)
    assert large < small * 2 + 1_000_000


def test_retry_caps_error_summaries_and_retains_input(tmp_path, caplog):
    source = tmp_path / "retry.jsonl"
    source.write_text("not-json\n" * 120)
    with caplog.at_level(logging.CRITICAL):
        result = runner(tmp_path, delete_after=True).run_file(source)
    assert result.failed == result.total == 120
    assert len(result.errors) == 100
    assert source.exists()
