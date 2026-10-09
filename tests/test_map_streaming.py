import json
import tracemalloc
from pathlib import Path

import pytest

from piddiplatsch.consumer import HarvestProcessor, feed_messages_direct, map_dump_files
from piddiplatsch.exceptions import JsonlReadError, StopOnTransientSkipError
from piddiplatsch.result import ProcessingResult


class CountingProcessor(HarvestProcessor):
    def __init__(self):
        self.count = 0
        self.preflights = 0
        self.last_key = None

    def preflight_check(self, **kwargs):
        self.preflights += 1

    def process(self, key, value):
        self.count += 1
        self.last_key = key
        return super().process(key, value)


@pytest.fixture
def processor(monkeypatch):
    processor = CountingProcessor()
    monkeypatch.setattr(
        "piddiplatsch.consumer.build_processing_target", lambda **kwargs: processor
    )
    return processor


@pytest.mark.parametrize("stop", [False, True])
def test_map_opens_once_and_processes_before_reading_more(
    tmp_path, monkeypatch, processor, stop
):
    source = tmp_path / "raw.jsonl"
    source.write_text("".join(json.dumps({"id": i}) + "\n" for i in range(4)))
    original_open = Path.open
    opened = []
    read_count = 0

    class WatchedStream:
        def __init__(self, stream):
            self.stream = stream

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.stream.close()

        def __iter__(self):
            nonlocal read_count
            for i, line in enumerate(self.stream):
                assert processor.count == i
                read_count += 1
                yield line

    def tracked_open(path, *args, **kwargs):
        stream = original_open(path, *args, **kwargs)
        if path == source:
            opened.append(stream)
            return WatchedStream(stream)
        return stream

    if stop:

        def skip(key, value):
            processor.count += 1
            return ProcessingResult(
                key=key, skipped=True, transient_skip=True, skip_reason="offline"
            )

        monkeypatch.setattr(processor, "process", skip)
    monkeypatch.setattr(Path, "open", tracked_open)
    if stop:
        with pytest.raises(StopOnTransientSkipError):
            map_dump_files([source], projects=["cmip6"])
        assert read_count == 1
    else:
        result = map_dump_files([source], projects=["cmip6"], limit=3)
        assert result.total == result.succeeded == 3
        assert read_count == 3
    assert processor.preflights == 1
    assert len(opened) == 1
    assert opened[0].closed


def test_map_offset_across_files_and_limit_excludes_malformed_tail(tmp_path, processor):
    first, second = tmp_path / "1.jsonl", tmp_path / "2.jsonl"
    first.write_text('{"id": 0}\n{"id": 1}\n')
    second.write_text('\n{"id": 2}\n\n{"id": 3}\nnot-json\n')
    result = map_dump_files([second, first], projects=["cmip6"], offset=3, limit=1)
    assert result.total == result.succeeded == 1
    assert processor.last_key == f"{second}:4"


def test_map_stops_at_malformed_line_after_processing_valid_prefix(tmp_path, processor):
    source = tmp_path / "raw.jsonl"
    source.write_text('{"id": 1}\nnot-json\n{"id": 3}\n')
    with pytest.raises(JsonlReadError, match="line 2"):
        map_dump_files([source], projects=["cmip6"])
    assert processor.count == 1


def test_empty_selection_skips_processor_setup(tmp_path, monkeypatch):
    source = tmp_path / "raw.jsonl"
    source.write_text('{"id": 1}\n')

    def unexpected_setup(**kwargs):
        pytest.fail("Empty selection should not initialize the processor")

    monkeypatch.setattr(
        "piddiplatsch.consumer.build_processing_target", unexpected_setup
    )
    assert map_dump_files([source], offset=1).total == 0


def test_direct_feed_accepts_unsized_iterators_and_counts_consumed_records(processor):
    def messages():
        for i in range(5):
            assert processor.count == i
            yield str(i), {"id": i}

    result = feed_messages_direct(messages(), processor=processor)
    assert result.total == result.succeeded == 5


def test_map_memory_does_not_scale_with_input_size(tmp_path, processor):
    def peak_for(count):
        source = tmp_path / "raw.jsonl"
        with source.open("w") as stream:
            for i in range(count):
                stream.write(json.dumps({"id": i, "payload": "x" * 8192}) + "\n")
        tracemalloc.start()
        try:
            result = map_dump_files([source], projects=["cmip6"])
            peak = tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()
        assert result.total == result.succeeded == count
        return peak

    small = peak_for(64)
    large = peak_for(2048)
    assert large < small * 2 + 1_000_000
