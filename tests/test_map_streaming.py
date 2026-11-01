"""Catalog mapping stays lazy, deterministic, and closes bounded selections."""

import tracemalloc

import pytest

from piddiplatsch.catalog import CatalogEntry
from piddiplatsch.catalog.sqlite import SQLiteCatalog
from piddiplatsch.config import config
from piddiplatsch.core.messages import KafkaSource
from piddiplatsch.core.pipeline import process_messages
from piddiplatsch.result import ProcessingResult
from piddiplatsch.runners.mapping import map_catalog


class CountingProcessor:
    def __init__(self):
        self.count = 0
        self.last_key = None

    def process(self, key, value):
        self.count += 1
        self.last_key = key
        return ProcessingResult(key=key, success=True)


@pytest.fixture
def processor(monkeypatch):
    processor = CountingProcessor()
    monkeypatch.setattr(
        "piddiplatsch.runners.sync.build_processing_target",
        lambda **kwargs: processor,
    )
    return processor


def seed(directory, project="cmip6", count=4, payload_size=0):
    config._set("batch", "directory", str(directory / "batches"))
    with SQLiteCatalog(project, directory / "piddi.db") as catalog:
        with catalog.transaction() as transaction:
            for index in range(count):
                entry = CatalogEntry(
                    project.upper(),
                    {
                        "id": f"{project}.dataset.v{index:06}",
                        "properties": {},
                        "padding": "x" * payload_size,
                    },
                    f"{project}.dataset",
                    index,
                )
                transaction.put_item(entry)
                transaction.add_work(entry, KafkaSource(project, 0, index))


@pytest.mark.parametrize("stop", [False, True])
def test_map_processes_before_reading_more_and_closes_early(
    tmp_path, monkeypatch, processor, stop
):
    seed(tmp_path)
    get_work = SQLiteCatalog.get_work
    close = SQLiteCatalog.close
    read_count = 0
    closed = []

    def tracked_work(catalog, identity):
        nonlocal read_count
        assert processor.count == read_count
        read_count += 1
        return get_work(catalog, identity)

    def tracked_close(catalog):
        closed.append(catalog)
        close(catalog)

    if stop:

        def skip(key, value):
            processor.count += 1
            return ProcessingResult(key=key, error="invalid record")

        monkeypatch.setattr(processor, "process", skip)
    monkeypatch.setattr(SQLiteCatalog, "get_work", tracked_work)
    monkeypatch.setattr(SQLiteCatalog, "close", tracked_close)
    if stop:
        assert map_catalog(directory=tmp_path, projects=["cmip6"]).failed == 1
        assert read_count == 1
    else:
        result = map_catalog(directory=tmp_path, projects=["cmip6"], limit=3)
        assert result.total == result.succeeded == 3
        assert read_count == 3
    assert closed
    assert all(catalog._connection is None for catalog in closed)


def test_map_offset_and_limit_count_across_selected_projects(tmp_path, processor):
    seed(tmp_path, "cmip6", count=2)
    seed(tmp_path, "cmip7", count=2)
    result = map_catalog(
        directory=tmp_path, projects=["cmip6", "cmip7"], offset=3, limit=1
    )
    assert result.total == result.succeeded == 1
    assert processor.last_key == "cmip7.dataset.v000001"


def test_empty_selection_skips_processor_setup(tmp_path, monkeypatch):
    seed(tmp_path, count=1)

    def unexpected_setup(**kwargs):
        pytest.fail("Empty selection should not initialize processors")

    monkeypatch.setattr(
        "piddiplatsch.runners.sync.build_processing_target", unexpected_setup
    )
    assert map_catalog(directory=tmp_path, projects=["cmip6"], offset=1).total == 0


def test_direct_feed_accepts_unsized_iterators(processor):
    def messages():
        for index in range(5):
            assert processor.count == index
            yield str(index), {"id": index}

    result = process_messages(messages(), processor=processor)
    assert result.total == result.succeeded == 5


def test_map_memory_does_not_scale_with_catalog_size(tmp_path, processor):
    def peak_for(count):
        directory = tmp_path / str(count)
        seed(directory, count=count, payload_size=8192)
        tracemalloc.start()
        try:
            result = map_catalog(directory=directory, projects=["cmip6"])
            peak = tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()
        assert result.total == result.succeeded == count
        return peak

    small = peak_for(64)
    large = peak_for(2048)
    assert large < small * 2 + 1_000_000
