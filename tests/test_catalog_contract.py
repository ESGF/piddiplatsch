"""Catalog identity/checkpoint rules and backend resource ownership."""

import pytest

from piddiplatsch.catalog import (
    BaseCatalog,
    BaseCatalogTransaction,
    CatalogEntry,
    CatalogError,
    KafkaCheckpoint,
)
from piddiplatsch.exceptions import PiddiplatschError


def test_entry_preserves_complete_item_and_explicit_version_metadata():
    item = {
        "id": "dataset.v20260101",
        "collection": "CMIP6",
        "properties": {"custom:field": "value"},
        "assets": {"file": {"href": "https://example.test/file"}},
    }
    entry = CatalogEntry("CMIP6", item, "dataset", 20260101)
    assert entry.item_id == item["id"]
    assert entry.item == item
    assert entry.dataset_id == "dataset"
    assert entry.version_number == 20260101


@pytest.mark.parametrize(
    "overrides",
    [
        {"collection_id": ""},
        {"item": []},
        {"item": {}},
        {"item": {"id": ""}},
        {"item": {"id": 123}},
        {"item": {"id": "item", "collection": "other"}},
        {"dataset_id": ""},
        {"version_number": -1},
        {"version_number": True},
        {"version_number": "20260101"},
    ],
)
def test_entry_rejects_invalid_index_metadata(overrides):
    values = {
        "collection_id": "CMIP6",
        "item": {"id": "item"},
        "dataset_id": "dataset",
        "version_number": 20260101,
    }
    values.update(overrides)
    with pytest.raises(ValueError):
        CatalogEntry(**values)


def test_checkpoint_uses_next_offset_including_zero():
    assert KafkaCheckpoint("publications", 0, 0).next_offset == 0
    assert KafkaCheckpoint("publications", 2, 43).next_offset == 43


@pytest.mark.parametrize(
    "topic, partition, next_offset",
    [
        ("", 0, 0),
        (None, 0, 0),
        ("topic", -1, 0),
        ("topic", True, 0),
        ("topic", 0, -1),
        ("topic", 0, 1.5),
        ("topic", 0, True),
    ],
)
def test_checkpoint_rejects_invalid_source_coordinates(topic, partition, next_offset):
    with pytest.raises(ValueError):
        KafkaCheckpoint(topic, partition, next_offset)


def test_backends_must_implement_storage_contracts():
    with pytest.raises(TypeError):
        BaseCatalog("cmip6")
    with pytest.raises(TypeError):
        BaseCatalogTransaction()
    assert issubclass(CatalogError, PiddiplatschError)


class LifecycleCatalog(BaseCatalog):
    """Minimal adapter to exercise shared lifecycle without claiming persistence."""

    def __init__(self, project):
        super().__init__(project)
        self.closed = False

    def close(self):
        self.closed = True

    def get_item(self, collection_id, item_id):
        raise NotImplementedError

    def find_previous_version(self, collection_id, dataset_id, version_number):
        raise NotImplementedError

    def find_versions(self, collection_id, dataset_id, limit=100):
        raise NotImplementedError

    def pending_changes(self):
        raise NotImplementedError

    def iter_pending(self, limit):
        raise NotImplementedError

    def acknowledge_mapped(self, generation, sequence):
        raise NotImplementedError

    def get_checkpoint(self, topic, partition):
        raise NotImplementedError

    def latest_event_date(self):
        raise NotImplementedError

    def transaction(self):
        raise NotImplementedError

    def iter_items(self, *, collection_id=None):
        raise NotImplementedError

    def reset(self):
        raise NotImplementedError

    def compact(self):
        raise NotImplementedError


@pytest.mark.parametrize("project", ["", None, 1])
def test_catalog_requires_project_scope(project):
    with pytest.raises(ValueError):
        LifecycleCatalog(project)


def test_catalog_context_closes_resources():
    catalog = LifecycleCatalog("cmip6")
    with catalog as opened:
        assert opened is catalog
        assert opened.project == "cmip6"
        assert not opened.closed
    assert catalog.closed


def test_catalog_context_closes_without_suppressing_processing_errors():
    catalog = LifecycleCatalog("cmip6")
    with pytest.raises(ValueError, match="patch failed"), catalog:
        raise ValueError("patch failed")
    assert catalog.closed
