from datetime import UTC, datetime

import pytest

from piddiplatsch.config import config
from piddiplatsch.core.models import HostingNode
from piddiplatsch.plugins.cmip6.record import CMIP6DatasetRecord

pytestmark = [pytest.mark.plugin, pytest.mark.cmip6]

# -----------------------------
# Fixtures
# -----------------------------


@pytest.fixture
def stac_item_basic():
    """Basic STAC item with minimal properties and assets."""
    return {
        "id": "CMIP6.TEST.001",
        "properties": {
            "pid": "12345678-1234-1234-1234-1234567890ab",
            "retracted": "false",
        },
        "assets": {
            "reference_file": {
                "alternate:name": "host1",
                "published_on": "2024-01-01T00:00:00Z",
            },
            "data0000": {"alternate:name": "host2"},
        },
    }


@pytest.fixture
def stac_item_complex():
    """Complex STAC item with multiple assets, retracted, with valid PID."""
    return {
        "id": "CMIP6.COMP.002",
        "properties": {
            "pid": "abcdefab-1234-5678-90ab-cdef12345678",
            "retracted": "true",
        },
        "assets": {
            "reference_file": {
                "alternate:name": "host1",
                "published_on": "2023-12-31T12:00:00Z",
                "alternate": {
                    "replica1": {"published_on": "2023-12-30T12:00:00Z"},
                    "replica2": {},
                },
            },
            "data0000": {"alternate:name": "host2"},
            "data0001": {"alternate:name": "host3"},
            "data0002": {"alternate:name": "host4"},
        },
    }


@pytest.fixture
def stac_item_with_alternates():
    """STAC item with alternates, some missing 'published_on', valid PID."""
    return {
        "id": "CMIP6.ALT.003",
        "properties": {
            "pid": "11223344-5566-7788-99aa-bbccddeeff00",
            "retracted": "false",
        },
        "assets": {
            "reference_file": {
                "alternate:name": "host1",
                "published_on": "2024-01-01T00:00:00Z",
                "alternate": {
                    "replica1": {"published_on": "2024-01-02T00:00:00Z"},
                    "replica2": {},
                },
            },
            "data0000": {"alternate:name": "host2"},
            "data0001": {"alternate:name": "host3"},
        },
    }


@pytest.fixture(autouse=True)
def disable_lookup():
    """Automatically disable lookup for all tests."""
    config._set("lookup", "enabled", False)


# -----------------------------
# Basic property tests
# -----------------------------


def test_dataset_id_and_version(stac_item_basic):
    record = CMIP6DatasetRecord(stac_item_basic)
    assert record.dataset_id == "CMIP6.TEST"
    assert record.dataset_version == "001"


def test_pid_existing(stac_item_basic):
    record = CMIP6DatasetRecord(stac_item_basic)
    assert record.pid == stac_item_basic["properties"]["pid"]


def test_has_parts_respects_max_parts(stac_item_complex):
    config._set("cmip6", "max_parts", 2)
    record = CMIP6DatasetRecord(stac_item_complex)
    parts = record.has_parts
    assert len(parts) == 2
    assert all(p.startswith("hdl:") for p in parts)


def test_host_and_published_on(stac_item_basic):
    record = CMIP6DatasetRecord(stac_item_basic)
    assert record.host == "host1"
    expected_dt = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    assert record.published_on == expected_dt


def test_hosting_node_and_replicas(stac_item_complex):
    record = CMIP6DatasetRecord(stac_item_complex)
    hosting_node = record.hosting_node
    assert isinstance(hosting_node, HostingNode)
    assert hosting_node.host == "host1"

    replicas = record.replica_nodes
    hosts = [n.host for n in replicas]
    assert "replica1" in hosts
    assert "replica2" in hosts


def test_retracted_behavior(stac_item_complex):
    record = CMIP6DatasetRecord(stac_item_complex)
    assert record.retracted
    assert isinstance(record.retracted_on, datetime)


def test_is_part_of_default(stac_item_basic):
    record = CMIP6DatasetRecord(stac_item_basic)
    assert record.is_part_of is None


def test_previous_version_returns_none_when_lookup_disabled(stac_item_basic):
    record = CMIP6DatasetRecord(stac_item_basic)
    assert record.previous_version is None


def test_as_handle_model_includes_expected_fields(stac_item_complex):
    record = CMIP6DatasetRecord(stac_item_complex)
    model = record.as_handle_model()
    assert model.DATASET_ID == "CMIP6.COMP"
    assert model.DATASET_VERSION == "002"
    assert model.HOSTING_NODE.host == "host1"
    assert model.REPLICA_NODES
    assert model.HAS_PARTS
    assert model._RETRACTED is False


def test_max_parts_limit_warning(caplog, stac_item_complex):
    config._set("cmip6", "max_parts", 1)
    record = CMIP6DatasetRecord(stac_item_complex)
    _ = record.has_parts
    assert any(
        "Asset link limit applied: project=cmip6, dataset=CMIP6.COMP.002, "
        "included=1, available=4" in rec.message
        for rec in caplog.records
    )


def test_zero_max_parts_disables_asset_links_without_warning(caplog, stac_item_complex):
    config._set("cmip6", "max_parts", 0)
    record = CMIP6DatasetRecord(stac_item_complex)

    assert record.has_parts == []
    assert not [record for record in caplog.records if record.levelname == "WARNING"]


# -----------------------------
# Exclude keys and replica published_on tests
# -----------------------------


def test_replica_nodes_published_on_fallback(stac_item_with_alternates):
    record = CMIP6DatasetRecord(stac_item_with_alternates)
    replicas = {n.host: n.published_on for n in record.replica_nodes}

    expected_replica1 = datetime(2024, 1, 2, 0, 0, tzinfo=UTC)
    expected_replica2 = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

    assert replicas["replica1"] == expected_replica1
    assert replicas["replica2"] == expected_replica2


def test_max_parts_with_exclude_keys_and_warning(caplog, stac_item_with_alternates):
    config._set("cmip6", "max_parts", 1)
    record = CMIP6DatasetRecord(
        stac_item_with_alternates, exclude_keys=["reference_file"]
    )
    parts = record.has_parts
    assert len(parts) == 1
    assert any("included=1, available=2" in rec.message for rec in caplog.records)
