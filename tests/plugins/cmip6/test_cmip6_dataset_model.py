import datetime
import uuid

import pytest
from pydantic import ValidationError

from piddiplatsch.config import config
from piddiplatsch.core.models import HostingNode
from piddiplatsch.plugins.cmip6.model import CMIP6DatasetModel

pytestmark = [pytest.mark.plugin, pytest.mark.cmip6]

# --- Helper constants ---
HOST_NODE = HostingNode(
    host="esgf-node", published_on=datetime.datetime.now(datetime.UTC)
)


# --- CMIP6DatasetModel tests ---
def make_dataset(
    has_parts=None,
    dataset_id="dataset-001",
    previous_version=None,
    is_part_of=None,
):
    return CMIP6DatasetModel(
        URL="https://example.com",
        DATASET_ID=dataset_id,
        HOSTING_NODE=HOST_NODE,
        HAS_PARTS=has_parts,
        PREVIOUS_VERSION=previous_version,
        IS_PART_OF=is_part_of,
    )


def test_dataset_model_basic():
    dataset = make_dataset(has_parts=["file1.nc"])
    assert dataset.AGGREGATION_LEVEL == "DATASET"
    assert dataset.HOSTING_NODE.host == "esgf-node"


def test_dataset_model_pid():
    dataset = make_dataset(has_parts=["file1.nc"])

    # PID can be UUID string
    uid = str(uuid.uuid4())
    dataset.set_pid(uid)
    assert dataset.get_pid() == uid

    # PID can be UUID object
    uid_obj = uuid.uuid4()
    dataset.set_pid(uid_obj)
    assert dataset.get_pid() == str(uid_obj)

    # Invalid PID raises
    with pytest.raises(ValueError):
        dataset.set_pid("not-a-uuid")


def test_dataset_model_requires_hosting_node():
    with pytest.raises(ValidationError):
        CMIP6DatasetModel(
            URL="https://example.com",
            DATASET_ID="dataset-001",
            HAS_PARTS=["file1.nc"],
        )


# --- max_parts and strict_mode tests ---
def test_max_parts_enforced():
    config._set("plugins", "cmip6", {**config.get_plugin("cmip6"), "max_parts": 2})
    config._set("schema", "strict_mode", True)

    dataset = make_dataset(has_parts=["file1.nc"])
    assert len(dataset.HAS_PARTS) == 1

    dataset = make_dataset(has_parts=["file1.nc", "file2.nc"])
    assert len(dataset.HAS_PARTS) == 2

    with pytest.raises(ValueError, match="Too many parts: 3 exceeds max_parts=2"):
        make_dataset(has_parts=["file1.nc", "file2.nc", "file3.nc"])


def test_strict_mode_requires_parts():
    config._set("plugins", "cmip6", {**config.get_plugin("cmip6"), "max_parts": 5})
    config._set("schema", "strict_mode", True)

    with pytest.raises(ValueError):
        make_dataset(has_parts=[])


def test_lenient_mode_allows_empty_parts():
    config._set("plugins", "cmip6", {**config.get_plugin("cmip6"), "max_parts": 5})
    config._set("schema", "strict_mode", False)

    dataset = make_dataset(has_parts=[])
    assert dataset.HAS_PARTS == []


# --- Dataset versioning and IS_PART_OF ---
def test_previous_version_set():
    dataset_v1 = make_dataset(dataset_id="dataset-001", has_parts=["file1.nc"])
    dataset_v2 = make_dataset(
        dataset_id="dataset-002", previous_version="dataset-001", has_parts=["file1.nc"]
    )

    assert dataset_v1.DATASET_ID == "dataset-001"
    assert dataset_v2.PREVIOUS_VERSION == "dataset-001"
    assert dataset_v2.IS_PART_OF is None


def test_is_part_of_set():
    dataset = make_dataset(is_part_of="collection-001", has_parts=["file1.nc"])
    assert dataset.IS_PART_OF == "collection-001"


def test_previous_version_with_is_part_of():
    dataset_v2 = make_dataset(
        dataset_id="dataset-002",
        previous_version="dataset-001",
        is_part_of="collection-001",
        has_parts=["file1.nc"],
    )
    assert dataset_v2.PREVIOUS_VERSION == "dataset-001"
    assert dataset_v2.IS_PART_OF == "collection-001"


def test_optional_fields_defaults():
    dataset = make_dataset(has_parts=["file1.nc"])

    assert dataset.DATASET_VERSION is None
    assert dataset.PREVIOUS_VERSION is None
    assert dataset.IS_PART_OF is None
    assert dataset.REPLICA_NODES == []
    assert dataset._RETRACTED is False
