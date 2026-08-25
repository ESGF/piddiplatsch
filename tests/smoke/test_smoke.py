import json
import time
from pathlib import Path

import pytest

from piddiplatsch.testing.kafka_client import send_message_to_kafka

pytestmark = pytest.mark.smoke


PUBLICATION_CASES = [
    pytest.param(
        "cmip6.json",
        "c8a64f32-53f9-393b-9fe3-0331dcb7759c",
        "1b37978f-caf6-4e4a-9893-3266a93077a2",
        id="cmip6",
    ),
    pytest.param(
        "cmip6plus.json",
        "bc85369b-44a5-3e57-8d91-251b63c8b9d3",
        "4485e7f1-06fb-46a5-99b3-2fb951eeb80d",
        id="cmip6plus",
    ),
    pytest.param(
        "cmip7.json",
        "1f062cde-b12d-335d-a30b-988188098842",
        "7c4a583c-0bfe-4517-98fa-325084b02684",
        id="cmip7",
    ),
    pytest.param(
        "cordex-cmip6.json",
        "b3eaa573-aee5-3f33-b36f-8970df2eba9a",
        "415fb9b8-f11a-47ae-ab62-a5c5e17c77bf",
        id="cordex-cmip6",
    ),
]


def wait_for_pid(handle_client, pid: str, timeout: float = 15.0):
    """Wait until a PID is available in the Handle service or time out."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        record = handle_client.get(pid)
        if record:
            return record
        time.sleep(0.2)
    raise AssertionError(f"PID {pid} was not registered within {timeout:.1f} seconds")


def assert_dataset_record(record: dict, dataset_pid: str, file_pid: str):
    assert record["URL"].endswith(f"/21.TEST/{dataset_pid}")
    assert record["AGGREGATION_LEVEL"] == "DATASET"
    assert record["DATASET_ID"]
    assert record["DATASET_VERSION"]
    assert json.loads(record["HAS_PARTS"]) == [f"hdl:21.TEST/{file_pid}"]
    assert json.loads(record["HOSTING_NODE"])["host"] != "unknown"


def assert_file_record(record: dict, dataset_pid: str, file_pid: str):
    assert record["URL"].endswith(f"/21.TEST/{file_pid}")
    assert record["AGGREGATION_LEVEL"] == "FILE"
    assert record["FILE_NAME"]
    assert record["IS_PART_OF"] == f"hdl:21.TEST/{dataset_pid}"
    assert record["DOWNLOAD_URL"].startswith("https://")
    assert record["CHECKSUM"]
    assert record["CHECKSUM_METHOD"] == "sha2-256"
    assert int(record["FILE_SIZE"]) > 0


@pytest.mark.parametrize("filename,dataset_pid,file_pid", PUBLICATION_CASES)
def test_real_publication_for_each_project(
    testfile, handle_client, filename: str, dataset_pid: str, file_pid: str
):
    publication: Path = testfile("publication_samples", filename)

    send_message_to_kafka(publication)

    dataset = wait_for_pid(handle_client, dataset_pid)
    file_record = wait_for_pid(handle_client, file_pid)
    assert_dataset_record(dataset, dataset_pid, file_pid)
    assert_file_record(file_record, dataset_pid, file_pid)
