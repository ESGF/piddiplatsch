from __future__ import annotations

import json
from pathlib import Path

import pytest

from piddiplatsch.config import config
from piddiplatsch.consumer import feed_messages_direct
from piddiplatsch.helpers import read_jsonl

pytestmark = pytest.mark.integration

EXPECTED_RELATIONSHIPS = {
    "c8a64f32-53f9-393b-9fe3-0331dcb7759c": (
        "1b37978f-caf6-4e4a-9893-3266a93077a2"
    ),
    "1f062cde-b12d-335d-a30b-988188098842": (
        "7c4a583c-0bfe-4517-98fa-325084b02684"
    ),
    "b3eaa573-aee5-3f33-b36f-8970df2eba9a": (
        "415fb9b8-f11a-47ae-ab62-a5c5e17c77bf"
    ),
    "bc85369b-44a5-3e57-8d91-251b63c8b9d3": (
        "4485e7f1-06fb-46a5-99b3-2fb951eeb80d"
    ),
}


def _load_publications(sample_dir: Path) -> list[tuple[str, dict]]:
    messages = []
    for path in sorted(sample_dir.glob("*.json")):
        with path.open(encoding="utf-8") as stream:
            messages.append((path.stem, json.load(stream)))
    return messages


def test_real_publications_route_and_write_valid_handle_jsonl(
    tmp_path: Path, testdata_path: Path
):
    """Exercise decode-to-JSONL integration without Kafka or Handle services."""
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("lookup", "enabled", False)
    messages = _load_publications(testdata_path / "publication_samples")

    result = feed_messages_direct(messages, projects="all", dry_run=True)

    assert result.total == 4
    assert result.succeeded == 4
    assert result.failed == 0
    assert result.filtered == 0

    output_files = list(tmp_path.glob("*/handles/handles_*.jsonl"))
    assert {path.parent.parent.name for path in output_files} == {
        "cmip6",
        "cmip6plus",
        "cmip7",
        "cordex-cmip6",
    }
    records = [record for path in output_files for record in read_jsonl(path)]
    assert len(records) == 8
    for path in output_files:
        project = path.parent.parent.name
        assert all(record["project"] == project for record in read_jsonl(path))

    by_handle = {record["handle"]: record for record in records}
    expected_handles = {
        f"21.TEST/{pid}"
        for relationship in EXPECTED_RELATIONSHIPS.items()
        for pid in relationship
    }
    assert set(by_handle) == expected_handles

    for dataset_pid, file_pid in EXPECTED_RELATIONSHIPS.items():
        dataset_handle = f"21.TEST/{dataset_pid}"
        file_handle = f"21.TEST/{file_pid}"
        dataset = by_handle[dataset_handle]
        file = by_handle[file_handle]

        assert dataset["URL"].endswith(f"/{dataset_handle}")
        assert dataset["data"]["AGGREGATION_LEVEL"] == "DATASET"
        assert json.loads(dataset["data"]["HAS_PARTS"]) == [f"hdl:{file_handle}"]
        assert json.loads(dataset["data"]["HOSTING_NODE"])["host"] != "unknown"

        assert file["URL"].endswith(f"/{file_handle}")
        assert file["data"]["AGGREGATION_LEVEL"] == "FILE"
        assert file["data"]["IS_PART_OF"] == f"hdl:{dataset_handle}"
        assert file["data"]["CHECKSUM_METHOD"] == "sha2-256"
        assert int(file["data"]["FILE_SIZE"]) > 0
