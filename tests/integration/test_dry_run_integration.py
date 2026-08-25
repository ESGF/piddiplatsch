from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from piddiplatsch.config import config
from piddiplatsch.consumer import ConsumerPipeline, DirectConsumer
from piddiplatsch.core.routing import ProjectRouter

pytestmark = pytest.mark.integration


def _make_minimal_cmip6_item() -> dict:
    # Valid-looking CMIP6 dataset id with version
    ds_id = "CMIP6.ActivityX.InstitutionY.SourceZ.ssp245.r1i1p1f1.Amon.pr.gn.v20190101"
    return {
        "id": ds_id,
        "type": "Feature",
        "collection": "cmip6",
        "properties": {},
        # Include one non-excluded asset so HAS_PARTS is not empty
        "assets": {
            "data0000": {
                "alternate:name": "ceda.ac.uk",
                "published_on": "2019-01-01 00:00:00",
            }
        },
    }


def _wrap_message(item: dict) -> tuple[str, dict]:
    key = "test-key"
    value = {
        "data": {
            "payload": {"item": item},
        },
        "metadata": {"time": datetime.now(UTC).isoformat()},
    }
    return key, value


def test_consumer_pipeline_writes_handles_jsonl(tmp_path: Path):
    # Route outputs into a temp directory and avoid any lookup/backend network
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("lookup", "enabled", False)

    item = _make_minimal_cmip6_item()
    msg = _wrap_message(item)

    pipeline = ConsumerPipeline(
        consumer=DirectConsumer([msg]),
        processor=ProjectRouter(["cmip6"], dry_run=True),
        dry_run=True,
        verbose=False,
    )

    pipeline.run()

    handles_dir = tmp_path / "handles"
    assert handles_dir.exists() and handles_dir.is_dir()

    files = list(handles_dir.glob("handles_*.jsonl"))
    assert files, "No handles jsonl file created in dry-run mode"

    # Expect at least one handle line written (dataset; may include assets)
    content_lines = sum(1 for _ in files[0].open("r", encoding="utf-8"))
    assert content_lines >= 1

    # Light check: first line is valid JSON and has basic keys
    with files[0].open("r", encoding="utf-8") as f:
        first_line = f.readline()
    obj = json.loads(first_line)
    assert "handle" in obj and "URL" in obj and "data" in obj
