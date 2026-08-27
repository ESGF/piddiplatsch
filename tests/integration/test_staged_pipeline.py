import json
from pathlib import Path

import pytest

from piddiplatsch.config import config
from piddiplatsch.consumer import (
    ConsumerPipeline,
    DirectConsumer,
    map_dump_files,
)
from piddiplatsch.core.routing import ProjectRouter

pytestmark = pytest.mark.integration


def _sample(testdata_path: Path, project: str) -> dict:
    path = testdata_path / "publication_samples" / f"{project}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def test_consume_dumps_every_raw_message_before_project_filtering(
    tmp_path: Path, testdata_path: Path
):
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("lookup", "enabled", False)
    messages = [
        ("cmip6", _sample(testdata_path, "cmip6")),
        ("cmip7", _sample(testdata_path, "cmip7")),
    ]
    pipeline = ConsumerPipeline(
        consumer=DirectConsumer(messages),
        processor=ProjectRouter(["cmip6"], dry_run=True),
        dump_messages=True,
        dry_run=True,
    )

    pipeline.run()

    dump_files = list((tmp_path / "dump").glob("dump_messages_*.jsonl"))
    assert len(dump_files) == 1
    dumped = [json.loads(line) for line in dump_files[0].read_text().splitlines()]
    assert dumped == [message for _, message in messages]
    assert list((tmp_path / "cmip6" / "handles").glob("handles_*.jsonl"))
    assert not (tmp_path / "cmip7" / "handles").exists()


def test_map_replays_dump_without_kafka_or_handle_service(
    tmp_path: Path, testdata_path: Path
):
    output_dir = tmp_path / "outputs"
    config._set("consumer", "output_dir", str(output_dir))
    config._set("lookup", "enabled", False)
    source = tmp_path / "raw.jsonl"
    records = [
        _sample(testdata_path, "cmip7"),
        _sample(testdata_path, "cmip6"),
    ]
    original = "".join(json.dumps(record) + "\n" for record in records)
    source.write_text(original, encoding="utf-8")

    result = map_dump_files([source], projects=["cmip6"], offset=1, limit=1)

    assert result.total == 1
    assert result.succeeded == 1
    assert result.failed == 0
    assert source.read_text(encoding="utf-8") == original
    assert list((output_dir / "cmip6" / "handles").glob("handles_*.jsonl"))
