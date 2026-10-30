import json
from contextlib import closing
from pathlib import Path

import pytest

from piddiplatsch.catalog.harvest import CatalogHarvester
from piddiplatsch.config import config
from piddiplatsch.consumer import HarvestProcessor
from piddiplatsch.core.messages import KafkaSource
from piddiplatsch.core.pipeline import ProcessingPipeline
from piddiplatsch.core.routing import ProjectRouter
from piddiplatsch.exceptions import MaxErrorsExceededError
from piddiplatsch.runners.mapping import map_catalog

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
    pipeline = ProcessingPipeline(
        messages=messages,
        processor=ProjectRouter(["cmip6"], publish=False),
        dump_messages=True,
        publish=False,
    )

    pipeline.run()

    dump_files = list((tmp_path / "dump").glob("dump_messages_*.jsonl"))
    assert len(dump_files) == 1
    dumped = [json.loads(line) for line in dump_files[0].read_text().splitlines()]
    assert dumped == [message for _, message in messages]
    assert list((tmp_path / "cmip6" / "handles").glob("handles_*.jsonl"))
    assert not (tmp_path / "cmip7" / "handles").exists()


def test_harvest_limit_stops_after_dumping_requested_messages(tmp_path: Path):
    config._set("consumer", "output_dir", str(tmp_path))
    messages = [(str(index), {"index": index}) for index in range(3)]
    pipeline = ProcessingPipeline(
        messages=messages,
        processor=HarvestProcessor(),
        dump_messages=True,
        force=True,
        limit=2,
    )

    pipeline.run()

    dump_files = list((tmp_path / "dump").glob("dump_messages_*.jsonl"))
    assert len(dump_files) == 1
    dumped = [json.loads(line) for line in dump_files[0].read_text().splitlines()]
    assert dumped == [{"index": 0}, {"index": 1}]


def test_map_reads_catalog_without_kafka_or_handle_service(
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
    with closing(CatalogHarvester(output_dir / "catalog")) as harvester:
        for offset, record in enumerate(records):
            harvester.process_with_source(
                "harvest", record, KafkaSource("topic", 0, offset)
            )
    result = map_catalog(projects=["cmip6"], limit=1)

    assert result.total == 1
    assert result.succeeded == 1
    assert result.failed == 0
    assert source.read_text(encoding="utf-8") == original
    assert list((output_dir / "cmip6" / "handles").glob("handles_*.jsonl"))


def test_map_force_dataset_pid_corrects_dataset_and_file_parent(
    tmp_path, testdata_path, caplog
):
    from piddiplatsch.utils.models import item_pid, parse_pid

    config._set("consumer", "output_dir", str(tmp_path / "outputs"))
    config._set(
        "plugins", "cmip7", {**config.get_plugin("cmip7"), "force_dataset_pid": True}
    )
    message = _sample(testdata_path, "cmip7")
    item = message["data"]["payload"]["item"]
    source = tmp_path / "raw.jsonl"
    original = json.dumps(message) + "\n"
    source.write_text(original)
    with closing(CatalogHarvester(tmp_path / "outputs/catalog")) as harvester:
        harvester.process_with_source("harvest", message, KafkaSource("topic", 0, 0))
    with caplog.at_level("INFO"):
        result = map_catalog(projects=["cmip7"])

    assert "[map] project=cmip7 force_dataset_pid=True" in caplog.text
    assert "Prepared dataset alias Handle" not in caplog.text

    assert result.succeeded == 1
    assert result.failed == 0
    output_files = list((tmp_path / "outputs/cmip7/handles").glob("*.jsonl"))
    assert len(output_files) == 1
    records = [json.loads(line) for line in output_files[0].read_text().splitlines()]
    dataset, alias, file = records
    generated = f"21.TEST/{item_pid(item['id'])}"
    asset = next(iter(item["assets"].values()))
    file_handle = f"21.TEST/{parse_pid(asset['cmip7:tracking_id'])}"
    assert dataset["handle"] == generated
    assert dataset["URL"].endswith(f"/{generated}")
    assert json.loads(dataset["data"]["HAS_PARTS"]) == [f"hdl:{file_handle}"]
    assert alias["handle"] == f"21.TEST/{parse_pid(item['properties']['cmip7:pid'])}"
    assert alias["URL"] == f"https://hdl.handle.net/{generated}"
    assert alias["data"] == {}
    assert file["handle"] == file_handle
    assert file["data"]["IS_PART_OF"] == f"hdl:{generated}"
    assert source.read_text() == original


def test_map_rejects_invalid_asset_instead_of_skipping_it(
    tmp_path, testdata_path, caplog
):
    config._set("consumer", "max_errors", 1)
    config._set("consumer", "output_dir", str(tmp_path / "outputs"))
    message = _sample(testdata_path, "cmip7")
    item = message["data"]["payload"]["item"]
    asset = next(iter(item["assets"].values()))
    asset["file:checksum"] = "not-hex"
    with closing(CatalogHarvester(tmp_path / "outputs/catalog")) as harvester:
        harvester.process_with_source("bad", message, KafkaSource("topic", 0, 0))

    with pytest.raises(MaxErrorsExceededError, match="1/1"):
        map_catalog(projects=["cmip7"])
    assert "expected a hexadecimal multihash" in caplog.text
    assert "Skipping asset" not in caplog.text
