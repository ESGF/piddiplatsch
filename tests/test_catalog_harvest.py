"""Harvest builds complete project catalogs before acknowledging Kafka records."""

import json
from contextlib import closing
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner
from confluent_kafka import KafkaError, KafkaException

from piddiplatsch.catalog import CatalogError
from piddiplatsch.catalog.harvest import CatalogHarvester
from piddiplatsch.catalog.sqlite import SQLiteCatalog
from piddiplatsch.cli import cli
from piddiplatsch.config import config
from piddiplatsch.consumer import HarvestProcessor
from piddiplatsch.core.messages import KafkaSource, SourceMessage
from piddiplatsch.core.pipeline import ProcessingPipeline


def publication(project="CMIP6", version=1):
    return {
        "data": {
            "type": "STAC",
            "payload": {
                "method": "POST",
                "collection_id": project,
                "item": {
                    "id": f"{project}.dataset.v{version}",
                    "collection": project,
                    "type": "Feature",
                    "properties": {"project": project, "updates": []},
                    "assets": {},
                    "links": [],
                },
            },
        },
    }


def patch_message(operations, *, project="CMIP6", version=1):
    return {
        "data": {
            "payload": {
                "method": "PATCH",
                "collection_id": project,
                "item_id": f"{project}.dataset.v{version}",
                "patch": operations,
            }
        }
    }


@pytest.mark.parametrize("project", ["cmip6", "cmip6plus", "cmip7", "cordex-cmip6"])
def test_real_publications_are_cached_without_handle_validation(
    tmp_path, publication_sample, project
):
    message = publication_sample(project)
    payload = message["data"]["payload"]
    with closing(CatalogHarvester(tmp_path / "catalog")) as harvester:
        result = harvester.process_with_source(
            "key", message, KafkaSource("topic", 0, 0)
        )
        assert result.success
        assert result.num_handles == 0
    assert [path.name for path in (tmp_path / "catalog").glob("piddi.db")] == [
        "piddi.db"
    ]
    with SQLiteCatalog(project, tmp_path / "catalog" / "piddi.db") as catalog:
        assert (
            catalog.get_item(payload["collection_id"], payload["item"]["id"]).item
            == payload["item"]
        )
        assert catalog.get_checkpoint("topic", 0).next_offset == 1
    assert not list(tmp_path.rglob("handles_*.jsonl"))


@pytest.mark.parametrize("wrapped", [False, True])
def test_patch_replay_is_idempotent_and_new_updates_replace_existing_item(
    tmp_path, wrapped
):
    directory = tmp_path / "catalog"
    operations = [{"op": "add", "path": "/properties/updates/-", "value": "one"}]
    patch = patch_message({"operations": operations} if wrapped else operations)
    with closing(CatalogHarvester(directory)) as harvester:
        harvester.process_with_source("post", publication(), KafkaSource("topic", 0, 0))
        assert harvester.process_with_source(
            "patch", patch, KafkaSource("topic", 0, 1)
        ).patched
    with closing(CatalogHarvester(directory)) as restarted:
        assert restarted.process_with_source(
            "patch", patch, KafkaSource("topic", 0, 1)
        ).filtered
        assert restarted.process_with_source(
            "post", publication(), KafkaSource("topic", 0, 0)
        ).filtered
        restarted.process_with_source("patch", patch, KafkaSource("topic", 0, 2))
        restarted.process_with_source(
            "new-version", publication(version=2), KafkaSource("topic", 0, 3)
        )
    with SQLiteCatalog("cmip6", directory / "piddi.db") as catalog:
        assert catalog.get_item("CMIP6", "CMIP6.dataset.v1").item["properties"][
            "updates"
        ] == ["one", "one"]
        assert catalog.get_checkpoint("topic", 0).next_offset == 4
        assert [work.event_type for work in catalog.iter_pending(10)] == [
            "full",
            "patch",
            "patch",
            "full",
        ]
        assert (
            catalog.find_previous_version("CMIP6", "CMIP6.dataset", 2)
            == "CMIP6.dataset.v1"
        )


@pytest.mark.parametrize("trigger", ["count", "time", "shutdown"])
def test_patch_resolution_summary_timing_and_flush(
    tmp_path, monkeypatch, caplog, trigger
):
    config._set("stats", "interval_seconds", 30)
    config._set("stats", "summary_interval", 2 if trigger == "count" else 10000)
    clock = [0.0]
    monkeypatch.setattr("piddiplatsch.catalog.harvest.perf_counter", lambda: clock[0])
    resolve = CatalogHarvester._resolve_patch

    def timed_resolution(self, *args):
        clock[0] += 0.25
        return resolve(self, *args)

    monkeypatch.setattr(CatalogHarvester, "_resolve_patch", timed_resolution)
    caplog.set_level("INFO")
    with closing(CatalogHarvester(tmp_path)) as harvester:
        harvester.process_with_source("post", publication(), KafkaSource("topic", 0, 0))
        for offset in (1, 2):
            if trigger == "time" and offset == 2:
                clock[0] += 31
            harvester.process_with_source(
                "patch", patch_message([]), KafkaSource("topic", 0, offset)
            )
            assert harvester.process_with_source(
                "duplicate", patch_message([]), KafkaSource("topic", 0, offset)
            ).filtered
        summaries = [line for line in caplog.messages if "patch resolutions:" in line]
        assert len(summaries) == (0 if trigger == "shutdown" else 1)
    # Explicit and context-manager closes must not repeat the summary.
    harvester.close()
    summaries = [line for line in caplog.messages if "patch resolutions:" in line]
    assert len(summaries) == 1
    assert (
        "[harvest] cmip6 | patch resolutions: 2 | local: 2 | remote STAC: 0 | errors: 0"
        in summaries[0]
    )
    assert "0.500s total | 250.000ms avg" in summaries[0]


def test_patch_summaries_keep_projects_separate_and_reset_after_logging(
    tmp_path, caplog
):
    config._set("stats", "summary_interval", 2)
    config._set("stats", "interval_seconds", 3600)
    with caplog.at_level("INFO"), closing(CatalogHarvester(tmp_path)) as harvester:
        for offset, project in enumerate(("CMIP6", "CMIP7")):
            harvester.process_with_source(
                "post", publication(project), KafkaSource("topic", 0, offset)
            )
        for offset, project in enumerate(("CMIP6", "CMIP7", "CMIP6"), start=2):
            harvester.process_with_source(
                "patch",
                patch_message([], project=project),
                KafkaSource("topic", 0, offset),
            )
    summaries = [line for line in caplog.messages if "patch resolutions:" in line]
    assert len(summaries) == 3
    for line, project in zip(summaries, ("cmip6", "cmip7", "cmip6"), strict=True):
        assert (
            f"[harvest] {project} | patch resolutions: 1 | local: 1 | remote STAC: 0 | errors: 0"
            in line
        )


def test_post_only_harvest_has_no_patch_summary(tmp_path, caplog):
    with caplog.at_level("INFO"), closing(CatalogHarvester(tmp_path)) as harvester:
        harvester.process_with_source("post", publication(), KafkaSource("topic", 0, 0))
    assert not any("patch resolutions:" in line for line in caplog.messages)


def test_missing_base_stops_before_acknowledgement_and_records_source(tmp_path, caplog):
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("stac", "base_url", "")
    patch = patch_message([])
    acknowledge = MagicMock()
    following_ack = MagicMock()
    caplog.set_level("INFO")
    with closing(HarvestProcessor(catalog_dir=tmp_path / "catalog")) as processor:
        pipeline = ProcessingPipeline(
            [
                SourceMessage("patch", patch, KafkaSource("topic", 2, 10), acknowledge),
                SourceMessage(
                    "post", publication(), KafkaSource("topic", 2, 11), following_ack
                ),
            ],
            processor,
            dump_messages=True,
            max_errors=-1,
        )
        try:
            with pytest.raises(CatalogError, match="Missing PATCH base"):
                pipeline.run()
        finally:
            pipeline.close_progress()
    acknowledge.assert_not_called()
    following_ack.assert_not_called()
    summaries = [line for line in caplog.messages if "patch resolutions:" in line]
    assert len(summaries) == 1
    assert (
        "patch resolutions: 1 | local: 0 | remote STAC: 0 | errors: 1" in summaries[0]
    )
    with SQLiteCatalog("cmip6", tmp_path / "catalog/piddi.db") as catalog:
        assert catalog.get_checkpoint("topic", 2) is None
        assert list(catalog.iter_items()) == []
    failure = json.loads(
        next((tmp_path / "cmip6/failures/r0").glob("*.jsonl")).read_text()
    )
    assert failure["__infos__"]["kafka"] == {
        "topic": "topic",
        "partition": 2,
        "offset": 10,
    }
    dumped = json.loads(next((tmp_path / "dump").glob("*.jsonl")).read_text())
    assert dumped["payload"] == patch
    assert dumped["key"] == "patch"
    assert dumped["source"] == {"topic": "topic", "partition": 2, "offset": 10}


@pytest.mark.parametrize(
    "operations",
    [
        [{"op": "replace", "path": "/id", "value": "CMIP6.other.v1"}],
        [{"op": "replace", "path": "/collection", "value": "CMIP7"}],
        [{"op": "replace", "path": "/properties/project", "value": "CMIP7"}],
        [{"op": "replace", "path": "/assets", "value": []}],
        [{"op": "remove", "path": "/not-present"}],
    ],
)
def test_invalid_patch_preserves_item_and_checkpoint(tmp_path, operations):
    with closing(CatalogHarvester(tmp_path)) as harvester:
        harvester.process_with_source("post", publication(), KafkaSource("topic", 0, 0))
        with pytest.raises((ValueError, CatalogError)):
            harvester.process_with_source(
                "patch", patch_message(operations), KafkaSource("topic", 0, 1)
            )
    with SQLiteCatalog("cmip6", tmp_path / "piddi.db") as catalog:
        assert (
            catalog.get_item("CMIP6", "CMIP6.dataset.v1").item
            == publication()["data"]["payload"]["item"]
        )
        assert catalog.get_checkpoint("topic", 0).next_offset == 1


def test_acknowledgement_observes_durable_catalog_state(tmp_path):
    def acknowledge():
        with SQLiteCatalog("cmip6", tmp_path / "piddi.db") as catalog:
            assert catalog.get_item("CMIP6", "CMIP6.dataset.v1") is not None
            assert catalog.get_checkpoint("topic", 0).next_offset == 1

    with closing(HarvestProcessor(catalog_dir=tmp_path)) as processor:
        pipeline = ProcessingPipeline(
            [
                SourceMessage(
                    "post", publication(), KafkaSource("topic", 0, 0), acknowledge
                )
            ],
            processor,
        )
        try:
            assert pipeline.run().succeeded == 1
        finally:
            pipeline.close_progress()


def kafka_message(value, offset=0):
    message = MagicMock()
    message.error.return_value = None
    message.key.return_value = b"key"
    message.value.return_value = json.dumps(value).encode()
    message.topic.return_value = "topic"
    message.partition.return_value = 0
    message.offset.return_value = offset
    return message


def test_harvest_cli_creates_project_databases_and_commits_after_store(
    tmp_path, monkeypatch
):
    config._set("consumer", "output_dir", str(tmp_path / "outputs"))
    consumer_class = MagicMock()
    client = consumer_class.return_value
    messages = [kafka_message(publication(), 0), kafka_message(publication("CMIP7"), 1)]
    client.poll.side_effect = messages
    client.commit.return_value = []
    monkeypatch.setattr("confluent_kafka.Consumer", consumer_class)
    monkeypatch.setattr("piddiplatsch.consumer.signal.signal", MagicMock())
    result = CliRunner().invoke(cli, ["--no-progress", "harvest", "--limit", "2"])
    assert result.exit_code == 0, result.output
    settings = consumer_class.call_args.args[0]
    assert settings["group.id"] == config.get("kafka", "group.id") + ".harvest"
    assert settings["enable.auto.commit"] is False
    assert settings["enable.auto.offset.store"] is False
    assert settings["auto.offset.reset"] == "error"
    assert client.commit.call_count == 1
    assert client.commit.call_args.kwargs["asynchronous"] is False
    assert [
        (p.topic, p.partition, p.offset)
        for p in client.commit.call_args.kwargs["offsets"]
    ] == [("topic", 0, 2)]
    client.close.assert_called_once()
    assert sorted(path.name for path in (tmp_path / "outputs").glob("piddi.db")) == [
        "piddi.db"
    ]
    assert not list((tmp_path / "outputs").rglob("handles_*.jsonl"))


def test_raw_only_harvest_retains_previous_behavior(tmp_path, monkeypatch):
    consumer_class = MagicMock()
    client = consumer_class.return_value
    client.poll.return_value = kafka_message({"arbitrary": "raw record"})
    monkeypatch.setattr("confluent_kafka.Consumer", consumer_class)
    monkeypatch.setattr("piddiplatsch.consumer.signal.signal", MagicMock())
    result = CliRunner().invoke(
        cli, ["--no-progress", "harvest", "--no-catalog", "--limit", "1"]
    )
    assert result.exit_code == 0, result.output
    assert consumer_class.call_args.args[0]["group.id"] == config.get(
        "kafka", "group.id"
    )
    client.commit.assert_not_called()
    assert not (tmp_path / "outputs/piddi.db").exists()


def test_kafka_commit_failure_can_be_replayed_without_reapplying_patch(tmp_path):
    def fail_commit():
        raise RuntimeError("Kafka commit failed")

    with closing(HarvestProcessor(catalog_dir=tmp_path)) as processor:
        processor.process_with_source("post", publication(), KafkaSource("topic", 0, 0))
        patch = patch_message(
            [{"op": "add", "path": "/properties/updates/-", "value": "one"}]
        )
        pipeline = ProcessingPipeline(
            [SourceMessage("patch", patch, KafkaSource("topic", 0, 1), fail_commit)],
            processor,
        )
        try:
            with pytest.raises(RuntimeError, match="commit failed"):
                pipeline.run()
        finally:
            pipeline.close_progress()
    with closing(CatalogHarvester(tmp_path)) as restarted:
        assert restarted.process_with_source(
            "patch", patch, KafkaSource("topic", 0, 1)
        ).filtered
    with SQLiteCatalog("cmip6", tmp_path / "piddi.db") as catalog:
        assert catalog.get_item("CMIP6", "CMIP6.dataset.v1").item["properties"][
            "updates"
        ] == ["one"]


@pytest.mark.parametrize(
    "value, exit_code",
    [
        (patch_message([]), 1),
        (publication("UNKNOWN"), 65),
        ([], 65),
        ({"data": []}, 65),
    ],
)
def test_cli_failed_event_does_not_commit_or_consume_following_records(
    value, exit_code, monkeypatch
):
    config._set("stac", "base_url", "")
    consumer_class = MagicMock()
    client = consumer_class.return_value
    client.poll.side_effect = [kafka_message(value), kafka_message(publication(), 1)]
    monkeypatch.setattr("confluent_kafka.Consumer", consumer_class)
    monkeypatch.setattr("piddiplatsch.consumer.signal.signal", MagicMock())
    result = CliRunner().invoke(cli, ["--no-progress", "harvest", "--limit", "2"])
    assert result.exit_code == exit_code
    client.commit.assert_not_called()
    client.poll.assert_called_once()
    client.close.assert_called_once()


def test_cli_malformed_json_is_fatal_without_acknowledgement(monkeypatch):
    consumer_class = MagicMock()
    client = consumer_class.return_value
    message = kafka_message({})
    message.value.return_value = b"not JSON"
    client.poll.return_value = message
    monkeypatch.setattr("confluent_kafka.Consumer", consumer_class)
    monkeypatch.setattr("piddiplatsch.consumer.signal.signal", MagicMock())
    result = CliRunner().invoke(cli, ["--no-progress", "harvest", "--limit", "1"])
    assert result.exit_code == 65
    assert "topic/0/0" in result.output
    client.commit.assert_not_called()
    client.close.assert_called_once()


def test_cli_invalid_patch_blocks_service_restart_and_preserves_failed_offset(
    tmp_path, monkeypatch
):
    config._set("consumer", "output_dir", str(tmp_path / "outputs"))
    config._set("catalog", "directory", str(tmp_path / "catalog"))
    config._set("stats", "enable_db", True)
    patch = patch_message(
        [
            {
                "op": "add",
                "path": "/properties/alternate/replica",
                "value": {"href": "https://replica.example"},
            }
        ]
    )
    consumer_class = MagicMock()
    client = consumer_class.return_value
    client.poll.side_effect = [
        kafka_message(publication(), 0),
        kafka_message(patch, 1),
        kafka_message(publication(version=2), 2),
    ]
    remote = MagicMock(
        side_effect=AssertionError("Invalid patches must not fetch remote state")
    )
    monkeypatch.setattr("piddiplatsch.utils.stac.RemoteStacClient.get_item", remote)
    monkeypatch.setattr("confluent_kafka.Consumer", consumer_class)
    monkeypatch.setattr("piddiplatsch.consumer.signal.signal", MagicMock())
    result = CliRunner().invoke(cli, ["--no-progress", "harvest", "--limit", "3"])
    assert result.exit_code == 65
    assert "Harvest blocked by invalid input" in result.output
    assert "alternate" in result.output
    assert client.poll.call_count == 2
    client.close.assert_called_once()
    remote.assert_not_called()
    with SQLiteCatalog(
        "cmip6", tmp_path / "catalog/piddi.db", read_only=True
    ) as catalog:
        assert catalog.get_checkpoint("topic", 0).next_offset == 1
        assert (
            catalog.get_item("CMIP6", "CMIP6.dataset.v1").item
            == publication()["data"]["payload"]["item"]
        )
        assert len(list(catalog.iter_pending(10))) == 1
    dumped = [
        json.loads(line)
        for path in (tmp_path / "outputs/dump").glob("*.jsonl")
        for line in path.read_text().splitlines()
    ]
    assert [record["source"]["offset"] for record in dumped] == [0, 1]
    failures = list((tmp_path / "outputs/cmip6/failures/r0").glob("*.jsonl"))
    assert len(failures) == 1
    assert len(failures[0].read_text().splitlines()) == 1
    assert json.loads(failures[0].read_text())["__infos__"]["kafka"]["offset"] == 1
    top = CliRunner().invoke(cli, ["top", "--once"])
    assert top.exit_code == 0, top.output
    assert "Harvest: failed" in top.output
    assert "Could not apply PATCH" in top.output
    assert "cmip6: errors recorded" in top.output


def test_cli_remote_failure_keeps_retryable_exit_status(monkeypatch):
    config._set("stac", "base_url", "https://stac.example")
    consumer_class = MagicMock()
    client = consumer_class.return_value
    client.poll.return_value = kafka_message(patch_message([]))
    remote = MagicMock(side_effect=OSError("STAC service unavailable"))
    monkeypatch.setattr("piddiplatsch.utils.stac.RemoteStacClient.get_item", remote)
    monkeypatch.setattr("confluent_kafka.Consumer", consumer_class)
    monkeypatch.setattr("piddiplatsch.consumer.signal.signal", MagicMock())
    result = CliRunner().invoke(cli, ["--no-progress", "harvest", "--limit", "1"])
    assert result.exit_code == 1
    assert "Remote refresh failed" in result.output
    remote.assert_called_once()
    client.commit.assert_not_called()
    client.close.assert_called_once()


def test_partition_commit_error_is_not_treated_as_success(monkeypatch):
    from piddiplatsch.consumer import KafkaConsumer

    consumer_class = MagicMock()
    client = consumer_class.return_value
    message = kafka_message(publication())
    client.poll.return_value = message
    failed_partition = MagicMock()
    failed_partition.error = KafkaError(KafkaError._TIMED_OUT)
    client.commit.return_value = [failed_partition]
    monkeypatch.setattr("confluent_kafka.Consumer", consumer_class)
    consumer = KafkaConsumer("topic", {}, with_source=True, commit_batch_size=1)
    with closing(consumer.consume()) as messages:
        value = next(messages)
        with pytest.raises(KafkaException):
            value.acknowledge()
    client.close.assert_called_once()


def test_configured_catalog_directory_and_disable_setting(tmp_path, monkeypatch):
    directory = tmp_path / "custom-catalog"
    config._set("catalog", "directory", str(directory))
    consumer_class = MagicMock()
    client = consumer_class.return_value
    client.poll.return_value = kafka_message(publication())
    client.commit.return_value = []
    monkeypatch.setattr("confluent_kafka.Consumer", consumer_class)
    monkeypatch.setattr("piddiplatsch.consumer.signal.signal", MagicMock())
    result = CliRunner().invoke(cli, ["--no-progress", "harvest", "--limit", "1"])
    assert result.exit_code == 0, result.output
    assert (directory / "piddi.db").exists()
    config._set("catalog", "enabled", False)
    client.poll.return_value = kafka_message(publication("CMIP7"))
    result = CliRunner().invoke(cli, ["--no-progress", "harvest", "--limit", "1"])
    assert result.exit_code == 0, result.output
    with SQLiteCatalog("cmip7", directory / "piddi.db", read_only=True) as catalog:
        assert list(catalog.iter_items()) == []


def test_remote_recovery_never_reapplies_patch_and_survives_restart(
    tmp_path, monkeypatch, caplog
):
    from copy import deepcopy

    from piddiplatsch.catalog.remote import fetch_current_item

    caplog.set_level("INFO")
    config._set("stac", "base_url", "https://stac.example")
    current = publication()["data"]["payload"]["item"]
    current["properties"]["updates"] = ["already-applied"]
    calls = []

    def get_item(client, collection_id, item_id):
        calls.append((collection_id, item_id))
        return deepcopy(current)

    monkeypatch.setattr("piddiplatsch.utils.stac.RemoteStacClient.get_item", get_item)
    patch = patch_message(
        [{"op": "add", "path": "/properties/updates/-", "value": "already-applied"}]
    )
    with closing(CatalogHarvester(tmp_path)) as harvester:
        harvester.process_with_source("patch", patch, KafkaSource("topic", 0, 5))
        # Duplicate offsets do not fetch again or apply a patch.
        assert harvester.process_with_source(
            "patch", patch, KafkaSource("topic", 0, 5)
        ).filtered
    with SQLiteCatalog("cmip6", tmp_path / "piddi.db", read_only=True) as catalog:
        entry = catalog.get_item("CMIP6", "CMIP6.dataset.v1")
        assert entry.item == current
        assert entry.remote_refresh["trigger"] == {
            "topic": "topic",
            "partition": 0,
            "offset": 5,
        }
        assert entry.remote_refresh["endpoint"] == "https://stac.example"
        assert catalog.get_work(1).event_type == "patch"
        assert entry.remote_refresh["fetched_at"]
        assert list(catalog.iter_items(remote_refresh_only=True)) == [entry]
        assert catalog.get_checkpoint("topic", 0).next_offset == 6
    current["properties"]["updates"].append("newer")
    with closing(CatalogHarvester(tmp_path)) as harvester:
        # Startup catches up marked items without needing another Kafka event.
        assert (
            harvester._get_catalog("cmip6").get_item("CMIP6", "CMIP6.dataset.v1").item
            == current
        )
        harvester.process_with_source("patch", patch, KafkaSource("topic", 0, 6))
        # An older full publication cannot regress the recovered item or clear mode.
        harvester.process_with_source("post", publication(), KafkaSource("topic", 0, 7))
    assert len(calls) == 4
    summaries = [line for line in caplog.messages if "patch resolutions:" in line]
    assert len(summaries) == 2
    assert all(
        "patch resolutions: 1 | local: 0 | remote STAC: 1 | errors: 0" in line
        for line in summaries
    )
    with SQLiteCatalog("cmip6", tmp_path / "piddi.db", read_only=True) as catalog:
        entry = catalog.get_item("CMIP6", "CMIP6.dataset.v1")
        assert entry.item == current
        assert entry.remote_refresh["trigger"]["offset"] == 5
        assert catalog.get_checkpoint("topic", 0).next_offset == 8
    # No placeholder is ever returned without a configured remote service.
    config._set("stac", "base_url", "")
    with pytest.raises(CatalogError, match="base_url"):
        fetch_current_item("cmip6", "CMIP6", "CMIP6.dataset.v1")


@pytest.mark.parametrize(
    "fault",
    ["unavailable", "wrong-id", "wrong-collection", "wrong-project", "invalid-assets"],
)
def test_remote_recovery_failures_are_atomic(tmp_path, monkeypatch, fault):
    import requests

    config._set("stac", "base_url", "https://stac.example")
    item = publication()["data"]["payload"]["item"]
    if fault == "wrong-id":
        item["id"] = "CMIP6.other.v1"
    if fault == "wrong-collection":
        item["collection"] = "CMIP7"
    if fault == "wrong-project":
        item["properties"]["project"] = "CMIP7"
    if fault == "invalid-assets":
        item["assets"] = []

    def get_item(*args):
        if fault == "unavailable":
            raise requests.Timeout("service unavailable")
        return item

    monkeypatch.setattr("piddiplatsch.utils.stac.RemoteStacClient.get_item", get_item)
    with closing(CatalogHarvester(tmp_path)) as harvester:
        with pytest.raises((CatalogError, ValueError)):
            harvester.process_with_source(
                "patch", patch_message([]), KafkaSource("topic", 0, 0)
            )
    with SQLiteCatalog("cmip6", tmp_path / "piddi.db") as catalog:
        assert catalog.get_item("CMIP6", "CMIP6.dataset.v1") is None
        assert catalog.get_checkpoint("topic", 0) is None


def test_failed_refresh_retains_previous_item_and_checkpoint(tmp_path, monkeypatch):
    config._set("stac", "base_url", "https://stac.example")
    item = publication()["data"]["payload"]["item"]
    client = MagicMock(return_value=item)
    monkeypatch.setattr("piddiplatsch.utils.stac.RemoteStacClient.get_item", client)
    with closing(CatalogHarvester(tmp_path)) as harvester:
        harvester.process_with_source(
            "patch", patch_message([]), KafkaSource("topic", 0, 0)
        )
        client.side_effect = OSError("offline")
        with pytest.raises(CatalogError, match="Remote refresh failed"):
            harvester.process_with_source(
                "patch", patch_message([]), KafkaSource("topic", 0, 1)
            )
    with pytest.raises(CatalogError, match="Remote refresh failed"):
        CatalogHarvester(tmp_path)
    with SQLiteCatalog("cmip6", tmp_path / "piddi.db", read_only=True) as catalog:
        assert catalog.get_item("CMIP6", "CMIP6.dataset.v1").item == item
        assert catalog.get_checkpoint("topic", 0).next_offset == 1
