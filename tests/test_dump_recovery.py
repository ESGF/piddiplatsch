"""A dump rebuild hands off to Kafka using durable per-partition positions."""

import gzip
import json
from contextlib import closing
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner
from confluent_kafka import TopicPartition

from piddiplatsch.catalog.replay import archive_paths, archived_messages
from piddiplatsch.catalog.sqlite import SQLiteCatalog
from piddiplatsch.cli import cli
from piddiplatsch.config import config
from piddiplatsch.consumer import KafkaConsumer
from piddiplatsch.exceptions import CatalogError


def archive_record(payload, partition, offset):
    return dict(
        source=dict(topic="topic", partition=partition, offset=offset),
        key="key",
        payload=payload,
    )


def kafka_message(record):
    message = MagicMock()
    message.topic.return_value = record["source"]["topic"]
    message.partition.return_value = record["source"]["partition"]
    message.offset.return_value = record["source"]["offset"]
    message.key.return_value = b"key"
    message.value.return_value = json.dumps(record["payload"]).encode()
    message.error.return_value = None
    return message


@pytest.fixture
def restored(tmp_path, publication_sample, monkeypatch):
    backup = tmp_path / "backup"
    backup.mkdir()
    output = tmp_path / "recovered"
    config._set("consumer", "output_dir", str(output))
    config._set("consumer", "topic", "topic")
    config._set("catalog", "directory", str(output))
    messages = {}
    for name in ("cmip6", "cmip7"):
        messages[name] = publication_sample(name)
        messages[name]["data"]["payload"]["item"]["properties"]["recovery_steps"] = []
    payload = messages["cmip6"]["data"]["payload"]
    patch = dict(
        data=dict(
            payload=dict(
                method="PATCH",
                collection_id=payload["collection_id"],
                item_id=payload["item"]["id"],
                patch=[
                    dict(op="add", path="/properties/recovery_steps/-", value="one")
                ],
            )
        )
    )
    first = backup / "dump_messages_2026-10-30_000001.jsonl.gz"
    with gzip.open(first, "wt", encoding="utf-8") as stream:
        for record in (
            archive_record(messages["cmip6"], 0, 100),
            archive_record(messages["cmip7"], 1, 40),
        ):
            stream.write(json.dumps(record) + "\n")
    last = backup / "dump_messages_2026-10-31_000001.jsonl"
    last.write_text(json.dumps(archive_record(patch, 0, 101)) + "\n")
    # The directory selector must not replay unrelated publication/unknown files.
    (backup / "unknown_projects_2026-10-31_000001.jsonl").write_text("unrelated")
    (backup / "handles.jsonl").write_text("unrelated")
    factory = MagicMock()
    factory.return_value.commit.return_value = []
    monkeypatch.setattr("confluent_kafka.Consumer", factory)
    monkeypatch.setattr("piddiplatsch.consumer.signal.signal", MagicMock())
    for _ in range(2):
        result = CliRunner().invoke(
            cli, ["--silent", "harvest", "--archive", str(backup)]
        )
        assert result.exit_code == 0, result.output
    factory.assert_not_called()  # File replay never connects to Kafka.
    assert len(list(archived_messages((backup,)))) == 3
    return output, backup, messages, patch, factory


def test_dump_replay_then_kafka_uses_local_offsets_despite_broker_group_position(
    restored,
):
    output, backup, messages, patch, factory = restored
    client = factory.return_value
    client.committed.return_value = [
        TopicPartition("topic", 0, 999),
        TopicPartition("topic", 1, 999),
    ]
    client.get_watermark_offsets.side_effect = lambda partition, **kwargs: (
        (90, 200) if partition.partition == 0 else (35, 60)
    )
    records = iter(
        [
            kafka_message(
                archive_record(patch, 0, 101)
            ),  # An overlap is still harmless.
            kafka_message(archive_record(patch, 0, 102)),
            kafka_message(archive_record(messages["cmip7"], 1, 41)),
        ]
    )

    def poll(**kwargs):
        if not client.assign.called:
            callback = client.subscribe.call_args.kwargs["on_assign"]
            callback(client, [TopicPartition("topic", 0), TopicPartition("topic", 1)])
        return next(records)

    client.poll.side_effect = poll
    before = sorted(path.name for path in backup.iterdir())
    result = CliRunner().invoke(cli, ["--silent", "harvest", "--limit", "3"])
    assert result.exit_code == 0, result.output
    assert [(p.partition, p.offset) for p in client.assign.call_args.args[0]] == [
        (0, 102),
        (1, 41),
    ]
    client.committed.assert_not_called()
    assert factory.call_args.args[0]["auto.offset.reset"] == "error"
    assert sorted(path.name for path in backup.iterdir()) == before
    with SQLiteCatalog("cmip6", output / "piddi.db", read_only=True) as catalog:
        assert catalog.get_checkpoint("topic", 0).next_offset == 103
        assert catalog.get_checkpoint("topic", 1).next_offset == 42
        assert (
            catalog.pending_work().count == 3
        )  # POST plus two distinct PATCH offsets.
        item = messages["cmip6"]["data"]["payload"]
        assert catalog.get_item(item["collection_id"], item["item"]["id"]).item[
            "properties"
        ]["recovery_steps"] == ["one", "one"]
    client.close.assert_called_once()


@pytest.mark.parametrize(
    "bounds, text",
    [((103, 200), "missing interval [102, 103)"), ((0, 101), "beyond Kafka's end")],
)
def test_invalid_kafka_position_stops_without_processing_or_committing(
    restored, bounds, text
):
    output, _, _, _, factory = restored
    client = factory.return_value
    client.get_watermark_offsets.return_value = bounds

    def poll(**kwargs):
        client.subscribe.call_args.kwargs["on_assign"](
            client, [TopicPartition("topic", 0)]
        )

    client.poll.side_effect = poll
    result = CliRunner().invoke(cli, ["--silent", "harvest", "--limit", "1"])
    assert result.exit_code == 1
    assert text in result.output
    client.assign.assert_not_called()
    client.commit.assert_not_called()
    client.close.assert_called_once()
    with SQLiteCatalog("cmip6", output / "piddi.db", read_only=True) as catalog:
        assert catalog.get_checkpoint("topic", 0).next_offset == 102
        assert catalog.pending_work().count == 2


@pytest.mark.parametrize(
    "settings, incremental",
    [
        ({}, False),
        ({"partition.assignment.strategy": "cooperative-sticky"}, True),
        ({"group.protocol": "consumer"}, True),
    ],
)
def test_assignment_rereads_checkpoints_and_supports_empty_and_new_partitions(
    monkeypatch, settings, incremental
):
    factory = MagicMock()
    client = factory.return_value
    client.get_watermark_offsets.return_value = (10, 20)
    monkeypatch.setattr("confluent_kafka.Consumer", factory)
    positions = {0: 15, 1: None, 2: 20, 3: 10}
    KafkaConsumer(
        "topic",
        settings,
        with_source=True,
        checkpoint_reader=lambda topic, p: positions[p],
    )
    callback = client.subscribe.call_args.kwargs["on_assign"]
    partitions = [TopicPartition("topic", p) for p in positions]
    callback(client, partitions)
    assign = client.incremental_assign if incremental else client.assign
    other = client.assign if incremental else client.incremental_assign
    assert [(p.partition, p.offset) for p in assign.call_args.args[0]] == [
        (0, 15),
        (1, 10),
        (2, 20),
        (3, 10),
    ]
    other.assert_not_called()
    assert all(
        call.kwargs == dict(timeout=10.0, cached=False)
        for call in client.get_watermark_offsets.call_args_list
    )
    positions[0] = 19
    callback(client, [TopicPartition("topic", 0)])
    assert assign.call_args.args[0][0].offset == 19
    # An empty partition at its log end is a valid place to wait for new data.
    client.get_watermark_offsets.return_value = (0, 0)
    positions[4] = None
    callback(client, [TopicPartition("topic", 4)])
    assert assign.call_args.args[0][0].offset == 0


def test_all_partitions_are_validated_before_assignment_and_poll_cannot_hide_failure(
    monkeypatch,
):
    factory = MagicMock()
    client = factory.return_value
    monkeypatch.setattr("confluent_kafka.Consumer", factory)
    client.get_watermark_offsets.side_effect = [(0, 100), (20, 100)]
    consumer = KafkaConsumer(
        "topic", {}, with_source=True, checkpoint_reader=lambda topic, p: 10
    )

    def poll(**kwargs):
        try:
            client.subscribe.call_args.kwargs["on_assign"](
                client, [TopicPartition("topic", 0), TopicPartition("topic", 1)]
            )
        except CatalogError:
            pass  # Even if a client defers callback errors, no message may escape.
        return kafka_message(archive_record({}, 0, 10))

    client.poll.side_effect = poll
    with (
        closing(consumer.consume()) as records,
        pytest.raises(CatalogError, match="Recovery gap for topic/1"),
    ):
        next(records)
    client.assign.assert_not_called()
    client.commit.assert_not_called()
    client.close.assert_called_once()


def test_empty_dump_directory_is_rejected_before_cleaning(restored, tmp_path):
    output, _, _, _, _ = restored
    empty = tmp_path / "empty"
    empty.mkdir()
    result = CliRunner().invoke(
        cli,
        [
            "--silent",
            "admin",
            "recover",
            "--apply",
            "--clean-catalog",
            "--archive",
            str(empty),
        ],
    )
    assert result.exit_code == 65
    assert "No dump_messages_" in result.output
    with SQLiteCatalog("cmip6", output / "piddi.db", read_only=True) as catalog:
        assert catalog.get_checkpoint("topic", 0).next_offset == 102


def test_archive_parts_sort_chronologically_across_directories(tmp_path):
    a, z = tmp_path / "a", tmp_path / "z"
    a.mkdir()
    z.mkdir()
    later = a / "dump_messages_2026-11-02_000002.jsonl"
    earlier = z / "dump_messages_2026-11-01_000001.jsonl.gz"
    later.touch()
    earlier.touch()
    assert archive_paths((a, z, earlier)) == (earlier, later)
