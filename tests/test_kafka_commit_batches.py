"""Batch acknowledgements include only offsets already durable in the catalog."""

from contextlib import closing
from unittest.mock import MagicMock

import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition

from piddiplatsch.consumer import KafkaConsumer


class Clock:
    now = 0.0

    def __call__(self):
        return self.now


def message(offset, *, partition=0):
    value = MagicMock()
    value.topic.return_value = "topic"
    value.partition.return_value = partition
    value.offset.return_value = offset
    value.error.return_value = None
    value.key.return_value = b"key"
    value.value.return_value = b'{"data": {}}'
    return value


def committed_offsets(client):
    return [
        (p.topic, p.partition, p.offset)
        for p in client.commit.call_args.kwargs["offsets"]
    ]


@pytest.fixture
def client(monkeypatch):
    factory = MagicMock()
    factory.return_value.commit.return_value = []
    monkeypatch.setattr("confluent_kafka.Consumer", factory)
    return factory.return_value


def test_batches_durable_messages_and_retains_highest_offset_per_partition(client):
    consumer = KafkaConsumer("topic", {}, with_source=True, commit_batch_size=3)
    consumer._acknowledge(message(10, partition=0))
    consumer._acknowledge(message(5, partition=1))
    client.commit.assert_not_called()
    consumer._acknowledge(message(11, partition=0))
    assert committed_offsets(client) == [("topic", 0, 12), ("topic", 1, 6)]
    assert client.commit.call_args.kwargs["asynchronous"] is False
    assert consumer._pending_offsets == {}


def test_interval_flushes_low_volume_and_idle_traffic(client):
    clock = Clock()
    consumer = KafkaConsumer(
        "topic", {}, with_source=True, clock=clock, commit_interval=1.0
    )
    consumer._acknowledge(message(0))
    clock.now = 0.9
    consumer._maybe_commit()
    client.commit.assert_not_called()
    clock.now = 1.0
    consumer._maybe_commit()
    assert committed_offsets(client) == [("topic", 0, 1)]


def test_early_input_close_flushes_only_successfully_processed_messages(client):
    client.poll.side_effect = [message(0), message(1)]
    consumer = KafkaConsumer("topic", {}, with_source=True)
    with closing(consumer.consume()) as records:
        first = next(records)
        first.acknowledge()
        # The second event was polled but its processing failed; do not ack it.
        next(records)
        client.commit.assert_not_called()
    assert committed_offsets(client) == [("topic", 0, 1)]
    client.close.assert_called_once()


def test_idle_timeout_flushes_pending_offset(client):
    clock = Clock()
    client.poll.side_effect = [message(0), None]
    consumer = KafkaConsumer("topic", {}, with_source=True, clock=clock, idle_timeout=2)
    with closing(consumer.consume()) as records:
        next(records).acknowledge()
        clock.now = 2.0
        with pytest.raises(StopIteration):
            next(records)
    assert committed_offsets(client) == [("topic", 0, 1)]
    client.close.assert_called_once()


def test_revocation_flushes_only_revoked_partitions_and_loss_never_commits(client):
    consumer = KafkaConsumer("topic", {}, with_source=True)
    consumer._acknowledge(message(1, partition=0))
    consumer._acknowledge(message(3, partition=1))
    callbacks = client.subscribe.call_args.kwargs
    callbacks["on_revoke"](client, [TopicPartition("topic", 0)])
    assert committed_offsets(client) == [("topic", 0, 2)]
    client.commit.reset_mock()
    callbacks["on_lost"](client, [TopicPartition("topic", 1)])
    consumer.flush_acknowledgements()
    client.commit.assert_not_called()


def test_failed_commit_keeps_pending_offsets_and_closes_without_automatic_retry(client):
    client.poll.return_value = message(0)
    client.commit.side_effect = RuntimeError("commit failed")
    consumer = KafkaConsumer("topic", {}, with_source=True, commit_batch_size=1)
    with closing(consumer.consume()) as records:
        record = next(records)
        with pytest.raises(RuntimeError, match="commit failed"):
            record.acknowledge()
        assert consumer._pending_offsets == {("topic", 0): 1}
    client.commit.assert_called_once()
    client.close.assert_called_once()


def test_partial_partition_failure_does_not_clear_pending_offsets(client):
    consumer = KafkaConsumer("topic", {}, with_source=True, commit_batch_size=2)
    consumer._acknowledge(message(0))
    failed = MagicMock()
    failed.error = KafkaError(KafkaError._TIMED_OUT)
    client.commit.return_value = [failed]
    with pytest.raises(KafkaException):
        consumer._acknowledge(message(3, partition=1))
    assert consumer._pending_offsets == {("topic", 0): 1, ("topic", 1): 4}


@pytest.mark.parametrize(
    "options",
    [{"commit_batch_size": 0}, {"commit_batch_size": True}, {"commit_interval": 0}],
)
def test_invalid_batch_configuration_is_rejected_before_subscription(client, options):
    with pytest.raises(ValueError):
        KafkaConsumer("topic", {}, with_source=True, **options)
    client.subscribe.assert_not_called()
