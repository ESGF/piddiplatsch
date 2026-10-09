from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaException

from piddiplatsch.consumer import KafkaConsumer


class FakeClock:
    def __init__(self, *values: float) -> None:
        self.values = iter(values)

    def __call__(self) -> float:
        return next(self.values)


@patch("confluent_kafka.Consumer")
def test_kafka_consumer_stops_after_idle_timeout(consumer_cls):
    client = consumer_cls.return_value
    client.poll.side_effect = [None, None]
    consumer = KafkaConsumer(
        "topic",
        {},
        idle_timeout=3.0,
        clock=FakeClock(0.0, 0.0, 1.0, 3.0),
    )

    assert list(consumer.consume()) == []
    assert client.poll.call_count == 2
    client.close.assert_called_once_with()


@patch("confluent_kafka.Consumer")
def test_kafka_consumer_resets_idle_timeout_after_message(consumer_cls):
    message = MagicMock()
    message.error.return_value = None
    message.key.return_value = b"key"
    message.value.return_value = b'{"value": 1}'
    client = consumer_cls.return_value
    client.poll.side_effect = [message, None]
    consumer = KafkaConsumer(
        "topic",
        {},
        idle_timeout=3.0,
        clock=FakeClock(0.0, 0.0, 2.0, 4.0, 5.0),
    )

    assert list(consumer.consume()) == [("key", {"value": 1})]
    assert client.poll.call_count == 2
    client.close.assert_called_once_with()


@patch("confluent_kafka.Consumer")
def test_kafka_consumer_fails_on_reported_error(consumer_cls):
    error = MagicMock()
    message = MagicMock()
    message.error.return_value = error
    client = consumer_cls.return_value
    client.poll.return_value = message
    consumer = KafkaConsumer("topic", {})

    with pytest.raises(KafkaException):
        next(consumer.consume())

    client.close.assert_called_once_with()


@pytest.mark.parametrize("fails", [False, True])
@patch("piddiplatsch.consumer.signal.signal")
@patch("confluent_kafka.Consumer")
def test_kafka_input_closes_when_processing_stops(consumer_cls, signal_mock, fails):
    from piddiplatsch.config import config
    from piddiplatsch.consumer import HarvestProcessor, start_consumer

    client = consumer_cls.return_value
    message = MagicMock()
    message.error.return_value = None
    message.key.return_value = b"key"
    message.value.return_value = b'{"value": 1}'
    client.poll.return_value = message
    processor = HarvestProcessor()
    if fails:
        config._set("consumer", "max_errors", 1)
        processor.process = MagicMock(side_effect=RuntimeError("mapping failed"))
        with pytest.raises(SystemExit) as exc:
            start_consumer(
                "topic", {"group.id": "test"}, processor=processor, force=True
            )
        assert exc.value.code == 1
    else:
        start_consumer(
            "topic", {"group.id": "test"}, processor=processor, limit=1, force=True
        )
    client.poll.assert_called_once()
    client.close.assert_called_once_with()
