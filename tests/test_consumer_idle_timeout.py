from unittest.mock import MagicMock, patch

from piddiplatsch.consumer import KafkaConsumer


class FakeClock:
    def __init__(self, *values: float) -> None:
        self.values = iter(values)

    def __call__(self) -> float:
        return next(self.values)


@patch("piddiplatsch.consumer.ConfluentConsumer")
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


@patch("piddiplatsch.consumer.ConfluentConsumer")
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
