"""Tests for LineageConsumer and LineageMessage."""

import json
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
from confluent_kafka import KafkaError

from eventlineage.consumer import LineageConsumer, LineageMessage
from eventlineage.headers import LineageHeaders, LINEAGE_EVENTS_TOPIC
from eventlineage.context import (
    LineageContext,
    get_current_context,
    clear_current_context,
)


@pytest.fixture(autouse=True)
def _clean_context():
    """Ensure lineage context is cleared before and after each test."""
    clear_current_context()
    yield
    clear_current_context()


def _make_kafka_message(
    value=b'{"data": 1}',
    key=b"key-1",
    topic="test-topic",
    partition=0,
    offset=42,
    timestamp=(1, 1700000000000),
    headers=None,
    error=None,
):
    """Create a mock confluent_kafka.Message."""
    msg = MagicMock()
    msg.value.return_value = value
    msg.key.return_value = key
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.timestamp.return_value = timestamp
    msg.headers.return_value = headers
    msg.error.return_value = error
    return msg


def _lineage_headers(
    message_id="msg-001",
    correlation_id="corr-001",
    root_message_id="root-001",
    parent_id=None,
    causation_reason=None,
    retry_count="0",
    service_origin="origin-service",
):
    """Build a list of lineage header tuples as confluent_kafka uses."""
    hdrs = [
        (LineageHeaders.MESSAGE_ID, message_id.encode()),
        (LineageHeaders.CORRELATION_ID, correlation_id.encode()),
        (LineageHeaders.ROOT_MESSAGE_ID, root_message_id.encode()),
        (LineageHeaders.RETRY_COUNT, retry_count.encode()),
        (LineageHeaders.SERVICE_ORIGIN, service_origin.encode()),
    ]
    if parent_id:
        hdrs.append((LineageHeaders.PARENT_ID, parent_id.encode()))
    if causation_reason:
        hdrs.append((LineageHeaders.CAUSATION_REASON, causation_reason.encode()))
    return hdrs


@pytest.fixture
def mock_kafka():
    """Patch both confluent_kafka.Consumer and confluent_kafka.Producer.

    Consumer is imported at the module level in eventlineage.consumer,
    so we patch it there.  Producer is imported lazily inside __init__
    via ``from confluent_kafka import Producer``, so we patch the
    canonical location ``confluent_kafka.Producer``.
    """
    consumer_instance = MagicMock()
    producer_instance = MagicMock()
    with patch("eventlineage.consumer.Consumer", return_value=consumer_instance) as MockConsumer, \
         patch("confluent_kafka.Producer", return_value=producer_instance) as MockProducer:
        yield {
            "ConsumerClass": MockConsumer,
            "ProducerClass": MockProducer,
            "consumer_instance": consumer_instance,
            "producer_instance": producer_instance,
        }


def _make_consumer(mock_kafka, **overrides) -> LineageConsumer:
    """Helper to create a LineageConsumer with mocked Kafka."""
    defaults = dict(
        bootstrap_servers="localhost:9092",
        group_id="test-group",
        service_name="test-service",
        topics=["test-topic"],
    )
    defaults.update(overrides)
    return LineageConsumer(**defaults)


class TestPollReturnsNone:
    """Test that poll returns None when there are no messages."""

    def test_poll_returns_none_when_no_messages(self, mock_kafka):
        consumer = _make_consumer(mock_kafka)
        mock_kafka["consumer_instance"].poll.return_value = None

        result = consumer.poll(timeout=0.1)
        assert result is None

    def test_poll_returns_none_on_partition_eof(self, mock_kafka):
        consumer = _make_consumer(mock_kafka)
        eof_msg = MagicMock()
        eof_error = MagicMock()
        eof_error.code.return_value = KafkaError._PARTITION_EOF
        eof_msg.error.return_value = eof_error
        mock_kafka["consumer_instance"].poll.return_value = eof_msg

        result = consumer.poll(timeout=0.1)
        assert result is None


class TestProcessMessage:
    """Tests for _process_message() extracting lineage headers."""

    def test_extracts_message_id(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, auto_set_context=False)
        msg = _make_kafka_message(
            headers=_lineage_headers(message_id="extracted-id")
        )

        lineage_msg = consumer._process_message(msg)
        assert lineage_msg.context.message_id == "extracted-id"

    def test_extracts_correlation_id(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, auto_set_context=False)
        msg = _make_kafka_message(
            headers=_lineage_headers(correlation_id="corr-extracted")
        )

        lineage_msg = consumer._process_message(msg)
        assert lineage_msg.context.correlation_id == "corr-extracted"

    def test_extracts_root_message_id(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, auto_set_context=False)
        msg = _make_kafka_message(
            headers=_lineage_headers(root_message_id="root-extracted")
        )

        lineage_msg = consumer._process_message(msg)
        assert lineage_msg.context.root_message_id == "root-extracted"

    def test_extracts_retry_count(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, auto_set_context=False)
        msg = _make_kafka_message(
            headers=_lineage_headers(retry_count="3")
        )

        lineage_msg = consumer._process_message(msg)
        assert lineage_msg.context.retry_count == 3

    def test_defaults_root_message_id_to_message_id(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, auto_set_context=False)
        # Headers with no ROOT_MESSAGE_ID
        hdrs = [
            (LineageHeaders.MESSAGE_ID, b"msg-abc"),
            (LineageHeaders.CORRELATION_ID, b"corr-abc"),
            (LineageHeaders.RETRY_COUNT, b"0"),
        ]
        msg = _make_kafka_message(headers=hdrs)

        lineage_msg = consumer._process_message(msg)
        assert lineage_msg.context.root_message_id == "msg-abc"

    def test_handles_missing_headers_gracefully(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, auto_set_context=False)
        msg = _make_kafka_message(headers=None)

        lineage_msg = consumer._process_message(msg)
        assert lineage_msg.context.message_id == ""
        assert lineage_msg.context.correlation_id == ""
        assert lineage_msg.context.retry_count == 0


class TestAutoSetContext:
    """Tests for auto_set_context behaviour."""

    def test_sets_context_when_enabled(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, auto_set_context=True)
        msg = _make_kafka_message(
            headers=_lineage_headers(
                message_id="ctx-msg",
                correlation_id="ctx-corr",
                root_message_id="ctx-root",
            )
        )

        consumer._process_message(msg)

        ctx = get_current_context()
        assert ctx is not None
        assert ctx.message_id == "ctx-msg"
        assert ctx.correlation_id == "ctx-corr"
        assert ctx.root_message_id == "ctx-root"

    def test_does_not_set_context_when_disabled(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, auto_set_context=False)
        msg = _make_kafka_message(
            headers=_lineage_headers(message_id="ctx-msg")
        )

        consumer._process_message(msg)

        assert get_current_context() is None


class TestEmitConsumeEvent:
    """Tests for lineage consume event emission."""

    def test_emits_consume_event_when_enabled(self, mock_kafka):
        consumer = _make_consumer(
            mock_kafka,
            auto_set_context=False,
            emit_consume_events=True,
        )
        msg = _make_kafka_message(
            headers=_lineage_headers(message_id="emit-msg", correlation_id="emit-corr"),
            topic="orders",
        )

        consumer._process_message(msg)

        lin_producer = consumer.lineage_producer
        assert lin_producer.produce.call_count == 1
        kwargs = lin_producer.produce.call_args.kwargs
        assert kwargs["topic"] == LINEAGE_EVENTS_TOPIC

        event = json.loads(kwargs["value"].decode())
        assert event["eventType"] == "consumed"
        assert event["messageId"] == "emit-msg"
        assert event["topic"] == "orders"

    def test_no_event_when_disabled(self, mock_kafka):
        consumer = _make_consumer(
            mock_kafka,
            auto_set_context=False,
            emit_consume_events=False,
        )
        msg = _make_kafka_message(
            headers=_lineage_headers(message_id="emit-msg")
        )

        consumer._process_message(msg)

        # lineage_producer should be None when emit_consume_events=False
        assert consumer.lineage_producer is None

    def test_no_event_when_message_id_empty(self, mock_kafka):
        consumer = _make_consumer(
            mock_kafka,
            auto_set_context=False,
            emit_consume_events=True,
        )
        msg = _make_kafka_message(headers=None)  # No headers, so message_id is ""

        consumer._process_message(msg)

        # Should not emit because message_id is empty
        lin_producer = consumer.lineage_producer
        assert lin_producer.produce.call_count == 0


class TestLineageMessage:
    """Tests for LineageMessage wrapper."""

    def test_value_property(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, auto_set_context=False)
        msg = _make_kafka_message(
            value=b'{"order": "123"}',
            headers=_lineage_headers(),
        )

        lineage_msg = consumer._process_message(msg)
        assert lineage_msg.value == b'{"order": "123"}'

    def test_value_json(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, auto_set_context=False)
        msg = _make_kafka_message(
            value=b'{"order": "123"}',
            headers=_lineage_headers(),
        )

        lineage_msg = consumer._process_message(msg)
        assert lineage_msg.value_json() == {"order": "123"}

    def test_value_str(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, auto_set_context=False)
        msg = _make_kafka_message(
            value=b"hello world",
            headers=_lineage_headers(),
        )

        lineage_msg = consumer._process_message(msg)
        assert lineage_msg.value_str() == "hello world"

    def test_key_property(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, auto_set_context=False)
        msg = _make_kafka_message(
            key=b"my-key",
            headers=_lineage_headers(),
        )

        lineage_msg = consumer._process_message(msg)
        assert lineage_msg.key == b"my-key"

    def test_key_str(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, auto_set_context=False)
        msg = _make_kafka_message(
            key=b"my-key",
            headers=_lineage_headers(),
        )

        lineage_msg = consumer._process_message(msg)
        assert lineage_msg.key_str() == "my-key"

    def test_key_str_none(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, auto_set_context=False)
        msg = _make_kafka_message(
            key=None,
            headers=_lineage_headers(),
        )

        lineage_msg = consumer._process_message(msg)
        assert lineage_msg.key_str() is None

    def test_topic_property(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, auto_set_context=False)
        msg = _make_kafka_message(
            topic="payments",
            headers=_lineage_headers(),
        )

        lineage_msg = consumer._process_message(msg)
        assert lineage_msg.topic == "payments"

    def test_partition_property(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, auto_set_context=False)
        msg = _make_kafka_message(
            partition=7,
            headers=_lineage_headers(),
        )

        lineage_msg = consumer._process_message(msg)
        assert lineage_msg.partition == 7

    def test_offset_property(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, auto_set_context=False)
        msg = _make_kafka_message(
            offset=999,
            headers=_lineage_headers(),
        )

        lineage_msg = consumer._process_message(msg)
        assert lineage_msg.offset == 999

    def test_timestamp_property(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, auto_set_context=False)
        msg = _make_kafka_message(
            timestamp=(1, 1700000000000),
            headers=_lineage_headers(),
        )

        lineage_msg = consumer._process_message(msg)
        assert lineage_msg.timestamp == (1, 1700000000000)

    def test_headers_property(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, auto_set_context=False)
        raw_headers = _lineage_headers(message_id="h-msg")
        msg = _make_kafka_message(headers=raw_headers)

        lineage_msg = consumer._process_message(msg)
        hdrs = lineage_msg.headers
        assert isinstance(hdrs, dict)
        assert hdrs[LineageHeaders.MESSAGE_ID] == "h-msg"

    def test_lineage_convenience_properties(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, auto_set_context=False)
        msg = _make_kafka_message(
            headers=_lineage_headers(
                message_id="conv-msg",
                correlation_id="conv-corr",
                root_message_id="conv-root",
                retry_count="2",
            )
        )

        lineage_msg = consumer._process_message(msg)
        assert lineage_msg.message_id == "conv-msg"
        assert lineage_msg.correlation_id == "conv-corr"
        assert lineage_msg.root_message_id == "conv-root"
        assert lineage_msg.retry_count == 2


class TestConsumerInit:
    """Tests for consumer initialization."""

    def test_subscribes_to_topics(self, mock_kafka):
        _make_consumer(mock_kafka, topics=["t1", "t2"])
        mock_kafka["consumer_instance"].subscribe.assert_called_once_with(["t1", "t2"])

    def test_no_subscribe_when_topics_is_none(self, mock_kafka):
        _make_consumer(mock_kafka, topics=None)
        mock_kafka["consumer_instance"].subscribe.assert_not_called()

    def test_creates_lineage_producer_when_emit_enabled(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, emit_consume_events=True)
        assert consumer.lineage_producer is not None

    def test_no_lineage_producer_when_emit_disabled(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, emit_consume_events=False)
        assert consumer.lineage_producer is None


class TestConsumerClose:
    """Tests for consumer close behaviour."""

    def test_close_clears_context(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, auto_set_context=True)
        msg = _make_kafka_message(
            headers=_lineage_headers(message_id="close-msg")
        )
        consumer._process_message(msg)
        assert get_current_context() is not None

        consumer.close()
        assert get_current_context() is None

    def test_close_calls_consumer_close(self, mock_kafka):
        consumer = _make_consumer(mock_kafka)
        consumer.close()
        mock_kafka["consumer_instance"].close.assert_called_once()

    def test_close_flushes_lineage_producer(self, mock_kafka):
        consumer = _make_consumer(mock_kafka, emit_consume_events=True)
        consumer.close()
        consumer.lineage_producer.flush.assert_called_once()


class TestConsumerContextManager:
    """Tests for consumer as context manager."""

    def test_context_manager_calls_close(self, mock_kafka):
        consumer = _make_consumer(mock_kafka)
        with consumer:
            pass
        mock_kafka["consumer_instance"].close.assert_called_once()
