"""Tests for LineageProducer."""

import json
import uuid
from unittest.mock import MagicMock, patch, call

import pytest

from eventlineage.producer import LineageProducer
from eventlineage.event import MessageStatus
from eventlineage.headers import LineageHeaders, LINEAGE_EVENTS_TOPIC
from eventlineage.context import (
    LineageContext,
    set_current_context,
    clear_current_context,
    lineage_scope,
)


@pytest.fixture(autouse=True)
def _clean_context():
    """Ensure lineage context is cleared before and after each test."""
    clear_current_context()
    yield
    clear_current_context()


@pytest.fixture
def mock_producer_cls():
    """Patch confluent_kafka.Producer so no real Kafka connection is made.

    Each call to Producer() returns a distinct MagicMock so that
    the business producer and lineage producer can be inspected
    independently.
    """
    with patch("eventlineage.producer.Producer") as MockProducer:
        MockProducer.side_effect = lambda *a, **kw: MagicMock()
        yield MockProducer


def _make_producer(mock_producer_cls, **overrides) -> LineageProducer:
    """Helper to create a LineageProducer with mocked Kafka."""
    defaults = dict(
        bootstrap_servers="localhost:9092",
        service_name="test-service",
    )
    defaults.update(overrides)
    return LineageProducer(**defaults)


def _extract_header_dict(header_list) -> dict:
    """Convert a list of (key, bytes_value) tuples to {key: str_value}."""
    return {k: v.decode() if isinstance(v, bytes) else v for k, v in header_list}


class TestProduceBasics:
    """Basic produce behaviour."""

    def test_produce_returns_uuid_string(self, mock_producer_cls):
        producer = _make_producer(mock_producer_cls)
        msg_id = producer.produce(topic="orders", value="hello")
        # Should be a valid UUID-4
        parsed = uuid.UUID(msg_id, version=4)
        assert str(parsed) == msg_id

    def test_produce_returns_string(self, mock_producer_cls):
        producer = _make_producer(mock_producer_cls)
        msg_id = producer.produce(topic="orders", value="hello")
        assert isinstance(msg_id, str)

    def test_produce_generates_different_ids(self, mock_producer_cls):
        producer = _make_producer(mock_producer_cls)
        id1 = producer.produce(topic="t", value="a")
        id2 = producer.produce(topic="t", value="b")
        assert id1 != id2


class TestProduceCallsKafka:
    """Verify the underlying Kafka producer is called correctly."""

    def test_calls_business_producer(self, mock_producer_cls):
        producer = _make_producer(mock_producer_cls)
        producer.produce(topic="orders", value="payload")

        biz_producer = producer.producer
        assert biz_producer.produce.call_count == 1
        kwargs = biz_producer.produce.call_args
        assert kwargs.kwargs["topic"] == "orders"
        assert kwargs.kwargs["value"] == b"payload"

    def test_calls_lineage_producer_with_event_to_lineage_topic(self, mock_producer_cls):
        producer = _make_producer(mock_producer_cls)
        producer.produce(topic="orders", value="payload")

        lin_producer = producer.lineage_producer
        assert lin_producer.produce.call_count == 1
        kwargs = lin_producer.produce.call_args
        assert kwargs.kwargs["topic"] == LINEAGE_EVENTS_TOPIC

        # The lineage event should be valid JSON
        event_bytes = kwargs.kwargs["value"]
        event = json.loads(event_bytes.decode())
        assert event["topic"] == "orders"
        assert event["eventType"] == "produced"
        assert event["serviceOrigin"] == "test-service"

    def test_lineage_event_key_is_correlation_id(self, mock_producer_cls):
        producer = _make_producer(mock_producer_cls)
        msg_id = producer.produce(topic="t", value="v", correlation_id="corr-explicit")

        lin_kwargs = producer.lineage_producer.produce.call_args.kwargs
        assert lin_kwargs["key"] == b"corr-explicit"


class TestProduceWithoutContext:
    """Produce without lineage context creates root messages."""

    def test_no_parent_id_header(self, mock_producer_cls):
        producer = _make_producer(mock_producer_cls)
        producer.produce(topic="t", value="v")

        biz_kwargs = producer.producer.produce.call_args.kwargs
        headers = _extract_header_dict(biz_kwargs["headers"])
        assert LineageHeaders.PARENT_ID not in headers

    def test_root_message_id_equals_message_id(self, mock_producer_cls):
        producer = _make_producer(mock_producer_cls)
        msg_id = producer.produce(topic="t", value="v")

        biz_kwargs = producer.producer.produce.call_args.kwargs
        headers = _extract_header_dict(biz_kwargs["headers"])
        assert headers[LineageHeaders.MESSAGE_ID] == msg_id
        assert headers[LineageHeaders.ROOT_MESSAGE_ID] == msg_id

    def test_generates_correlation_id_when_none_provided(self, mock_producer_cls):
        producer = _make_producer(mock_producer_cls)
        producer.produce(topic="t", value="v")

        biz_kwargs = producer.producer.produce.call_args.kwargs
        headers = _extract_header_dict(biz_kwargs["headers"])
        corr_id = headers[LineageHeaders.CORRELATION_ID]
        # Should be a valid UUID
        uuid.UUID(corr_id, version=4)


class TestProduceWithContext:
    """Produce with lineage context inherits parent chain."""

    def test_inherits_parent_id(self, mock_producer_cls):
        ctx = LineageContext(
            message_id="parent-msg",
            correlation_id="corr-from-ctx",
            root_message_id="root-from-ctx",
        )
        set_current_context(ctx)

        producer = _make_producer(mock_producer_cls)
        producer.produce(topic="t", value="v")

        biz_kwargs = producer.producer.produce.call_args.kwargs
        headers = _extract_header_dict(biz_kwargs["headers"])
        assert headers[LineageHeaders.PARENT_ID] == "parent-msg"
        assert headers[LineageHeaders.CAUSATION_ID] == "parent-msg"

    def test_inherits_correlation_id(self, mock_producer_cls):
        ctx = LineageContext(
            message_id="parent-msg",
            correlation_id="corr-from-ctx",
            root_message_id="root-from-ctx",
        )
        set_current_context(ctx)

        producer = _make_producer(mock_producer_cls)
        producer.produce(topic="t", value="v")

        biz_kwargs = producer.producer.produce.call_args.kwargs
        headers = _extract_header_dict(biz_kwargs["headers"])
        assert headers[LineageHeaders.CORRELATION_ID] == "corr-from-ctx"

    def test_inherits_root_message_id(self, mock_producer_cls):
        ctx = LineageContext(
            message_id="parent-msg",
            correlation_id="corr-from-ctx",
            root_message_id="root-from-ctx",
        )
        set_current_context(ctx)

        producer = _make_producer(mock_producer_cls)
        producer.produce(topic="t", value="v")

        biz_kwargs = producer.producer.produce.call_args.kwargs
        headers = _extract_header_dict(biz_kwargs["headers"])
        assert headers[LineageHeaders.ROOT_MESSAGE_ID] == "root-from-ctx"

    def test_explicit_correlation_id_overrides_context(self, mock_producer_cls):
        ctx = LineageContext(
            message_id="p",
            correlation_id="ctx-corr",
            root_message_id="r",
        )
        set_current_context(ctx)

        producer = _make_producer(mock_producer_cls)
        producer.produce(topic="t", value="v", correlation_id="explicit-corr")

        biz_kwargs = producer.producer.produce.call_args.kwargs
        headers = _extract_header_dict(biz_kwargs["headers"])
        assert headers[LineageHeaders.CORRELATION_ID] == "explicit-corr"

    def test_lineage_event_has_parent_id(self, mock_producer_cls):
        ctx = LineageContext(
            message_id="parent-msg",
            correlation_id="corr",
            root_message_id="root",
        )
        set_current_context(ctx)

        producer = _make_producer(mock_producer_cls)
        producer.produce(topic="t", value="v")

        lin_kwargs = producer.lineage_producer.produce.call_args.kwargs
        event = json.loads(lin_kwargs["value"].decode())
        assert event["parentId"] == "parent-msg"


class TestProduceValueSerialization:
    """Test serialization of different value types."""

    def test_dict_value_serialized_to_json(self, mock_producer_cls):
        producer = _make_producer(mock_producer_cls)
        producer.produce(topic="t", value={"amount": 100, "currency": "USD"})

        biz_kwargs = producer.producer.produce.call_args.kwargs
        value_bytes = biz_kwargs["value"]
        parsed = json.loads(value_bytes)
        assert parsed == {"amount": 100, "currency": "USD"}

    def test_string_value_encoded_to_bytes(self, mock_producer_cls):
        producer = _make_producer(mock_producer_cls)
        producer.produce(topic="t", value="plain text")

        biz_kwargs = producer.producer.produce.call_args.kwargs
        assert biz_kwargs["value"] == b"plain text"

    def test_bytes_value_passed_through(self, mock_producer_cls):
        producer = _make_producer(mock_producer_cls)
        raw = b"\x00\x01\x02"
        producer.produce(topic="t", value=raw)

        biz_kwargs = producer.producer.produce.call_args.kwargs
        assert biz_kwargs["value"] == raw


class TestProduceCausationReason:
    """Test causation reason handling."""

    def test_causation_reason_in_headers(self, mock_producer_cls):
        producer = _make_producer(mock_producer_cls)
        producer.produce(topic="t", value="v", causation_reason="OrderPlaced")

        biz_kwargs = producer.producer.produce.call_args.kwargs
        headers = _extract_header_dict(biz_kwargs["headers"])
        assert headers[LineageHeaders.CAUSATION_REASON] == "OrderPlaced"

    def test_no_causation_reason_when_not_set(self, mock_producer_cls):
        producer = _make_producer(mock_producer_cls)
        producer.produce(topic="t", value="v")

        biz_kwargs = producer.producer.produce.call_args.kwargs
        headers = _extract_header_dict(biz_kwargs["headers"])
        assert LineageHeaders.CAUSATION_REASON not in headers


class TestProduceTenantId:
    """Test tenant ID injection."""

    def test_tenant_id_in_headers_when_set(self, mock_producer_cls):
        producer = _make_producer(mock_producer_cls, tenant_id="tenant-A")
        producer.produce(topic="t", value="v")

        biz_kwargs = producer.producer.produce.call_args.kwargs
        headers = _extract_header_dict(biz_kwargs["headers"])
        assert headers[LineageHeaders.TENANT_ID] == "tenant-A"

    def test_no_tenant_id_header_when_not_set(self, mock_producer_cls):
        producer = _make_producer(mock_producer_cls)
        producer.produce(topic="t", value="v")

        biz_kwargs = producer.producer.produce.call_args.kwargs
        headers = _extract_header_dict(biz_kwargs["headers"])
        assert LineageHeaders.TENANT_ID not in headers


class TestServiceOriginHeader:
    """Test service origin in headers."""

    def test_service_origin_header_set(self, mock_producer_cls):
        producer = _make_producer(mock_producer_cls, service_name="payment-svc")
        producer.produce(topic="t", value="v")

        biz_kwargs = producer.producer.produce.call_args.kwargs
        headers = _extract_header_dict(biz_kwargs["headers"])
        assert headers[LineageHeaders.SERVICE_ORIGIN] == "payment-svc"


class TestFlush:
    """Tests for flush()."""

    def test_flush_calls_both_producers(self, mock_producer_cls):
        producer = _make_producer(mock_producer_cls)
        producer.producer.flush.return_value = 0
        producer.lineage_producer.flush.return_value = 0

        producer.flush(timeout=5.0)

        producer.producer.flush.assert_called_once_with(5.0)
        producer.lineage_producer.flush.assert_called_once_with(5.0)


class TestProducerInit:
    """Tests for producer initialization."""

    def test_creates_two_kafka_producers(self, mock_producer_cls):
        _make_producer(mock_producer_cls)
        # Should create two confluent_kafka.Producer instances
        assert mock_producer_cls.call_count == 2

    def test_uses_same_servers_for_lineage_by_default(self, mock_producer_cls):
        _make_producer(mock_producer_cls)
        calls = mock_producer_cls.call_args_list
        biz_config = calls[0][0][0]
        lin_config = calls[1][0][0]
        assert biz_config["bootstrap.servers"] == "localhost:9092"
        assert lin_config["bootstrap.servers"] == "localhost:9092"

    def test_separate_lineage_servers(self, mock_producer_cls):
        _make_producer(
            mock_producer_cls,
            lineage_bootstrap_servers="lineage-kafka:9093",
        )
        calls = mock_producer_cls.call_args_list
        biz_config = calls[0][0][0]
        lin_config = calls[1][0][0]
        assert biz_config["bootstrap.servers"] == "localhost:9092"
        assert lin_config["bootstrap.servers"] == "lineage-kafka:9093"


class TestProduceWithLineageScope:
    """Test produce within a lineage_scope context manager."""

    def test_produce_inside_scope_inherits_context(self, mock_producer_cls):
        ctx = LineageContext(
            message_id="scope-parent",
            correlation_id="scope-corr",
            root_message_id="scope-root",
        )
        producer = _make_producer(mock_producer_cls)

        with lineage_scope(ctx):
            producer.produce(topic="t", value="v")

        biz_kwargs = producer.producer.produce.call_args.kwargs
        headers = _extract_header_dict(biz_kwargs["headers"])
        assert headers[LineageHeaders.PARENT_ID] == "scope-parent"
        assert headers[LineageHeaders.CORRELATION_ID] == "scope-corr"
        assert headers[LineageHeaders.ROOT_MESSAGE_ID] == "scope-root"

    def test_produce_outside_scope_is_root(self, mock_producer_cls):
        ctx = LineageContext(
            message_id="scope-parent",
            correlation_id="scope-corr",
            root_message_id="scope-root",
        )
        producer = _make_producer(mock_producer_cls)

        with lineage_scope(ctx):
            pass  # scope ended

        producer.produce(topic="t", value="v")

        biz_kwargs = producer.producer.produce.call_args.kwargs
        headers = _extract_header_dict(biz_kwargs["headers"])
        assert LineageHeaders.PARENT_ID not in headers
