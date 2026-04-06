"""Tests for LineageEvent data class."""

import json
from datetime import datetime, timezone

import pytest

from eventlineage.event import LineageEvent, EventType, MessageStatus


def _make_event(**overrides) -> LineageEvent:
    """Helper to create a LineageEvent with sensible defaults."""
    defaults = dict(
        event_type=EventType.PRODUCED,
        message_id="msg-001",
        correlation_id="corr-001",
        root_message_id="root-001",
        service_origin="test-service",
        topic="test-topic",
    )
    defaults.update(overrides)
    return LineageEvent(**defaults)


class TestLineageEventCreation:
    """Tests for creating LineageEvent instances."""

    def test_create_with_required_fields(self):
        event = _make_event()
        assert event.event_type == EventType.PRODUCED
        assert event.message_id == "msg-001"
        assert event.correlation_id == "corr-001"
        assert event.root_message_id == "root-001"
        assert event.service_origin == "test-service"
        assert event.topic == "test-topic"

    def test_defaults_for_optional_fields(self):
        event = _make_event()
        assert event.parent_id is None
        assert event.causation_reason is None
        assert event.retry_count == 0
        assert event.status == MessageStatus.OK
        assert event.payload_preview is None
        assert event.tenant_id is None
        assert event.partition is None
        assert event.offset is None

    def test_timestamp_is_utc_by_default(self):
        event = _make_event()
        assert event.timestamp.tzinfo is not None
        assert event.timestamp.tzinfo == timezone.utc


class TestToDict:
    """Tests for to_dict() producing correct camelCase keys."""

    def test_to_dict_keys_are_camel_case(self):
        event = _make_event()
        d = event.to_dict()
        expected_keys = {
            "eventType", "messageId", "parentId", "correlationId",
            "rootMessageId", "causationReason", "serviceOrigin", "topic",
            "retryCount", "timestamp", "status", "payloadPreview",
            "tenantId", "partition", "offset",
        }
        assert set(d.keys()) == expected_keys

    def test_to_dict_values_correct(self):
        event = _make_event()
        d = event.to_dict()
        assert d["eventType"] == "produced"
        assert d["messageId"] == "msg-001"
        assert d["correlationId"] == "corr-001"
        assert d["rootMessageId"] == "root-001"
        assert d["serviceOrigin"] == "test-service"
        assert d["topic"] == "test-topic"
        assert d["retryCount"] == 0
        assert d["status"] == "ok"
        assert d["parentId"] is None
        assert d["causationReason"] is None

    def test_to_dict_timestamp_is_iso_string(self):
        event = _make_event()
        d = event.to_dict()
        # Should be a parseable ISO-format string
        parsed = datetime.fromisoformat(d["timestamp"])
        assert parsed.year == event.timestamp.year


class TestToJson:
    """Tests for to_json() producing valid JSON."""

    def test_to_json_is_valid_json(self):
        event = _make_event()
        raw = event.to_json()
        parsed = json.loads(raw)
        assert isinstance(parsed, dict)
        assert parsed["messageId"] == "msg-001"

    def test_to_json_matches_to_dict(self):
        event = _make_event()
        assert json.loads(event.to_json()) == event.to_dict()


class TestFromDict:
    """Tests for from_dict() roundtrip."""

    def test_roundtrip(self):
        original = _make_event(
            parent_id="parent-001",
            causation_reason="OrderCreated",
            retry_count=2,
            status=MessageStatus.FAILED,
            tenant_id="tenant-A",
            partition=3,
            offset=42,
        )
        d = original.to_dict()
        restored = LineageEvent.from_dict(d)

        assert restored.event_type == original.event_type
        assert restored.message_id == original.message_id
        assert restored.parent_id == original.parent_id
        assert restored.correlation_id == original.correlation_id
        assert restored.root_message_id == original.root_message_id
        assert restored.causation_reason == original.causation_reason
        assert restored.service_origin == original.service_origin
        assert restored.topic == original.topic
        assert restored.retry_count == original.retry_count
        assert restored.status == original.status
        assert restored.tenant_id == original.tenant_id
        assert restored.partition == original.partition
        assert restored.offset == original.offset

    def test_roundtrip_minimal(self):
        original = _make_event()
        d = original.to_dict()
        restored = LineageEvent.from_dict(d)
        assert restored.message_id == original.message_id
        assert restored.parent_id is None
        assert restored.retry_count == 0

    def test_from_dict_defaults_status_to_ok(self):
        d = _make_event().to_dict()
        del d["status"]
        restored = LineageEvent.from_dict(d)
        assert restored.status == MessageStatus.OK

    def test_from_dict_defaults_retry_count_to_zero(self):
        d = _make_event().to_dict()
        del d["retryCount"]
        restored = LineageEvent.from_dict(d)
        assert restored.retry_count == 0


class TestWithParent:
    """Tests for with_parent() fluent setter."""

    def test_with_parent_sets_parent_id(self):
        event = _make_event()
        result = event.with_parent("parent-123")
        assert event.parent_id == "parent-123"
        assert result is event  # fluent interface

    def test_with_parent_sets_causation_reason(self):
        event = _make_event()
        event.with_parent("parent-123", reason="OrderPlaced")
        assert event.parent_id == "parent-123"
        assert event.causation_reason == "OrderPlaced"

    def test_with_parent_no_reason_leaves_reason_none(self):
        event = _make_event()
        event.with_parent("parent-123")
        assert event.causation_reason is None


class TestWithRetry:
    """Tests for with_retry() fluent setter."""

    def test_with_retry_sets_retry_count(self):
        event = _make_event()
        result = event.with_retry(3)
        assert event.retry_count == 3
        assert result is event

    def test_with_retry_zero(self):
        event = _make_event(retry_count=5)
        event.with_retry(0)
        assert event.retry_count == 0


class TestWithStatus:
    """Tests for with_status() fluent setter."""

    def test_with_status_sets_status(self):
        event = _make_event()
        result = event.with_status(MessageStatus.FAILED)
        assert event.status == MessageStatus.FAILED
        assert result is event

    def test_with_status_pending(self):
        event = _make_event()
        event.with_status(MessageStatus.PENDING)
        assert event.status == MessageStatus.PENDING


class TestWithPayloadPreview:
    """Tests for with_payload_preview() truncation."""

    def test_short_payload_not_truncated(self):
        event = _make_event()
        event.with_payload_preview("short payload")
        assert event.payload_preview == "short payload"

    def test_exactly_500_chars_not_truncated(self):
        payload = "x" * 500
        event = _make_event()
        event.with_payload_preview(payload)
        assert event.payload_preview == payload
        assert len(event.payload_preview) == 500

    def test_over_500_chars_truncated_with_ellipsis(self):
        payload = "y" * 600
        event = _make_event()
        event.with_payload_preview(payload)
        assert len(event.payload_preview) == 503  # 500 + "..."
        assert event.payload_preview.endswith("...")
        assert event.payload_preview[:500] == "y" * 500

    def test_empty_payload(self):
        event = _make_event()
        event.with_payload_preview("")
        assert event.payload_preview == ""


class TestIsRootMessage:
    """Tests for is_root_message()."""

    def test_true_when_no_parent(self):
        event = _make_event(parent_id=None)
        assert event.is_root_message() is True

    def test_true_when_message_id_equals_root(self):
        event = _make_event(
            message_id="same-id",
            root_message_id="same-id",
            parent_id="some-parent",
        )
        assert event.is_root_message() is True

    def test_false_when_has_parent_and_different_root(self):
        event = _make_event(
            message_id="child-id",
            root_message_id="root-id",
            parent_id="parent-id",
        )
        assert event.is_root_message() is False


class TestIsRetry:
    """Tests for is_retry()."""

    def test_true_when_retry_count_positive(self):
        event = _make_event(retry_count=1)
        assert event.is_retry() is True

    def test_true_when_retry_count_large(self):
        event = _make_event(retry_count=10)
        assert event.is_retry() is True

    def test_false_when_retry_count_zero(self):
        event = _make_event(retry_count=0)
        assert event.is_retry() is False


class TestIsFailed:
    """Tests for is_failed()."""

    def test_true_when_status_failed(self):
        event = _make_event(status=MessageStatus.FAILED)
        assert event.is_failed() is True

    def test_false_when_status_ok(self):
        event = _make_event(status=MessageStatus.OK)
        assert event.is_failed() is False

    def test_false_when_status_pending(self):
        event = _make_event(status=MessageStatus.PENDING)
        assert event.is_failed() is False


class TestEventTypeEnum:
    """Tests for EventType enum values."""

    def test_produced_value(self):
        assert EventType.PRODUCED.value == "produced"

    def test_consumed_value(self):
        assert EventType.CONSUMED.value == "consumed"


class TestMessageStatusEnum:
    """Tests for MessageStatus enum values."""

    def test_ok_value(self):
        assert MessageStatus.OK.value == "ok"

    def test_failed_value(self):
        assert MessageStatus.FAILED.value == "failed"

    def test_pending_value(self):
        assert MessageStatus.PENDING.value == "pending"
