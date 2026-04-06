"""Tests for header constants and configuration values."""

import pytest

from eventlineage.headers import (
    LineageHeaders,
    LINEAGE_EVENTS_TOPIC,
    DEFAULT_PAYLOAD_PREVIEW_MAX_LENGTH,
    DEFAULT_CHAIN_DEPTH_LIMIT,
)


class TestLineageHeadersPrefix:
    """All header constants must have the x-lineage- prefix."""

    HEADER_ATTRS = [
        "MESSAGE_ID",
        "PARENT_ID",
        "CORRELATION_ID",
        "CAUSATION_ID",
        "CAUSATION_REASON",
        "ROOT_MESSAGE_ID",
        "SERVICE_ORIGIN",
        "RETRY_COUNT",
        "TIMESTAMP",
        "TENANT_ID",
    ]

    @pytest.mark.parametrize("attr", HEADER_ATTRS)
    def test_header_has_x_lineage_prefix(self, attr):
        value = getattr(LineageHeaders, attr)
        assert value.startswith("x-lineage-"), (
            f"LineageHeaders.{attr} = {value!r} does not start with 'x-lineage-'"
        )

    def test_all_header_values_are_unique(self):
        values = [getattr(LineageHeaders, a) for a in self.HEADER_ATTRS]
        assert len(values) == len(set(values)), "Duplicate header values found"


class TestHeaderValues:
    """Spot-check specific header key values."""

    def test_message_id(self):
        assert LineageHeaders.MESSAGE_ID == "x-lineage-message-id"

    def test_parent_id(self):
        assert LineageHeaders.PARENT_ID == "x-lineage-parent-id"

    def test_correlation_id(self):
        assert LineageHeaders.CORRELATION_ID == "x-lineage-correlation-id"

    def test_causation_id(self):
        assert LineageHeaders.CAUSATION_ID == "x-lineage-causation-id"

    def test_causation_reason(self):
        assert LineageHeaders.CAUSATION_REASON == "x-lineage-causation-reason"

    def test_root_message_id(self):
        assert LineageHeaders.ROOT_MESSAGE_ID == "x-lineage-root-id"

    def test_service_origin(self):
        assert LineageHeaders.SERVICE_ORIGIN == "x-lineage-service"

    def test_retry_count(self):
        assert LineageHeaders.RETRY_COUNT == "x-lineage-retry-count"

    def test_timestamp(self):
        assert LineageHeaders.TIMESTAMP == "x-lineage-timestamp"

    def test_tenant_id(self):
        assert LineageHeaders.TENANT_ID == "x-lineage-tenant-id"


class TestLineageEventsTopic:
    """Tests for the lineage events topic constant."""

    def test_equals_expected_value(self):
        assert LINEAGE_EVENTS_TOPIC == "_lineage.events"


class TestDefaultConstants:
    """Tests for default configuration constants."""

    def test_payload_preview_max_length(self):
        assert DEFAULT_PAYLOAD_PREVIEW_MAX_LENGTH == 500

    def test_chain_depth_limit(self):
        assert DEFAULT_CHAIN_DEPTH_LIMIT == 20
