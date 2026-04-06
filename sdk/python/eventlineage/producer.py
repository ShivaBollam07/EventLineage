"""
Lineage-aware Kafka producer.

Wraps confluent-kafka Producer to automatically inject lineage headers
and emit lineage events.
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Union

from confluent_kafka import Producer

from .headers import LineageHeaders, LINEAGE_EVENTS_TOPIC
from .event import LineageEvent, EventType, MessageStatus
from .context import get_current_context, LineageContext


class LineageProducer:
    """
    A Kafka producer that automatically tracks message lineage.

    Wraps confluent-kafka Producer to:
    1. Inject lineage headers into every produced message
    2. Emit lineage events to the _lineage.events topic
    3. Link produced messages to consumed messages via context

    Usage:
        producer = LineageProducer(
            bootstrap_servers="localhost:9092",
            service_name="payment-service"
        )

        # Produce with automatic lineage tracking
        producer.produce(
            topic="payments",
            value={"amount": 100},
            causation_reason="OrderCreated"
        )
    """

    def __init__(
        self,
        bootstrap_servers: str,
        service_name: str,
        lineage_bootstrap_servers: Optional[str] = None,
        tenant_id: Optional[str] = None,
        include_payload_preview: bool = True,
        payload_preview_max_length: int = 500,
        **kafka_config
    ):
        """
        Initialize the lineage producer.

        Args:
            bootstrap_servers: Kafka bootstrap servers for business messages
            service_name: Name of this service (used in lineage tracking)
            lineage_bootstrap_servers: Separate Kafka cluster for lineage events
                                      (defaults to same as bootstrap_servers)
            tenant_id: Optional tenant ID for multi-tenancy
            include_payload_preview: Whether to include truncated payload in lineage
            payload_preview_max_length: Max chars for payload preview
            **kafka_config: Additional confluent-kafka Producer config
        """
        self.service_name = service_name
        self.tenant_id = tenant_id
        self.include_payload_preview = include_payload_preview
        self.payload_preview_max_length = payload_preview_max_length

        # Business message producer
        config = {"bootstrap.servers": bootstrap_servers, **kafka_config}
        self.producer = Producer(config)

        # Lineage event producer (may be same or different cluster)
        lineage_servers = lineage_bootstrap_servers or bootstrap_servers
        lineage_config = {"bootstrap.servers": lineage_servers, **kafka_config}
        self.lineage_producer = Producer(lineage_config)

    def produce(
        self,
        topic: str,
        value: Union[str, bytes, dict],
        key: Optional[Union[str, bytes]] = None,
        headers: Optional[Dict[str, str]] = None,
        partition: Optional[int] = None,
        causation_reason: Optional[str] = None,
        correlation_id: Optional[str] = None,
        retry_count: int = 0,
        status: MessageStatus = MessageStatus.OK,
        on_delivery: Optional[Callable] = None,
    ) -> str:
        """
        Produce a message with automatic lineage tracking.

        Args:
            topic: Target Kafka topic
            value: Message value (dict will be JSON serialized)
            key: Optional message key
            headers: Additional headers to include
            partition: Optional target partition
            causation_reason: Why this message was produced
            correlation_id: Override correlation ID (usually inherited from context)
            retry_count: Retry attempt number
            status: Message status
            on_delivery: Callback for delivery reports

        Returns:
            The generated message ID
        """
        # Generate message ID
        message_id = str(uuid.uuid4())

        # Get lineage context (set by consumer)
        context = get_current_context()

        # Determine lineage fields
        if context:
            parent_id = context.message_id
            corr_id = correlation_id or context.correlation_id
            root_id = context.root_message_id
            reason = causation_reason or context.get_next_causation_reason()
        else:
            # This is a root message
            parent_id = None
            corr_id = correlation_id or str(uuid.uuid4())
            root_id = message_id
            reason = causation_reason

        timestamp = datetime.now(timezone.utc)

        # Build lineage headers
        lineage_headers = {
            LineageHeaders.MESSAGE_ID: message_id,
            LineageHeaders.CORRELATION_ID: corr_id,
            LineageHeaders.ROOT_MESSAGE_ID: root_id,
            LineageHeaders.SERVICE_ORIGIN: self.service_name,
            LineageHeaders.RETRY_COUNT: str(retry_count),
            LineageHeaders.TIMESTAMP: timestamp.isoformat(),
        }

        if parent_id:
            lineage_headers[LineageHeaders.PARENT_ID] = parent_id
            lineage_headers[LineageHeaders.CAUSATION_ID] = parent_id

        if reason:
            lineage_headers[LineageHeaders.CAUSATION_REASON] = reason

        if self.tenant_id:
            lineage_headers[LineageHeaders.TENANT_ID] = self.tenant_id

        # Merge with provided headers
        all_headers = {**(headers or {}), **lineage_headers}
        header_list = [(k, v.encode() if isinstance(v, str) else v) for k, v in all_headers.items()]

        # Serialize value
        if isinstance(value, dict):
            value_bytes = json.dumps(value).encode()
        elif isinstance(value, str):
            value_bytes = value.encode()
        else:
            value_bytes = value

        # Serialize key
        key_bytes = None
        if key:
            key_bytes = key.encode() if isinstance(key, str) else key

        # Produce the business message
        self.producer.produce(
            topic=topic,
            value=value_bytes,
            key=key_bytes,
            headers=header_list,
            partition=partition if partition is not None else -1,
            on_delivery=on_delivery,
        )

        # Emit lineage event
        self._emit_lineage_event(
            event_type=EventType.PRODUCED,
            message_id=message_id,
            parent_id=parent_id,
            correlation_id=corr_id,
            root_message_id=root_id,
            causation_reason=reason,
            topic=topic,
            retry_count=retry_count,
            timestamp=timestamp,
            status=status,
            payload=value_bytes.decode() if self.include_payload_preview else None,
        )

        return message_id

    def _emit_lineage_event(
        self,
        event_type: EventType,
        message_id: str,
        parent_id: Optional[str],
        correlation_id: str,
        root_message_id: str,
        causation_reason: Optional[str],
        topic: str,
        retry_count: int,
        timestamp: datetime,
        status: MessageStatus,
        payload: Optional[str],
    ) -> None:
        """Emit a lineage event to the lineage events topic."""
        event = LineageEvent(
            event_type=event_type,
            message_id=message_id,
            correlation_id=correlation_id,
            root_message_id=root_message_id,
            service_origin=self.service_name,
            topic=topic,
            parent_id=parent_id,
            causation_reason=causation_reason,
            retry_count=retry_count,
            timestamp=timestamp,
            status=status,
            tenant_id=self.tenant_id,
        )

        if payload:
            event.with_payload_preview(payload)

        self.lineage_producer.produce(
            topic=LINEAGE_EVENTS_TOPIC,
            value=event.to_json().encode(),
            key=correlation_id.encode(),
        )

    def flush(self, timeout: float = 10.0) -> int:
        """Flush pending messages."""
        remaining = self.producer.flush(timeout)
        self.lineage_producer.flush(timeout)
        return remaining

    def poll(self, timeout: float = 0) -> int:
        """Poll for delivery reports."""
        events = self.producer.poll(timeout)
        self.lineage_producer.poll(0)
        return events

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()
        return False
