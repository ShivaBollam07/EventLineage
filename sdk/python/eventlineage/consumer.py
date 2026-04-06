"""
Lineage-aware Kafka consumer.

Wraps confluent-kafka Consumer to automatically extract lineage headers
and set lineage context for downstream produces.
"""

import json
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, Union

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from .headers import LineageHeaders, LINEAGE_EVENTS_TOPIC
from .event import LineageEvent, EventType, MessageStatus
from .context import LineageContext, set_current_context, clear_current_context


class LineageMessage:
    """
    Wrapper around Kafka message with lineage context.

    Provides easy access to both the message content and lineage metadata.
    """

    def __init__(self, kafka_message: Message, context: LineageContext):
        self._message = kafka_message
        self.context = context

    @property
    def value(self) -> bytes:
        """Raw message value."""
        return self._message.value()

    def value_json(self) -> Any:
        """Parse message value as JSON."""
        return json.loads(self._message.value())

    def value_str(self) -> str:
        """Decode message value as string."""
        return self._message.value().decode()

    @property
    def key(self) -> Optional[bytes]:
        """Message key."""
        return self._message.key()

    def key_str(self) -> Optional[str]:
        """Decode message key as string."""
        k = self._message.key()
        return k.decode() if k else None

    @property
    def topic(self) -> str:
        """Topic name."""
        return self._message.topic()

    @property
    def partition(self) -> int:
        """Partition number."""
        return self._message.partition()

    @property
    def offset(self) -> int:
        """Message offset."""
        return self._message.offset()

    @property
    def timestamp(self) -> tuple:
        """Message timestamp (type, value)."""
        return self._message.timestamp()

    @property
    def headers(self) -> Dict[str, str]:
        """All message headers as dict."""
        hdrs = self._message.headers()
        if not hdrs:
            return {}
        return {k: v.decode() if isinstance(v, bytes) else v for k, v in hdrs}

    # Lineage properties for convenience
    @property
    def message_id(self) -> str:
        return self.context.message_id

    @property
    def correlation_id(self) -> str:
        return self.context.correlation_id

    @property
    def root_message_id(self) -> str:
        return self.context.root_message_id

    @property
    def retry_count(self) -> int:
        return self.context.retry_count


class LineageConsumer:
    """
    A Kafka consumer that automatically tracks message lineage.

    Wraps confluent-kafka Consumer to:
    1. Extract lineage headers from consumed messages
    2. Set lineage context for downstream produces
    3. Emit consume lineage events

    Usage:
        consumer = LineageConsumer(
            bootstrap_servers="localhost:9092",
            group_id="payment-service",
            service_name="payment-service",
            topics=["orders"]
        )

        for msg in consumer:
            process(msg)
            # Any produces here will automatically link to msg
            producer.produce("payments", result)
    """

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        service_name: str,
        topics: Optional[List[str]] = None,
        lineage_bootstrap_servers: Optional[str] = None,
        tenant_id: Optional[str] = None,
        auto_set_context: bool = True,
        emit_consume_events: bool = True,
        **kafka_config
    ):
        """
        Initialize the lineage consumer.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            group_id: Consumer group ID
            service_name: Name of this service
            topics: List of topics to subscribe to
            lineage_bootstrap_servers: Separate Kafka cluster for lineage events
            tenant_id: Optional tenant ID
            auto_set_context: Automatically set lineage context on consume
            emit_consume_events: Emit lineage events for consumed messages
            **kafka_config: Additional confluent-kafka Consumer config
        """
        self.service_name = service_name
        self.tenant_id = tenant_id
        self.auto_set_context = auto_set_context
        self.emit_consume_events = emit_consume_events

        # Consumer config
        config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            **kafka_config
        }
        self.consumer = Consumer(config)

        # Subscribe to topics
        if topics:
            self.consumer.subscribe(topics)

        # Lineage event producer
        if emit_consume_events:
            from confluent_kafka import Producer
            lineage_servers = lineage_bootstrap_servers or bootstrap_servers
            self.lineage_producer = Producer({"bootstrap.servers": lineage_servers})
        else:
            self.lineage_producer = None

    def subscribe(self, topics: List[str]) -> None:
        """Subscribe to topics."""
        self.consumer.subscribe(topics)

    def poll(self, timeout: float = 1.0) -> Optional[LineageMessage]:
        """
        Poll for a single message.

        Returns LineageMessage with context, or None if no message.
        Automatically sets lineage context if auto_set_context is True.
        """
        msg = self.consumer.poll(timeout)

        if msg is None:
            return None

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return None
            raise KafkaException(msg.error())

        return self._process_message(msg)

    def __iter__(self) -> Iterator[LineageMessage]:
        """Iterate over messages."""
        return self

    def __next__(self) -> LineageMessage:
        """Get next message (blocking)."""
        while True:
            msg = self.poll(timeout=1.0)
            if msg is not None:
                return msg

    def _process_message(self, msg: Message) -> LineageMessage:
        """Extract lineage context and wrap message."""
        headers = {}
        if msg.headers():
            headers = {k: v.decode() if isinstance(v, bytes) else v for k, v in msg.headers()}

        # Extract lineage headers
        message_id = headers.get(LineageHeaders.MESSAGE_ID, "")
        correlation_id = headers.get(LineageHeaders.CORRELATION_ID, "")
        root_message_id = headers.get(LineageHeaders.ROOT_MESSAGE_ID, message_id)
        retry_count = int(headers.get(LineageHeaders.RETRY_COUNT, "0"))

        # Create context
        context = LineageContext(
            message_id=message_id,
            correlation_id=correlation_id,
            root_message_id=root_message_id,
            retry_count=retry_count,
        )

        # Set context for downstream produces
        if self.auto_set_context:
            set_current_context(context)

        # Emit consume lineage event
        if self.emit_consume_events and message_id:
            self._emit_consume_event(msg, context, headers)

        return LineageMessage(msg, context)

    def _emit_consume_event(
        self,
        msg: Message,
        context: LineageContext,
        headers: Dict[str, str]
    ) -> None:
        """Emit a consume lineage event."""
        event = LineageEvent(
            event_type=EventType.CONSUMED,
            message_id=context.message_id,
            correlation_id=context.correlation_id,
            root_message_id=context.root_message_id,
            service_origin=self.service_name,
            topic=msg.topic(),
            parent_id=headers.get(LineageHeaders.PARENT_ID),
            causation_reason=headers.get(LineageHeaders.CAUSATION_REASON),
            retry_count=context.retry_count,
            timestamp=datetime.now(timezone.utc),
            status=MessageStatus.OK,
            tenant_id=self.tenant_id,
            partition=msg.partition(),
            offset=msg.offset(),
        )

        self.lineage_producer.produce(
            topic=LINEAGE_EVENTS_TOPIC,
            value=event.to_json().encode(),
            key=context.correlation_id.encode() if context.correlation_id else None,
        )
        self.lineage_producer.poll(0)

    def commit(self, message: Optional[LineageMessage] = None, asynchronous: bool = True) -> None:
        """Commit offsets."""
        if message:
            self.consumer.commit(message=message._message, asynchronous=asynchronous)
        else:
            self.consumer.commit(asynchronous=asynchronous)

    def close(self) -> None:
        """Close the consumer."""
        clear_current_context()
        self.consumer.close()
        if self.lineage_producer:
            self.lineage_producer.flush()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
