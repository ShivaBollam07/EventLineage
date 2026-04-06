"""
EventLineage Python SDK

A Kafka instrumentation SDK for automatic message lineage tracking.
Wraps confluent-kafka to inject lineage headers and emit lineage events.
"""

from .headers import LineageHeaders, LINEAGE_EVENTS_TOPIC
from .event import LineageEvent, EventType, MessageStatus
from .context import (
    LineageContext,
    get_current_context,
    set_current_context,
    clear_current_context,
    lineage_scope,
    async_lineage_scope,
)
from .producer import LineageProducer
from .consumer import LineageConsumer, LineageMessage

__version__ = "1.0.0"
__all__ = [
    "LineageHeaders",
    "LINEAGE_EVENTS_TOPIC",
    "LineageEvent",
    "EventType",
    "MessageStatus",
    "LineageContext",
    "get_current_context",
    "set_current_context",
    "clear_current_context",
    "lineage_scope",
    "async_lineage_scope",
    "LineageProducer",
    "LineageConsumer",
    "LineageMessage",
]
