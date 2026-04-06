"""
LineageEvent data class for representing lineage events.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Optional
import json

from .headers import DEFAULT_PAYLOAD_PREVIEW_MAX_LENGTH


class EventType(str, Enum):
    """Type of lineage event."""
    PRODUCED = "produced"
    CONSUMED = "consumed"


class MessageStatus(str, Enum):
    """Processing status of a message."""
    OK = "ok"
    FAILED = "failed"
    PENDING = "pending"


@dataclass
class LineageEvent:
    """
    Represents a lineage event to be published to the lineage events topic.

    This class mirrors the Go backend's envelope.LineageEvent struct.
    """

    # Required fields
    event_type: EventType
    message_id: str
    correlation_id: str
    root_message_id: str
    service_origin: str
    topic: str

    # Optional fields
    parent_id: Optional[str] = None
    causation_reason: Optional[str] = None
    retry_count: int = 0
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: MessageStatus = MessageStatus.OK
    payload_preview: Optional[str] = None
    tenant_id: Optional[str] = None
    partition: Optional[int] = None
    offset: Optional[int] = None

    def with_parent(self, parent_id: str, reason: Optional[str] = None) -> "LineageEvent":
        """Set parent message ID and optional causation reason."""
        self.parent_id = parent_id
        if reason:
            self.causation_reason = reason
        return self

    def with_retry(self, count: int) -> "LineageEvent":
        """Set retry count."""
        self.retry_count = count
        return self

    def with_status(self, status: MessageStatus) -> "LineageEvent":
        """Set message status."""
        self.status = status
        return self

    def with_payload_preview(self, payload: str) -> "LineageEvent":
        """Set truncated payload preview."""
        if len(payload) > DEFAULT_PAYLOAD_PREVIEW_MAX_LENGTH:
            self.payload_preview = payload[:DEFAULT_PAYLOAD_PREVIEW_MAX_LENGTH] + "..."
        else:
            self.payload_preview = payload
        return self

    def is_root_message(self) -> bool:
        """Check if this is a root message (chain originator)."""
        return not self.parent_id or self.message_id == self.root_message_id

    def is_retry(self) -> bool:
        """Check if this is a retry attempt."""
        return self.retry_count > 0

    def is_failed(self) -> bool:
        """Check if this message has failed status."""
        return self.status == MessageStatus.FAILED

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "eventType": self.event_type.value,
            "messageId": self.message_id,
            "parentId": self.parent_id,
            "correlationId": self.correlation_id,
            "rootMessageId": self.root_message_id,
            "causationReason": self.causation_reason,
            "serviceOrigin": self.service_origin,
            "topic": self.topic,
            "retryCount": self.retry_count,
            "timestamp": self.timestamp.isoformat(),
            "status": self.status.value,
            "payloadPreview": self.payload_preview,
            "tenantId": self.tenant_id,
            "partition": self.partition,
            "offset": self.offset,
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict) -> "LineageEvent":
        """Create LineageEvent from dictionary."""
        return cls(
            event_type=EventType(data["eventType"]),
            message_id=data["messageId"],
            correlation_id=data["correlationId"],
            root_message_id=data["rootMessageId"],
            service_origin=data["serviceOrigin"],
            topic=data["topic"],
            parent_id=data.get("parentId"),
            causation_reason=data.get("causationReason"),
            retry_count=data.get("retryCount", 0),
            timestamp=datetime.fromisoformat(data["timestamp"]) if data.get("timestamp") else datetime.now(timezone.utc),
            status=MessageStatus(data.get("status", "ok")),
            payload_preview=data.get("payloadPreview"),
            tenant_id=data.get("tenantId"),
            partition=data.get("partition"),
            offset=data.get("offset"),
        )
