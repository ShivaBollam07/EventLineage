#!/usr/bin/env python3
"""
EventLineage Demo Script

Produces a sample chain that demonstrates the key features:
- Normal messages (teal)
- Failed message (red)
- Retry message (amber)

Chain: Order Created -> Payment Requested -> Payment Failed -> Payment Retry -> Notification Sent
"""

import json
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

# Configuration
KAFKA_BOOTSTRAP = "localhost:9092"
LINEAGE_TOPIC = "_lineage.events"

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def emit_lineage_event(
    message_id: str,
    parent_id: str | None,
    correlation_id: str,
    root_message_id: str,
    service: str,
    topic: str,
    causation_reason: str | None,
    retry_count: int = 0,
    status: str = "ok",
):
    """Emit a lineage event to Kafka."""
    event = {
        "eventType": "produced",
        "messageId": message_id,
        "parentId": parent_id,
        "correlationId": correlation_id,
        "rootMessageId": root_message_id,
        "causationReason": causation_reason,
        "serviceOrigin": service,
        "topic": topic,
        "retryCount": retry_count,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "payloadPreview": f"Sample payload for {causation_reason or 'message'}",
    }

    producer.produce(
        topic=LINEAGE_TOPIC,
        key=correlation_id.encode(),
        value=json.dumps(event).encode(),
    )
    producer.flush()

    print(f"  -> {service}: {causation_reason or topic} [{status}]")


def create_demo_chain():
    """Create the demo chain with retry and failure."""
    correlation_id = str(uuid.uuid4())
    print(f"\nCreating demo chain: {correlation_id}\n")
    print("Chain visualization:")
    print("  Order Created (ok)")
    print("      └── Payment Requested (ok)")
    print("              └── Payment Failed (failed)")
    print("                      └── Payment Retry (ok, retry #1)")
    print("                              └── Notification Sent (ok)")
    print()

    # Message 1: Order Created (root)
    msg1_id = str(uuid.uuid4())
    emit_lineage_event(
        message_id=msg1_id,
        parent_id=None,
        correlation_id=correlation_id,
        root_message_id=msg1_id,
        service="order-service",
        topic="orders",
        causation_reason="OrderCreated",
        status="ok",
    )
    time.sleep(0.5)

    # Message 2: Payment Requested
    msg2_id = str(uuid.uuid4())
    emit_lineage_event(
        message_id=msg2_id,
        parent_id=msg1_id,
        correlation_id=correlation_id,
        root_message_id=msg1_id,
        service="payment-service",
        topic="payments",
        causation_reason="PaymentRequested",
        status="ok",
    )
    time.sleep(0.5)

    # Message 3: Payment Failed
    msg3_id = str(uuid.uuid4())
    emit_lineage_event(
        message_id=msg3_id,
        parent_id=msg2_id,
        correlation_id=correlation_id,
        root_message_id=msg1_id,
        service="payment-service",
        topic="payments",
        causation_reason="PaymentFailed",
        status="failed",
    )
    time.sleep(0.5)

    # Message 4: Payment Retry
    msg4_id = str(uuid.uuid4())
    emit_lineage_event(
        message_id=msg4_id,
        parent_id=msg3_id,
        correlation_id=correlation_id,
        root_message_id=msg1_id,
        service="payment-service",
        topic="payments",
        causation_reason="PaymentRetry",
        retry_count=1,
        status="ok",
    )
    time.sleep(0.5)

    # Message 5: Notification Sent
    msg5_id = str(uuid.uuid4())
    emit_lineage_event(
        message_id=msg5_id,
        parent_id=msg4_id,
        correlation_id=correlation_id,
        root_message_id=msg1_id,
        service="notification-service",
        topic="notifications",
        causation_reason="NotificationSent",
        status="ok",
    )

    print(f"\nDemo chain created successfully!")
    print(f"Correlation ID: {correlation_id}")
    print(f"\nView in UI: http://localhost:3000/chains/{correlation_id}")
    print(f"API endpoint: http://localhost:8080/api/v1/chains/{correlation_id}")

    return correlation_id


def create_multiple_chains(count: int = 5):
    """Create multiple demo chains for realistic testing."""
    print(f"\nCreating {count} demo chains...\n")

    correlation_ids = []
    for i in range(count):
        print(f"Chain {i + 1}/{count}:")
        cid = create_demo_chain()
        correlation_ids.append(cid)
        time.sleep(1)

    print(f"\nCreated {count} chains:")
    for cid in correlation_ids:
        print(f"  - {cid}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--multi":
        count = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        create_multiple_chains(count)
    else:
        create_demo_chain()
