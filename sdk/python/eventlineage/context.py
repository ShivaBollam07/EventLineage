"""
Async-safe lineage context using contextvars.

Works correctly with both sync and async code (FastAPI, asyncio, Celery).
Thread-local storage would break with async frameworks.
"""

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Optional


@dataclass
class LineageContext:
    """
    Holds lineage context for the current execution scope.

    When a message is consumed, this context is set with the consumed
    message's details. Subsequent produces will automatically link
    to this parent message.
    """

    message_id: str
    correlation_id: str
    root_message_id: str
    causation_reason: Optional[str] = None
    retry_count: int = 0

    def get_next_causation_reason(self, default: Optional[str] = None) -> Optional[str]:
        """Get causation reason for next message, then clear it."""
        reason = self.causation_reason or default
        self.causation_reason = None
        return reason


# ContextVar for async-safe context storage
# Works correctly with asyncio, threads, and greenlets
_current_context: ContextVar[Optional[LineageContext]] = ContextVar(
    "lineage_context",
    default=None
)


def get_current_context() -> Optional[LineageContext]:
    """
    Get the current lineage context.

    Returns None if no context is set (e.g., for root messages).
    """
    return _current_context.get()


def set_current_context(ctx: LineageContext) -> None:
    """
    Set the lineage context for the current execution scope.

    This is typically called by the consumer when a message is received.
    """
    _current_context.set(ctx)


def clear_current_context() -> None:
    """
    Clear the current lineage context.

    Call this after processing is complete to prevent context leakage.
    """
    _current_context.set(None)


class lineage_scope:
    """
    Context manager for lineage scope.

    Usage:
        with lineage_scope(context):
            # produce messages here, they'll be linked to context
            producer.produce(...)
    """

    def __init__(self, context: LineageContext):
        self.context = context
        self.token = None

    def __enter__(self):
        self.token = _current_context.set(self.context)
        return self.context

    def __exit__(self, exc_type, exc_val, exc_tb):
        _current_context.reset(self.token)
        return False


async def async_lineage_scope(context: LineageContext):
    """
    Async context manager for lineage scope.

    Usage:
        async with async_lineage_scope(context):
            await producer.produce_async(...)
    """
    token = _current_context.set(context)
    try:
        yield context
    finally:
        _current_context.reset(token)
