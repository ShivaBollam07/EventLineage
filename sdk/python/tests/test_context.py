"""Tests for LineageContext and context management utilities."""

import pytest

from eventlineage.context import (
    LineageContext,
    get_current_context,
    set_current_context,
    clear_current_context,
    lineage_scope,
    _current_context,
)


def _make_context(**overrides) -> LineageContext:
    """Helper to create a LineageContext with sensible defaults."""
    defaults = dict(
        message_id="msg-001",
        correlation_id="corr-001",
        root_message_id="root-001",
    )
    defaults.update(overrides)
    return LineageContext(**defaults)


@pytest.fixture(autouse=True)
def _clean_context():
    """Ensure context is cleared before and after each test."""
    clear_current_context()
    yield
    clear_current_context()


class TestGetCurrentContext:
    """Tests for get_current_context()."""

    def test_returns_none_by_default(self):
        assert get_current_context() is None


class TestSetAndGetContext:
    """Tests for set_current_context() and get_current_context() roundtrip."""

    def test_roundtrip(self):
        ctx = _make_context()
        set_current_context(ctx)
        retrieved = get_current_context()
        assert retrieved is ctx
        assert retrieved.message_id == "msg-001"
        assert retrieved.correlation_id == "corr-001"
        assert retrieved.root_message_id == "root-001"

    def test_overwrite_context(self):
        ctx1 = _make_context(message_id="first")
        ctx2 = _make_context(message_id="second")
        set_current_context(ctx1)
        set_current_context(ctx2)
        assert get_current_context().message_id == "second"


class TestClearCurrentContext:
    """Tests for clear_current_context()."""

    def test_clears_context(self):
        ctx = _make_context()
        set_current_context(ctx)
        assert get_current_context() is not None
        clear_current_context()
        assert get_current_context() is None

    def test_clear_when_already_none(self):
        # Should not raise
        clear_current_context()
        assert get_current_context() is None


class TestLineageScope:
    """Tests for lineage_scope context manager."""

    def test_sets_context_inside_scope(self):
        ctx = _make_context(message_id="scoped")
        with lineage_scope(ctx):
            current = get_current_context()
            assert current is ctx
            assert current.message_id == "scoped"

    def test_resets_context_after_scope(self):
        ctx = _make_context(message_id="scoped")
        with lineage_scope(ctx):
            pass
        assert get_current_context() is None

    def test_restores_previous_context_after_scope(self):
        outer = _make_context(message_id="outer")
        inner = _make_context(message_id="inner")
        set_current_context(outer)
        with lineage_scope(inner):
            assert get_current_context().message_id == "inner"
        # After the scope exits, the outer context is restored
        assert get_current_context().message_id == "outer"

    def test_nested_scopes(self):
        ctx1 = _make_context(message_id="level-1")
        ctx2 = _make_context(message_id="level-2")
        with lineage_scope(ctx1):
            assert get_current_context().message_id == "level-1"
            with lineage_scope(ctx2):
                assert get_current_context().message_id == "level-2"
            assert get_current_context().message_id == "level-1"
        assert get_current_context() is None

    def test_scope_returns_context(self):
        ctx = _make_context()
        with lineage_scope(ctx) as returned:
            assert returned is ctx

    def test_scope_resets_even_on_exception(self):
        ctx = _make_context()
        with pytest.raises(ValueError):
            with lineage_scope(ctx):
                raise ValueError("boom")
        assert get_current_context() is None


class TestGetNextCausationReason:
    """Tests for LineageContext.get_next_causation_reason()."""

    def test_returns_set_reason_and_clears(self):
        ctx = _make_context(causation_reason="OrderCreated")
        reason = ctx.get_next_causation_reason()
        assert reason == "OrderCreated"
        # Should be cleared now
        assert ctx.causation_reason is None

    def test_returns_default_when_no_reason(self):
        ctx = _make_context(causation_reason=None)
        reason = ctx.get_next_causation_reason(default="Fallback")
        assert reason == "Fallback"

    def test_returns_none_when_no_reason_and_no_default(self):
        ctx = _make_context(causation_reason=None)
        reason = ctx.get_next_causation_reason()
        assert reason is None

    def test_second_call_returns_default_or_none(self):
        ctx = _make_context(causation_reason="First")
        assert ctx.get_next_causation_reason() == "First"
        assert ctx.get_next_causation_reason() is None
        assert ctx.get_next_causation_reason(default="D") == "D"


class TestLineageContextDataclass:
    """Tests for LineageContext defaults."""

    def test_defaults(self):
        ctx = LineageContext(
            message_id="m",
            correlation_id="c",
            root_message_id="r",
        )
        assert ctx.causation_reason is None
        assert ctx.retry_count == 0

    def test_all_fields(self):
        ctx = LineageContext(
            message_id="m",
            correlation_id="c",
            root_message_id="r",
            causation_reason="reason",
            retry_count=3,
        )
        assert ctx.causation_reason == "reason"
        assert ctx.retry_count == 3
