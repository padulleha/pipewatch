"""Tests for pipewatch.escalation."""
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from pipewatch.escalation import EscalationPolicy, EscalationRegistry


@pytest.fixture()
def registry() -> EscalationRegistry:
    return EscalationRegistry()


@pytest.fixture()
def policy() -> EscalationPolicy:
    return EscalationPolicy(interval_minutes=30, max_escalations=3, enabled=True)


def test_mark_initial_fire_creates_state(registry, policy):
    state = registry.mark_initial_fire("pipe1", "row_count")
    assert state.count == 1
    assert state.pipeline == "pipe1"
    assert state.metric_name == "row_count"


def test_no_escalation_immediately_after_first_fire(registry, policy):
    registry.mark_initial_fire("pipe1", "row_count")
    result = registry.check_and_escalate("pipe1", "row_count", policy)
    assert result is False


def test_escalation_fires_after_interval(registry, policy):
    state = registry.mark_initial_fire("pipe1", "row_count")
    # Simulate last_fired in the past
    state.last_fired = datetime.utcnow() - timedelta(minutes=31)
    # count still 1 but first_fired != last_fired won't apply; force count > 1
    state.count = 2
    result = registry.check_and_escalate("pipe1", "row_count", policy)
    assert result is True
    assert state.count == 3


def test_no_escalation_before_interval(registry, policy):
    state = registry.mark_initial_fire("pipe1", "row_count")
    state.count = 2
    state.last_fired = datetime.utcnow() - timedelta(minutes=10)
    result = registry.check_and_escalate("pipe1", "row_count", policy)
    assert result is False


def test_max_escalations_respected(registry, policy):
    state = registry.mark_initial_fire("pipe1", "row_count")
    state.count = policy.max_escalations
    state.last_fired = datetime.utcnow() - timedelta(minutes=60)
    result = registry.check_and_escalate("pipe1", "row_count", policy)
    assert result is False


def test_disabled_policy_never_escalates(registry):
    disabled = EscalationPolicy(enabled=False)
    state = registry.mark_initial_fire("pipe1", "row_count")
    state.count = 2
    state.last_fired = datetime.utcnow() - timedelta(minutes=60)
    result = registry.check_and_escalate("pipe1", "row_count", disabled)
    assert result is False


def test_clear_removes_state(registry, policy):
    registry.mark_initial_fire("pipe1", "row_count")
    registry.clear("pipe1", "row_count")
    # After clear, get_or_create returns a fresh state
    state = registry.get_or_create("pipe1", "row_count")
    assert state.count == 1


def test_clear_nonexistent_is_safe(registry):
    registry.clear("nope", "nope")  # should not raise
