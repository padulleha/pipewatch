"""Tests for pipewatch.window and WindowPolicy."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from pipewatch.metrics import PipelineMetric, ThresholdRule, EvaluationResult
from pipewatch.window import WindowPolicy, WindowRegistry, WindowState, _WindowEntry


def _metric(value: float = 10.0) -> PipelineMetric:
    return PipelineMetric(pipeline="pipe1", name="row_count", value=value)


def _rule() -> ThresholdRule:
    return ThresholdRule(metric="row_count", warning=5.0, critical=1.0)


def _result(value: float = 10.0) -> EvaluationResult:
    return EvaluationResult(metric=_metric(value), rule=_rule(), status="ok", triggered_value=value)


# --- WindowPolicy validation ---

def test_policy_defaults():
    p = WindowPolicy()
    assert p.window_seconds == 300
    assert p.min_events == 1
    assert p.aggregate == "count"


def test_policy_invalid_window():
    with pytest.raises(ValueError, match="window_seconds"):
        WindowPolicy(window_seconds=0)


def test_policy_invalid_min_events():
    with pytest.raises(ValueError, match="min_events"):
        WindowPolicy(min_events=0)


def test_policy_invalid_aggregate():
    with pytest.raises(ValueError, match="aggregate"):
        WindowPolicy(aggregate="median")


# --- WindowState aggregation ---

def test_aggregate_count():
    state = WindowState(policy=WindowPolicy(aggregate="count"))
    for v in [1.0, 2.0, 3.0]:
        state.record(_result(v))
    assert state.aggregate_value() == 3.0


def test_aggregate_avg():
    state = WindowState(policy=WindowPolicy(aggregate="avg"))
    for v in [10.0, 20.0]:
        state.record(_result(v))
    assert state.aggregate_value() == 15.0


def test_aggregate_max():
    state = WindowState(policy=WindowPolicy(aggregate="max"))
    for v in [5.0, 99.0, 3.0]:
        state.record(_result(v))
    assert state.aggregate_value() == 99.0


def test_aggregate_min():
    state = WindowState(policy=WindowPolicy(aggregate="min"))
    for v in [5.0, 99.0, 3.0]:
        state.record(_result(v))
    assert state.aggregate_value() == 3.0


def test_aggregate_empty_returns_none():
    state = WindowState(policy=WindowPolicy())
    assert state.aggregate_value() is None


def test_prune_removes_old_entries():
    policy = WindowPolicy(window_seconds=60)
    state = WindowState(policy=policy)
    state.record(_result())
    # manually age the entry
    old_ts = datetime.now(timezone.utc) - timedelta(seconds=120)
    state._entries[0] = _WindowEntry(result=_result(), ts=old_ts)
    assert state.aggregate_value() is None


def test_has_min_events_true():
    policy = WindowPolicy(min_events=2)
    state = WindowState(policy=policy)
    state.record(_result())
    assert not state.has_min_events()
    state.record(_result())
    assert state.has_min_events()


# --- WindowRegistry ---

def test_registry_creates_state():
    reg = WindowRegistry()
    policy = WindowPolicy()
    s = reg.get_or_create("pipe1", "metric1", policy)
    assert isinstance(s, WindowState)


def test_registry_returns_same_state():
    reg = WindowRegistry()
    policy = WindowPolicy()
    s1 = reg.get_or_create("pipe1", "metric1", policy)
    s2 = reg.get_or_create("pipe1", "metric1", policy)
    assert s1 is s2


def test_registry_reset_removes_state():
    reg = WindowRegistry()
    policy = WindowPolicy()
    reg.get_or_create("pipe1", "metric1", policy)
    reg.reset("pipe1", "metric1")
    s2 = reg.get_or_create("pipe1", "metric1", policy)
    assert len(s2._entries) == 0
