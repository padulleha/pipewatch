"""Tests for WindowMiddleware."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pipewatch.metrics import PipelineMetric, ThresholdRule, EvaluationResult
from pipewatch.window import WindowPolicy
from pipewatch.window_middleware import WindowMiddleware


def _metric(value: float = 10.0) -> PipelineMetric:
    return PipelineMetric(pipeline="pipe1", name="row_count", value=value)


def _rule() -> ThresholdRule:
    return ThresholdRule(metric="row_count", warning=5.0, critical=1.0)


def _result(value: float = 10.0) -> EvaluationResult:
    return EvaluationResult(metric=_metric(value), rule=_rule(), status="warning", triggered_value=value)


def _make_middleware(min_events: int = 1) -> tuple[WindowMiddleware, MagicMock]:
    downstream = MagicMock()
    policy = WindowPolicy(min_events=min_events)
    mw = WindowMiddleware(downstream=downstream, default_policy=policy)
    return mw, downstream


def test_forwards_when_min_events_met():
    mw, downstream = _make_middleware(min_events=1)
    mw.process(_result())
    downstream.assert_called_once()


def test_suppresses_when_below_min_events():
    mw, downstream = _make_middleware(min_events=3)
    mw.process(_result())
    mw.process(_result())
    downstream.assert_not_called()


def test_forwards_once_threshold_reached():
    mw, downstream = _make_middleware(min_events=3)
    for _ in range(3):
        mw.process(_result())
    assert downstream.call_count == 1


def test_stats_forwarded_and_suppressed():
    mw, _ = _make_middleware(min_events=2)
    mw.process(_result())  # suppressed
    mw.process(_result())  # forwarded
    mw.process(_result())  # forwarded
    s = mw.stats()
    assert s["suppressed"] == 1
    assert s["forwarded"] == 2


def test_reset_stats():
    mw, _ = _make_middleware(min_events=1)
    mw.process(_result())
    mw.reset_stats()
    assert mw.stats() == {"forwarded": 0, "suppressed": 0}


def test_set_policy_overrides_default():
    mw, downstream = _make_middleware(min_events=1)
    new_policy = WindowPolicy(min_events=5)
    mw.set_policy("pipe1", "row_count", new_policy)
    for _ in range(4):
        mw.process(_result())
    downstream.assert_not_called()
