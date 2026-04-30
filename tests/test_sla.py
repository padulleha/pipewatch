"""Tests for SLA tracking, config parsing, and middleware."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from pipewatch.metrics import EvaluationResult, PipelineMetric, ThresholdRule
from pipewatch.sla import SLAPolicy, SLATracker, SLAViolation
from pipewatch.sla_config import build_sla_tracker_from_config, parse_sla_policies
from pipewatch.sla_middleware import SLAMiddleware


def _metric(pipeline: str = "pipe", name: str = "row_count", value: float = 10.0) -> PipelineMetric:
    return PipelineMetric(pipeline=pipeline, name=name, value=value)


def _result(status: str = "ok", pipeline: str = "pipe", metric: str = "row_count") -> EvaluationResult:
    m = _metric(pipeline=pipeline, name=metric)
    rule = ThresholdRule(metric=metric, warning_above=50.0, critical_above=100.0)
    return EvaluationResult(metric=m, rule=rule, status=status)


# --- SLAPolicy ---

def test_policy_defaults():
    p = SLAPolicy(pipeline="p", metric="m")
    assert p.max_critical_rate == 0.05
    assert p.max_warning_rate == 0.20
    assert p.window_minutes == 60


def test_policy_invalid_critical_rate():
    with pytest.raises(ValueError):
        SLAPolicy(pipeline="p", metric="m", max_critical_rate=1.5)


def test_policy_invalid_window():
    with pytest.raises(ValueError):
        SLAPolicy(pipeline="p", metric="m", window_minutes=0)


# --- SLATracker ---

def test_no_policy_returns_none():
    tracker = SLATracker()
    tracker.record(_result("critical"))
    assert tracker.check("pipe", "row_count") is None


def test_no_violation_when_within_bounds():
    tracker = SLATracker()
    policy = SLAPolicy(pipeline="pipe", metric="row_count", max_critical_rate=0.5)
    tracker.add_policy(policy)
    for _ in range(8):
        tracker.record(_result("ok"))
    tracker.record(_result("critical"))
    assert tracker.check("pipe", "row_count") is None


def test_violation_when_critical_rate_exceeded():
    tracker = SLATracker()
    policy = SLAPolicy(pipeline="pipe", metric="row_count", max_critical_rate=0.1)
    tracker.add_policy(policy)
    for _ in range(5):
        tracker.record(_result("critical"))
    for _ in range(5):
        tracker.record(_result("ok"))
    violation = tracker.check("pipe", "row_count")
    assert violation is not None
    assert violation.critical_rate == pytest.approx(0.5)
    assert "critical rate" in violation.summary()


# --- SLAViolation.summary ---

def test_summary_no_violation():
    policy = SLAPolicy(pipeline="p", metric="m", max_critical_rate=0.5, max_warning_rate=0.5)
    v = SLAViolation(pipeline="p", metric="m", critical_rate=0.1, warning_rate=0.1, policy=policy)
    assert v.summary() == "no violation"


# --- parse_sla_policies ---

def test_parse_empty_config():
    assert parse_sla_policies({}) == []


def test_parse_basic_entry():
    cfg = {"sla": [{"pipeline": "etl", "metric": "lag", "max_critical_rate": 0.02, "window_minutes": 30}]}
    policies = parse_sla_policies(cfg)
    assert len(policies) == 1
    assert policies[0].pipeline == "etl"
    assert policies[0].max_critical_rate == pytest.approx(0.02)
    assert policies[0].window_minutes == 30


def test_parse_skips_missing_pipeline():
    cfg = {"sla": [{"metric": "lag"}]}
    assert parse_sla_policies(cfg) == []


def test_build_tracker_from_config():
    cfg = {"sla": [{"pipeline": "p", "metric": "m"}]}
    tracker = build_sla_tracker_from_config(cfg)
    assert isinstance(tracker, SLATracker)


# --- SLAMiddleware ---

def test_middleware_calls_downstream():
    downstream = MagicMock()
    tracker = SLATracker()
    mw = SLAMiddleware(tracker=tracker, downstream=downstream)
    r = _result("ok")
    mw.process(r)
    downstream.assert_called_once_with(r)


def test_middleware_fires_callback_on_violation():
    callback = MagicMock()
    tracker = SLATracker()
    policy = SLAPolicy(pipeline="pipe", metric="row_count", max_critical_rate=0.0)
    tracker.add_policy(policy)
    mw = SLAMiddleware(tracker=tracker, on_violation=callback)
    mw.process(_result("critical"))
    callback.assert_called_once()
    assert mw.stats()["violations_fired"] == 1


def test_middleware_stats_reset():
    tracker = SLATracker()
    mw = SLAMiddleware(tracker=tracker)
    mw.process(_result("ok"))
    assert mw.stats()["processed"] == 1
    mw.reset_stats()
    assert mw.stats()["processed"] == 0
