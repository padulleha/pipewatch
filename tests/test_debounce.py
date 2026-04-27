"""Tests for debounce policy, registry, and middleware."""
from __future__ import annotations

import pytest

from pipewatch.debounce import DebouncePolicy, DebounceRegistry
from pipewatch.debounce_config import debounce_policy_for, parse_debounce_policy
from pipewatch.debounce_middleware import DebounceMiddleware
from pipewatch.metrics import EvaluationResult, PipelineMetric, ThresholdRule


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _metric(name: str = "row_count", pipeline: str = "etl") -> PipelineMetric:
    return PipelineMetric(pipeline=pipeline, name=name, value=10.0)


def _rule() -> ThresholdRule:
    return ThresholdRule(metric="row_count", warning=5.0, critical=1.0)


def _result(status: str, pipeline: str = "etl", name: str = "row_count") -> EvaluationResult:
    return EvaluationResult(metric=_metric(name=name, pipeline=pipeline), rule=_rule(), status=status)


# ---------------------------------------------------------------------------
# DebouncePolicy
# ---------------------------------------------------------------------------

def test_policy_default():
    p = DebouncePolicy()
    assert p.min_consecutive == 2


def test_policy_invalid_min_consecutive():
    with pytest.raises(ValueError):
        DebouncePolicy(min_consecutive=0)


# ---------------------------------------------------------------------------
# DebounceRegistry
# ---------------------------------------------------------------------------

def test_ok_result_never_fires():
    reg = DebounceRegistry()
    policy = DebouncePolicy(min_consecutive=1)
    assert reg.should_fire(_result("ok"), policy) is False


def test_single_warning_below_threshold():
    reg = DebounceRegistry()
    policy = DebouncePolicy(min_consecutive=2)
    assert reg.should_fire(_result("warning"), policy) is False


def test_consecutive_warnings_reach_threshold():
    reg = DebounceRegistry()
    policy = DebouncePolicy(min_consecutive=2)
    reg.should_fire(_result("warning"), policy)
    assert reg.should_fire(_result("warning"), policy) is True


def test_status_change_resets_consecutive():
    reg = DebounceRegistry()
    policy = DebouncePolicy(min_consecutive=2)
    reg.should_fire(_result("warning"), policy)
    reg.should_fire(_result("critical"), policy)  # status changed → resets to 1
    assert reg.consecutive_count("etl", "row_count") == 1


def test_ok_resets_consecutive():
    reg = DebounceRegistry()
    policy = DebouncePolicy(min_consecutive=2)
    reg.should_fire(_result("warning"), policy)
    reg.should_fire(_result("ok"), policy)
    assert reg.consecutive_count("etl", "row_count") == 0


def test_reset_clears_state():
    reg = DebounceRegistry()
    policy = DebouncePolicy(min_consecutive=3)
    reg.should_fire(_result("warning"), policy)
    reg.reset("etl", "row_count")
    assert reg.consecutive_count("etl", "row_count") == 0


# ---------------------------------------------------------------------------
# parse_debounce_policy / debounce_policy_for
# ---------------------------------------------------------------------------

def test_parse_default_when_empty():
    p = parse_debounce_policy({})
    assert p.min_consecutive == 2


def test_parse_explicit_value():
    p = parse_debounce_policy({"debounce": {"min_consecutive": 4}})
    assert p.min_consecutive == 4


def test_debounce_policy_for_pipeline_override():
    cfg = {
        "debounce": {"min_consecutive": 2},
        "pipelines": {"etl": {"debounce": {"min_consecutive": 5}}},
    }
    p = debounce_policy_for("etl", cfg)
    assert p.min_consecutive == 5


def test_debounce_policy_for_falls_back_to_global():
    cfg = {"debounce": {"min_consecutive": 3}}
    p = debounce_policy_for("missing_pipeline", cfg)
    assert p.min_consecutive == 3


# ---------------------------------------------------------------------------
# DebounceMiddleware
# ---------------------------------------------------------------------------

def test_middleware_suppresses_until_threshold():
    received = []
    mw = DebounceMiddleware(downstream=received.append, default_policy=DebouncePolicy(min_consecutive=3))
    mw.process(_result("warning"))
    mw.process(_result("warning"))
    assert len(received) == 0
    mw.process(_result("warning"))
    assert len(received) == 1


def test_middleware_stats():
    received = []
    mw = DebounceMiddleware(downstream=received.append, default_policy=DebouncePolicy(min_consecutive=2))
    mw.process(_result("warning"))
    mw.process(_result("warning"))
    s = mw.stats()
    assert s["suppressed"] == 1
    assert s["forwarded"] == 1


def test_middleware_reset_stats():
    received = []
    mw = DebounceMiddleware(downstream=received.append, default_policy=DebouncePolicy(min_consecutive=1))
    mw.process(_result("warning"))
    mw.reset_stats()
    assert mw.stats() == {"forwarded": 0, "suppressed": 0}


def test_middleware_per_pipeline_policy():
    received = []
    mw = DebounceMiddleware(downstream=received.append, default_policy=DebouncePolicy(min_consecutive=5))
    mw.set_policy("etl", DebouncePolicy(min_consecutive=1))
    mw.process(_result("critical", pipeline="etl"))
    assert len(received) == 1
