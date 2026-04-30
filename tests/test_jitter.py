"""Tests for pipewatch.jitter."""

from __future__ import annotations

import random
from unittest.mock import MagicMock

import pytest

from pipewatch.jitter import JitterMiddleware, JitterPolicy
from pipewatch.metrics import EvaluationResult, PipelineMetric, ThresholdRule


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _metric() -> PipelineMetric:
    return PipelineMetric(pipeline="p", name="rows", value=10.0)


def _rule() -> ThresholdRule:
    return ThresholdRule(metric="rows", warning=5.0, critical=20.0)


def _result() -> EvaluationResult:
    return EvaluationResult(metric=_metric(), rule=_rule(), status="warning", message="warn")


def _no_sleep(seconds: float) -> None:  # noqa: ARG001
    pass


# ---------------------------------------------------------------------------
# JitterPolicy
# ---------------------------------------------------------------------------

def test_policy_defaults():
    p = JitterPolicy()
    assert p.min_seconds == 0.0
    assert p.max_seconds == 1.0


def test_policy_invalid_negative_min():
    with pytest.raises(ValueError, match="min_seconds"):
        JitterPolicy(min_seconds=-0.1)


def test_policy_invalid_max_less_than_min():
    with pytest.raises(ValueError, match="max_seconds"):
        JitterPolicy(min_seconds=2.0, max_seconds=1.0)


def test_policy_sample_within_range():
    rng = random.Random(42)
    p = JitterPolicy(min_seconds=0.5, max_seconds=1.5)
    for _ in range(50):
        d = p.sample(rng)
        assert 0.5 <= d <= 1.5


def test_policy_zero_range_returns_fixed():
    p = JitterPolicy(min_seconds=0.3, max_seconds=0.3)
    assert p.sample() == pytest.approx(0.3)


# ---------------------------------------------------------------------------
# JitterMiddleware
# ---------------------------------------------------------------------------

def test_downstream_called():
    downstream = MagicMock()
    mw = JitterMiddleware(downstream, JitterPolicy(max_seconds=0.0), sleep_fn=_no_sleep)
    mw.process(_result())
    downstream.assert_called_once()


def test_sleep_called_with_delay():
    slept: list[float] = []
    rng = random.Random(0)
    policy = JitterPolicy(min_seconds=0.2, max_seconds=0.5)
    mw = JitterMiddleware(lambda _: None, policy, sleep_fn=slept.append, rng=rng)
    mw.process(_result())
    assert len(slept) == 1
    assert 0.2 <= slept[0] <= 0.5


def test_no_sleep_when_zero_range():
    slept: list[float] = []
    policy = JitterPolicy(min_seconds=0.0, max_seconds=0.0)
    mw = JitterMiddleware(lambda _: None, policy, sleep_fn=slept.append)
    mw.process(_result())
    # sleep(0) is still called but delay is 0
    assert slept == [0.0]


def test_stats_accumulate():
    rng = random.Random(7)
    policy = JitterPolicy(min_seconds=0.1, max_seconds=0.2)
    mw = JitterMiddleware(lambda _: None, policy, sleep_fn=_no_sleep, rng=rng)
    for _ in range(5):
        mw.process(_result())
    s = mw.stats()
    assert s["total"] == 5
    assert s["total_delay_seconds"] > 0
    assert 0.1 <= s["average_delay_seconds"] <= 0.2


def test_reset_stats():
    mw = JitterMiddleware(lambda _: None, JitterPolicy(max_seconds=0.0), sleep_fn=_no_sleep)
    mw.process(_result())
    mw.reset_stats()
    assert mw.stats()["total"] == 0


def test_set_policy_replaces():
    slept: list[float] = []
    mw = JitterMiddleware(lambda _: None, JitterPolicy(max_seconds=0.0), sleep_fn=slept.append)
    mw.set_policy(JitterPolicy(min_seconds=1.0, max_seconds=1.0))
    mw.process(_result())
    assert slept == [1.0]
