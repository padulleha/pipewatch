"""Tests for SamplingMiddleware."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pipewatch.metrics import EvaluationResult, PipelineMetric, ThresholdRule
from pipewatch.sampling import SamplingPolicy
from pipewatch.sampling_middleware import SamplingMiddleware


def _metric(pipeline: str = "pipe", name: str = "row_count", value: float = 100.0) -> PipelineMetric:
    return PipelineMetric(pipeline=pipeline, name=name, value=value)


def _rule() -> ThresholdRule:
    return ThresholdRule(metric="row_count", warning=50.0, critical=10.0)


def _result(pipeline: str = "pipe", name: str = "row_count") -> EvaluationResult:
    m = _metric(pipeline=pipeline, name=name)
    r = _rule()
    return EvaluationResult(metric=m, rule=r, status="ok", triggered_threshold=None)


# ---------------------------------------------------------------------------

def test_always_policy_forwards_all():
    downstream = MagicMock()
    mw = SamplingMiddleware(downstream, default_policy=SamplingPolicy(strategy="always"))
    for _ in range(5):
        mw.process(_result())
    assert downstream.call_count == 5


def test_interval_policy_drops_correctly():
    downstream = MagicMock()
    policy = SamplingPolicy(strategy="interval", every_n=3)
    mw = SamplingMiddleware(downstream, default_policy=policy)
    for _ in range(9):
        mw.process(_result())
    assert downstream.call_count == 3


def test_pipeline_specific_policy_overrides_default():
    downstream = MagicMock()
    default = SamplingPolicy(strategy="always")
    override = SamplingPolicy(strategy="interval", every_n=2)
    mw = SamplingMiddleware(downstream, default_policy=default)
    mw.set_policy("pipe", override)
    for _ in range(4):
        mw.process(_result(pipeline="pipe"))
    # every_n=2 → calls 1 and 3 pass → 2 out of 4
    assert downstream.call_count == 2


def test_different_pipelines_use_own_policies():
    downstream = MagicMock()
    mw = SamplingMiddleware(downstream, default_policy=SamplingPolicy(strategy="always"))
    mw.set_policy("slow", SamplingPolicy(strategy="interval", every_n=10))
    for _ in range(5):
        mw.process(_result(pipeline="fast"))  # always → 5 forwarded
    for _ in range(10):
        mw.process(_result(pipeline="slow"))  # every 10 → 1 forwarded
    assert downstream.call_count == 6


def test_stats_accurate():
    downstream = MagicMock()
    policy = SamplingPolicy(strategy="interval", every_n=2)
    mw = SamplingMiddleware(downstream, default_policy=policy)
    for _ in range(6):
        mw.process(_result())
    s = mw.stats()
    assert s["total"] == 6
    assert s["sampled"] == 3
    assert s["dropped"] == 3


def test_reset_stats_clears_counters():
    downstream = MagicMock()
    mw = SamplingMiddleware(downstream)
    mw.process(_result())
    mw.reset_stats()
    s = mw.stats()
    assert s == {"total": 0, "sampled": 0, "dropped": 0}
