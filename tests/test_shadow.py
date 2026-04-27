"""Tests for pipewatch.shadow (shadow-mode middleware)."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from pipewatch.metrics import EvaluationResult, PipelineMetric, ThresholdRule
from pipewatch.shadow import ShadowMiddleware, ShadowResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _metric(name: str = "row_count", value: float = 100.0) -> PipelineMetric:
    return PipelineMetric(
        pipeline="etl_main",
        name=name,
        value=value,
        timestamp=datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc),
    )


def _rule() -> ThresholdRule:
    return ThresholdRule(metric="row_count", warning=50.0, critical=10.0, operator="gte")


def _result(value: float = 100.0, status: str = "ok") -> EvaluationResult:
    return EvaluationResult(
        metric=_metric(value=value),
        rule=_rule(),
        status=status,
        message=f"status is {status}",
    )


def _always_fire_probe(result: EvaluationResult) -> tuple[bool, str]:
    return True, "always fires"


def _never_fire_probe(result: EvaluationResult) -> tuple[bool, str]:
    return False, "always suppressed"


# ---------------------------------------------------------------------------
# ShadowResult
# ---------------------------------------------------------------------------

class TestShadowResult:
    def test_str_would_fire(self):
        sr = ShadowResult(result=_result(), would_fire=True, reason="critical threshold")
        assert "WOULD FIRE" in str(sr)
        assert "etl_main" in str(sr)

    def test_str_suppressed(self):
        sr = ShadowResult(result=_result(), would_fire=False, reason="below threshold")
        assert "suppressed" in str(sr)


# ---------------------------------------------------------------------------
# ShadowMiddleware — basic behaviour
# ---------------------------------------------------------------------------

def test_probe_called_and_logged():
    mw = ShadowMiddleware(probe=_always_fire_probe)
    mw.process(_result())
    assert len(mw.log()) == 1
    assert mw.log()[0].would_fire is True


def test_suppressed_probe_logged():
    mw = ShadowMiddleware(probe=_never_fire_probe)
    mw.process(_result())
    assert mw.log()[0].would_fire is False


def test_stats_counts_correctly():
    mw = ShadowMiddleware(probe=_always_fire_probe)
    for _ in range(3):
        mw.process(_result())
    mw2 = ShadowMiddleware(probe=_never_fire_probe)
    mw2.process(_result())
    # Check mw independently
    s = mw.stats()
    assert s["total"] == 3
    assert s["would_fire"] == 3
    assert s["suppressed"] == 0


def test_stats_suppressed_count():
    mw = ShadowMiddleware(probe=_never_fire_probe)
    mw.process(_result())
    mw.process(_result())
    s = mw.stats()
    assert s["suppressed"] == 2
    assert s["would_fire"] == 0


def test_downstream_called_when_provided():
    downstream = MagicMock()
    mw = ShadowMiddleware(probe=_always_fire_probe, downstream=downstream)
    r = _result()
    mw.process(r)
    downstream.assert_called_once_with(r)


def test_downstream_not_called_when_none():
    # Should not raise; downstream is None
    mw = ShadowMiddleware(probe=_always_fire_probe)
    mw.process(_result())  # no error


def test_disabled_skips_probe_but_calls_downstream():
    downstream = MagicMock()
    probe = MagicMock(return_value=(True, "should not be called"))
    mw = ShadowMiddleware(probe=probe, downstream=downstream, enabled=False)
    mw.process(_result())
    probe.assert_not_called()
    downstream.assert_called_once()
    assert mw.stats()["total"] == 0


def test_reset_stats_clears_log_and_counters():
    mw = ShadowMiddleware(probe=_always_fire_probe)
    mw.process(_result())
    mw.process(_result())
    mw.reset_stats()
    assert mw.stats()["total"] == 0
    assert mw.log() == []


def test_log_returns_copy():
    mw = ShadowMiddleware(probe=_always_fire_probe)
    mw.process(_result())
    log_copy = mw.log()
    log_copy.clear()
    assert len(mw.log()) == 1  # original unaffected
