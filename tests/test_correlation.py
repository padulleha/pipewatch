"""Tests for pipewatch.correlation and pipewatch.correlation_config."""
from datetime import datetime, timedelta, timezone

import pytest

from pipewatch.correlation import (
    CorrelationAlert,
    CorrelationWindow,
    check_correlation,
)
from pipewatch.correlation_config import (
    CorrelationConfig,
    build_correlation_window,
    parse_correlation_config,
)
from pipewatch.metrics import EvaluationResult


def _ts(offset_seconds: float = 0.0) -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(
        seconds=offset_seconds
    )


def _result(pipeline: str, metric: str = "row_count", status: str = "critical") -> EvaluationResult:
    return EvaluationResult(
        pipeline=pipeline,
        metric_name=metric,
        value=0.0,
        status=status,
        message="",
        threshold=None,
    )


class TestCorrelationWindow:
    def test_empty_window(self):
        w = CorrelationWindow(window_seconds=60)
        assert w.active_pipelines() == []
        assert w.event_count() == 0

    def test_records_event(self):
        w = CorrelationWindow(window_seconds=60)
        w.record("pipe_a", "row_count", ts=_ts())
        assert "pipe_a" in w.active_pipelines(ts=_ts())

    def test_prunes_old_events(self):
        w = CorrelationWindow(window_seconds=60)
        w.record("pipe_a", "row_count", ts=_ts(0))
        assert w.event_count(ts=_ts(120)) == 0

    def test_deduplicates_pipeline_names(self):
        w = CorrelationWindow(window_seconds=60)
        w.record("pipe_a", "m1", ts=_ts(0))
        w.record("pipe_a", "m2", ts=_ts(1))
        assert w.active_pipelines(ts=_ts(2)) == ["pipe_a"]


class TestCheckCorrelation:
    def test_no_alert_below_threshold(self):
        w = CorrelationWindow(window_seconds=60)
        r = _result("pipe_a")
        alert = check_correlation(w, r, min_pipelines=2, ts=_ts())
        assert alert is None

    def test_alert_when_threshold_met(self):
        w = CorrelationWindow(window_seconds=60)
        check_correlation(w, _result("pipe_a"), min_pipelines=2, ts=_ts(0))
        alert = check_correlation(w, _result("pipe_b"), min_pipelines=2, ts=_ts(1))
        assert isinstance(alert, CorrelationAlert)
        assert "pipe_a" in alert.pipelines
        assert "pipe_b" in alert.pipelines

    def test_ok_status_not_recorded(self):
        w = CorrelationWindow(window_seconds=60)
        check_correlation(w, _result("pipe_a", status="ok"), min_pipelines=2, ts=_ts(0))
        check_correlation(w, _result("pipe_b", status="ok"), min_pipelines=2, ts=_ts(1))
        assert w.event_count(ts=_ts(2)) == 0

    def test_summary_contains_pipelines(self):
        alert = CorrelationAlert(
            pipelines=["pipe_a", "pipe_b"],
            metrics=["row_count"],
            event_count=4,
            window_seconds=300,
            detected_at=_ts(),
        )
        s = alert.summary()
        assert "pipe_a" in s
        assert "pipe_b" in s
        assert "CORRELATION" in s


class TestParseCorrelationConfig:
    def test_defaults(self):
        cfg = parse_correlation_config({})
        assert cfg.window_seconds == 300
        assert cfg.min_pipelines == 2
        assert cfg.enabled is True

    def test_custom_values(self):
        cfg = parse_correlation_config(
            {"correlation": {"window_seconds": 120, "min_pipelines": 3, "enabled": False}}
        )
        assert cfg.window_seconds == 120
        assert cfg.min_pipelines == 3
        assert cfg.enabled is False

    def test_invalid_window_raises(self):
        with pytest.raises(ValueError, match="window_seconds"):
            parse_correlation_config({"correlation": {"window_seconds": 0}})

    def test_invalid_min_pipelines_raises(self):
        with pytest.raises(ValueError, match="min_pipelines"):
            parse_correlation_config({"correlation": {"min_pipelines": 1}})

    def test_build_window_uses_config(self):
        cfg = CorrelationConfig(window_seconds=180)
        w = build_correlation_window(cfg)
        assert w.window_seconds == 180
