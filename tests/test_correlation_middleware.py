"""Tests for pipewatch.correlation_middleware."""
from datetime import datetime, timezone
from typing import List
from unittest.mock import MagicMock

from pipewatch.alerts import AlertEvent
from pipewatch.correlation import CorrelationWindow
from pipewatch.correlation_config import CorrelationConfig
from pipewatch.correlation_middleware import CorrelationMiddleware
from pipewatch.metrics import EvaluationResult


def _result(pipeline: str, status: str = "critical") -> EvaluationResult:
    return EvaluationResult(
        pipeline=pipeline,
        metric_name="row_count",
        value=0.0,
        status=status,
        message="",
        threshold=None,
    )


def _make_middleware(min_pipelines: int = 2, enabled: bool = True):
    window = CorrelationWindow(window_seconds=300)
    config = CorrelationConfig(window_seconds=300, min_pipelines=min_pipelines, enabled=enabled)
    channel = MagicMock()
    mw = CorrelationMiddleware(window=window, config=config, correlation_channels=[channel])
    return mw, channel


def test_dispatch_always_called():
    mw, _ = _make_middleware()
    dispatch = MagicMock()
    mw.process(_result("pipe_a"), dispatch)
    dispatch.assert_called_once()


def test_no_correlation_below_threshold():
    mw, channel = _make_middleware(min_pipelines=3)
    dispatch = MagicMock()
    mw.process(_result("pipe_a"), dispatch)
    mw.process(_result("pipe_b"), dispatch)
    channel.send.assert_not_called()


def test_correlation_fires_when_threshold_met():
    mw, channel = _make_middleware(min_pipelines=2)
    dispatch = MagicMock()
    mw.process(_result("pipe_a"), dispatch)
    mw.process(_result("pipe_b"), dispatch)
    channel.send.assert_called_once()
    event: AlertEvent = channel.send.call_args[0][0]
    assert event.pipeline == "__correlation__"
    assert event.status == "critical"


def test_correlation_fires_only_once_for_same_key():
    mw, channel = _make_middleware(min_pipelines=2)
    dispatch = MagicMock()
    mw.process(_result("pipe_a"), dispatch)
    mw.process(_result("pipe_b"), dispatch)
    mw.process(_result("pipe_a"), dispatch)  # same pipelines active
    assert channel.send.call_count == 1


def test_ok_status_does_not_trigger_correlation():
    mw, channel = _make_middleware(min_pipelines=2)
    dispatch = MagicMock()
    mw.process(_result("pipe_a", status="ok"), dispatch)
    mw.process(_result("pipe_b", status="ok"), dispatch)
    channel.send.assert_not_called()


def test_disabled_config_skips_correlation():
    mw, channel = _make_middleware(enabled=False)
    dispatch = MagicMock()
    mw.process(_result("pipe_a"), dispatch)
    mw.process(_result("pipe_b"), dispatch)
    channel.send.assert_not_called()


def test_stats_tracking():
    mw, _ = _make_middleware(min_pipelines=2)
    dispatch = MagicMock()
    mw.process(_result("pipe_a"), dispatch)
    mw.process(_result("pipe_b"), dispatch)
    s = mw.stats()
    assert s["dispatched"] == 2
    assert s["correlation_fires"] == 1


def test_channel_exception_does_not_propagate():
    mw, channel = _make_middleware(min_pipelines=2)
    channel.send.side_effect = RuntimeError("boom")
    dispatch = MagicMock()
    mw.process(_result("pipe_a"), dispatch)
    mw.process(_result("pipe_b"), dispatch)  # should not raise
