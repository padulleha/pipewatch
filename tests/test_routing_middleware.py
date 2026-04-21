"""Tests for pipewatch.routing_middleware."""
from __future__ import annotations

from unittest.mock import MagicMock, patch
import time

import pytest

from pipewatch.metrics import PipelineMetric, EvaluationResult
from pipewatch.routing import AlertRouter, RoutingRule
from pipewatch.dedup import DedupRegistry
from pipewatch.routing_middleware import RoutingMiddleware


def make_result(pipeline="pipe1", metric="rows", status="warning", value=1.0):
    m = PipelineMetric(pipeline=pipeline, name=metric, value=value)
    return EvaluationResult(metric=m, status=status, threshold=10.0, message="test")


def _router_with_mock_channel():
    router = AlertRouter()
    router.rules = [RoutingRule(channels=["mock"])]
    mock_ch = MagicMock()
    router.add_channel("mock", mock_ch)
    return router, mock_ch


def test_dispatch_called_without_guards():
    router, mock_ch = _router_with_mock_channel()
    mw = RoutingMiddleware(router=router)
    dispatched = mw.process(make_result(status="critical"))
    assert dispatched is True
    mock_ch.send.assert_called_once()


def test_ok_status_not_dispatched():
    router, mock_ch = _router_with_mock_channel()
    mw = RoutingMiddleware(router=router)
    dispatched = mw.process(make_result(status="ok"))
    # router.dispatch skips ok; but middleware returns True (not suppressed by mw)
    mock_ch.send.assert_not_called()


def test_dedup_suppresses_second_identical():
    router, mock_ch = _router_with_mock_channel()
    dedup = DedupRegistry(default_cooldown_seconds=60)
    mw = RoutingMiddleware(router=router, dedup=dedup)

    r = make_result(status="critical")
    first = mw.process(r)
    second = mw.process(r)

    assert first is True
    assert second is False
    assert mock_ch.send.call_count == 1
    assert mw.stats()["suppressed_dedup"] == 1


def test_dedup_allows_different_status():
    router, mock_ch = _router_with_mock_channel()
    dedup = DedupRegistry(default_cooldown_seconds=60)
    mw = RoutingMiddleware(router=router, dedup=dedup)

    mw.process(make_result(status="warning"))
    mw.process(make_result(status="critical"))  # different status — not duplicate
    assert mock_ch.send.call_count == 2


def test_reset_stats_clears_counts():
    router, _ = _router_with_mock_channel()
    dedup = DedupRegistry(default_cooldown_seconds=60)
    mw = RoutingMiddleware(router=router, dedup=dedup)

    r = make_result(status="critical")
    mw.process(r)
    mw.process(r)
    assert mw.stats()["suppressed_dedup"] == 1

    mw.reset_stats()
    assert mw.stats()["suppressed_dedup"] == 0
    assert mw.stats()["suppressed_throttle"] == 0
