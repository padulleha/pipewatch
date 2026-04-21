"""Tests for pipewatch.routing and pipewatch.routing_config."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List
from unittest.mock import MagicMock

import pytest

from pipewatch.metrics import PipelineMetric, EvaluationResult
from pipewatch.alerts import AlertEvent
from pipewatch.routing import RoutingRule, AlertRouter
from pipewatch.routing_config import parse_routing_rules, build_router_from_config


def make_result(pipeline="pipe1", metric="row_count", status="warning", value=5.0, threshold=10.0):
    m = PipelineMetric(pipeline=pipeline, name=metric, value=value)
    return EvaluationResult(
        metric=m,
        status=status,
        threshold=threshold,
        message=f"{metric} is {status}",
    )


# --- RoutingRule ---

class TestRoutingRule:
    def test_matches_any_when_no_filters(self):
        rule = RoutingRule(channels=["log"])
        assert rule.matches(make_result(status="warning"))

    def test_does_not_match_ok_by_default(self):
        rule = RoutingRule(channels=["log"], min_status="warning")
        assert not rule.matches(make_result(status="ok"))

    def test_matches_pipeline_filter(self):
        rule = RoutingRule(channels=["email"], pipeline="pipe1")
        assert rule.matches(make_result(pipeline="pipe1", status="critical"))
        assert not rule.matches(make_result(pipeline="other", status="critical"))

    def test_matches_metric_filter(self):
        rule = RoutingRule(channels=["log"], metric="row_count")
        assert rule.matches(make_result(metric="row_count", status="warning"))
        assert not rule.matches(make_result(metric="latency", status="warning"))

    def test_min_status_critical_skips_warning(self):
        rule = RoutingRule(channels=["pager"], min_status="critical")
        assert not rule.matches(make_result(status="warning"))
        assert rule.matches(make_result(status="critical"))


# --- AlertRouter ---

class TestAlertRouter:
    def _router_with_rules(self):
        router = AlertRouter()
        router.rules = [
            RoutingRule(channels=["email"], pipeline="pipe1", min_status="critical"),
            RoutingRule(channels=["log"], min_status="warning"),
        ]
        return router

    def test_route_returns_matched_channels(self):
        router = self._router_with_rules()
        result = make_result(pipeline="pipe1", status="critical")
        channels = router.route(result)
        assert "email" in channels
        assert "log" in channels

    def test_route_falls_back_to_defaults(self):
        router = AlertRouter()
        router.set_default_channels(["log"])
        channels = router.route(make_result(status="warning"))
        assert channels == ["log"]

    def test_route_deduplicates_channels(self):
        router = AlertRouter()
        router.rules = [
            RoutingRule(channels=["log"]),
            RoutingRule(channels=["log"]),
        ]
        channels = router.route(make_result(status="warning"))
        assert channels.count("log") == 1

    def test_dispatch_calls_channel_send(self):
        router = AlertRouter()
        router.rules = [RoutingRule(channels=["mock"])]
        mock_ch = MagicMock()
        router.add_channel("mock", mock_ch)
        router.dispatch(make_result(status="critical"))
        mock_ch.send.assert_called_once()

    def test_dispatch_skips_ok_status(self):
        router = AlertRouter()
        mock_ch = MagicMock()
        router.add_channel("mock", mock_ch)
        router.set_default_channels(["mock"])
        router.dispatch(make_result(status="ok"))
        mock_ch.send.assert_not_called()


# --- routing_config ---

def test_parse_routing_rules_basic():
    cfg = {
        "routing": {
            "rules": [
                {"channels": ["email"], "pipeline": "sales", "min_status": "critical"},
                {"channels": ["log"]},
            ]
        }
    }
    rules = parse_routing_rules(cfg)
    assert len(rules) == 2
    assert rules[0].pipeline == "sales"
    assert rules[0].min_status == "critical"
    assert rules[1].channels == ["log"]


def test_parse_routing_rules_skips_empty_channels():
    cfg = {"routing": {"rules": [{"channels": []}]}}
    assert parse_routing_rules(cfg) == []


def test_parse_routing_rules_empty_config():
    assert parse_routing_rules({}) == []
