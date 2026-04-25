"""Tests for normalization rules, config parsing, and middleware."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from pipewatch.metrics import PipelineMetric
from pipewatch.normalization import NormalizationRule, NormalizationRegistry
from pipewatch.normalization_config import parse_normalization_rules
from pipewatch.normalization_middleware import NormalizationMiddleware


def make_metric(pipeline="pipe1", name="row_count", value=100.0) -> PipelineMetric:
    return PipelineMetric(pipeline=pipeline, name=name, value=value)


# ---------------------------------------------------------------------------
# NormalizationRule
# ---------------------------------------------------------------------------

class TestNormalizationRule:
    def test_matches_no_constraints(self):
        rule = NormalizationRule()
        assert rule.matches(make_metric()) is True

    def test_matches_pipeline_only(self):
        rule = NormalizationRule(pipeline="pipe1")
        assert rule.matches(make_metric(pipeline="pipe1")) is True
        assert rule.matches(make_metric(pipeline="other")) is False

    def test_matches_metric_only(self):
        rule = NormalizationRule(metric="row_count")
        assert rule.matches(make_metric(name="row_count")) is True
        assert rule.matches(make_metric(name="latency")) is False

    def test_apply_scale(self):
        rule = NormalizationRule(scale=0.001)
        assert rule.apply(1000.0) == pytest.approx(1.0)

    def test_apply_offset(self):
        rule = NormalizationRule(offset=5.0)
        assert rule.apply(10.0) == pytest.approx(15.0)

    def test_apply_clamp_min(self):
        rule = NormalizationRule(scale=0.1, clamp_min=5.0)
        assert rule.apply(10.0) == pytest.approx(5.0)  # 10*0.1=1.0 clamped to 5.0

    def test_apply_clamp_max(self):
        rule = NormalizationRule(scale=10.0, clamp_max=50.0)
        assert rule.apply(10.0) == pytest.approx(50.0)  # 10*10=100 clamped to 50


# ---------------------------------------------------------------------------
# NormalizationRegistry
# ---------------------------------------------------------------------------

def test_registry_no_rules_returns_original():
    registry = NormalizationRegistry()
    m = make_metric(value=42.0)
    result = registry.normalize(m)
    assert result.value == 42.0

def test_registry_applies_first_matching_rule():
    registry = NormalizationRegistry()
    registry.add_rule(NormalizationRule(pipeline="pipe1", scale=2.0))
    registry.add_rule(NormalizationRule(scale=10.0))  # should not be reached
    m = make_metric(pipeline="pipe1", value=5.0)
    assert registry.normalize(m).value == pytest.approx(10.0)

def test_registry_skips_non_matching_rule():
    registry = NormalizationRegistry()
    registry.add_rule(NormalizationRule(pipeline="other", scale=99.0))
    m = make_metric(pipeline="pipe1", value=3.0)
    assert registry.normalize(m).value == pytest.approx(3.0)


# ---------------------------------------------------------------------------
# parse_normalization_rules
# ---------------------------------------------------------------------------

def test_parse_empty_config():
    registry = parse_normalization_rules({})
    assert registry.all_rules() == []

def test_parse_basic_rule():
    config = {
        "normalization": {
            "rules": [
                {"pipeline": "etl", "metric": "rows", "scale": 0.01, "clamp_min": 0.0}
            ]
        }
    }
    registry = parse_normalization_rules(config)
    rules = registry.all_rules()
    assert len(rules) == 1
    assert rules[0].pipeline == "etl"
    assert rules[0].scale == pytest.approx(0.01)
    assert rules[0].clamp_min == pytest.approx(0.0)

def test_parse_defaults():
    config = {"normalization": {"rules": [{}]}}
    registry = parse_normalization_rules(config)
    rule = registry.all_rules()[0]
    assert rule.scale == 1.0
    assert rule.offset == 0.0
    assert rule.clamp_min is None
    assert rule.clamp_max is None


# ---------------------------------------------------------------------------
# NormalizationMiddleware
# ---------------------------------------------------------------------------

def test_middleware_forwards_to_downstream():
    downstream = MagicMock()
    registry = NormalizationRegistry()
    mw = NormalizationMiddleware(registry, downstream)
    m = make_metric(value=7.0)
    mw.process(m, None, MagicMock())
    downstream.assert_called_once()

def test_middleware_counts_normalized():
    downstream = MagicMock()
    registry = NormalizationRegistry()
    registry.add_rule(NormalizationRule(scale=2.0))
    mw = NormalizationMiddleware(registry, downstream)
    mw.process(make_metric(value=5.0), None, MagicMock())
    assert mw.stats()["normalized"] == 1
    assert mw.stats()["passthrough"] == 0

def test_middleware_counts_passthrough():
    downstream = MagicMock()
    registry = NormalizationRegistry()  # no rules
    mw = NormalizationMiddleware(registry, downstream)
    mw.process(make_metric(value=5.0), None, MagicMock())
    assert mw.stats()["passthrough"] == 1
    assert mw.stats()["normalized"] == 0

def test_middleware_reset_stats():
    downstream = MagicMock()
    registry = NormalizationRegistry()
    registry.add_rule(NormalizationRule(scale=3.0))
    mw = NormalizationMiddleware(registry, downstream)
    mw.process(make_metric(value=1.0), None, MagicMock())
    mw.reset_stats()
    assert mw.stats() == {"normalized": 0, "passthrough": 0}
