"""Tests for pipewatch.enrichment and pipewatch.enrichment_config."""
from __future__ import annotations

import pytest

from pipewatch.alerts import AlertEvent
from pipewatch.enrichment import EnrichmentRegistry, EnrichmentRule
from pipewatch.enrichment_config import parse_enrichment_rules
from pipewatch.metrics import EvaluationResult, PipelineMetric, ThresholdRule


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_event(pipeline: str = "pipe_a", metric: str = "row_count") -> AlertEvent:
    m = PipelineMetric(pipeline=pipeline, name=metric, value=5.0)
    rule = ThresholdRule(metric=metric, warning=10.0, critical=1.0)
    result = EvaluationResult(metric=m, rule=rule, status="warning", value=5.0)
    return AlertEvent(result=result, rule=rule, metadata={})


# ---------------------------------------------------------------------------
# EnrichmentRule.matches
# ---------------------------------------------------------------------------

def test_matches_no_constraints():
    rule = EnrichmentRule(pipeline=None, metric=None, metadata={"env": "prod"})
    assert rule.matches(_make_event())


def test_matches_pipeline_only():
    rule = EnrichmentRule(pipeline="pipe_a", metric=None, metadata={})
    assert rule.matches(_make_event(pipeline="pipe_a"))
    assert not rule.matches(_make_event(pipeline="pipe_b"))


def test_matches_metric_only():
    rule = EnrichmentRule(pipeline=None, metric="row_count", metadata={})
    assert rule.matches(_make_event(metric="row_count"))
    assert not rule.matches(_make_event(metric="latency"))


def test_matches_both_constraints():
    rule = EnrichmentRule(pipeline="pipe_a", metric="row_count", metadata={})
    assert rule.matches(_make_event(pipeline="pipe_a", metric="row_count"))
    assert not rule.matches(_make_event(pipeline="pipe_a", metric="latency"))
    assert not rule.matches(_make_event(pipeline="pipe_b", metric="row_count"))


# ---------------------------------------------------------------------------
# EnrichmentRule.apply
# ---------------------------------------------------------------------------

def test_apply_merges_metadata():
    event = _make_event()
    event = AlertEvent(result=event.result, rule=event.rule, metadata={"existing": "yes"})
    rule = EnrichmentRule(pipeline=None, metric=None, metadata={"team": "data-eng"})
    enriched = rule.apply(event)
    assert enriched.metadata["existing"] == "yes"
    assert enriched.metadata["team"] == "data-eng"


def test_apply_rule_overwrites_existing_key():
    event = _make_event()
    event = AlertEvent(result=event.result, rule=event.rule, metadata={"env": "dev"})
    rule = EnrichmentRule(pipeline=None, metric=None, metadata={"env": "prod"})
    enriched = rule.apply(event)
    assert enriched.metadata["env"] == "prod"


# ---------------------------------------------------------------------------
# EnrichmentRegistry
# ---------------------------------------------------------------------------

def test_registry_applies_matching_rules_in_order():
    registry = EnrichmentRegistry()
    registry.add_rule(EnrichmentRule(pipeline=None, metric=None, metadata={"a": "1"}))
    registry.add_rule(EnrichmentRule(pipeline="pipe_a", metric=None, metadata={"a": "2", "b": "x"}))
    enriched = registry.enrich(_make_event(pipeline="pipe_a"))
    assert enriched.metadata["a"] == "2"  # second rule overwrites
    assert enriched.metadata["b"] == "x"


def test_registry_custom_enricher():
    registry = EnrichmentRegistry()

    def add_source(event: AlertEvent) -> AlertEvent:
        return AlertEvent(result=event.result, rule=event.rule,
                          metadata={**event.metadata, "source": "custom"})

    registry.add_enricher(add_source)
    enriched = registry.enrich(_make_event())
    assert enriched.metadata["source"] == "custom"


def test_registry_clear():
    registry = EnrichmentRegistry()
    registry.add_rule(EnrichmentRule(pipeline=None, metric=None, metadata={"k": "v"}))
    registry.clear()
    enriched = registry.enrich(_make_event())
    assert enriched.metadata == {}


# ---------------------------------------------------------------------------
# parse_enrichment_rules
# ---------------------------------------------------------------------------

def test_parse_empty_config():
    registry = parse_enrichment_rules({})
    event = _make_event()
    assert registry.enrich(event).metadata == {}


def test_parse_rules_from_config():
    cfg = {
        "enrichment": [
            {"pipeline": "pipe_a", "metadata": {"team": "eng", "env": "prod"}},
            {"metric": "row_count", "metadata": {"category": "volume"}},
        ]
    }
    registry = parse_enrichment_rules(cfg)
    enriched = registry.enrich(_make_event(pipeline="pipe_a", metric="row_count"))
    assert enriched.metadata["team"] == "eng"
    assert enriched.metadata["category"] == "volume"


def test_parse_rule_no_pipeline_or_metric():
    cfg = {"enrichment": [{"metadata": {"global": "true"}}]}
    registry = parse_enrichment_rules(cfg)
    enriched = registry.enrich(_make_event())
    assert enriched.metadata["global"] == "true"
