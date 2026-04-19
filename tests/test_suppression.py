"""Tests for pipewatch.suppression."""

import time
import pytest
from pipewatch.suppression import SuppressionRule, SuppressionRegistry, parse_suppressions


# ---------------------------------------------------------------------------
# SuppressionRule
# ---------------------------------------------------------------------------

class TestSuppressionRule:
    def test_permanent_rule_is_active(self):
        rule = SuppressionRule(pipeline="p1")
        assert rule.is_active() is True

    def test_future_expiry_is_active(self):
        rule = SuppressionRule(pipeline="p1", expires_at=time.time() + 9999)
        assert rule.is_active() is True

    def test_past_expiry_is_inactive(self):
        rule = SuppressionRule(pipeline="p1", expires_at=time.time() - 1)
        assert rule.is_active() is False

    def test_matches_pipeline_and_metric(self):
        rule = SuppressionRule(pipeline="p1", metric_name="row_count")
        assert rule.matches("p1", "row_count") is True

    def test_no_match_wrong_pipeline(self):
        rule = SuppressionRule(pipeline="p1", metric_name="row_count")
        assert rule.matches("p2", "row_count") is False

    def test_no_match_wrong_metric(self):
        rule = SuppressionRule(pipeline="p1", metric_name="row_count")
        assert rule.matches("p1", "latency") is False

    def test_matches_all_metrics_when_metric_name_none(self):
        rule = SuppressionRule(pipeline="p1", metric_name=None)
        assert rule.matches("p1", "anything") is True

    def test_expired_rule_does_not_match(self):
        rule = SuppressionRule(pipeline="p1", expires_at=time.time() - 1)
        assert rule.matches("p1", "row_count") is False


# ---------------------------------------------------------------------------
# SuppressionRegistry
# ---------------------------------------------------------------------------

class TestSuppressionRegistry:
    def test_is_suppressed_true(self):
        reg = SuppressionRegistry()
        reg.add(SuppressionRule(pipeline="p1", metric_name="row_count"))
        assert reg.is_suppressed("p1", "row_count") is True

    def test_is_suppressed_false(self):
        reg = SuppressionRegistry()
        assert reg.is_suppressed("p1", "row_count") is False

    def test_remove_expired(self):
        reg = SuppressionRegistry()
        reg.add(SuppressionRule(pipeline="p1", expires_at=time.time() - 1))
        reg.add(SuppressionRule(pipeline="p2"))
        removed = reg.remove_expired()
        assert removed == 1
        assert len(reg.active_rules()) == 1

    def test_active_rules_excludes_expired(self):
        reg = SuppressionRegistry()
        reg.add(SuppressionRule(pipeline="p1", expires_at=time.time() - 1))
        reg.add(SuppressionRule(pipeline="p2"))
        assert len(reg.active_rules()) == 1


# ---------------------------------------------------------------------------
# parse_suppressions
# ---------------------------------------------------------------------------

def test_parse_empty_config():
    reg = parse_suppressions({})
    assert reg.active_rules() == []


def test_parse_basic_entry():
    cfg = {"suppressions": [{"pipeline": "etl_main", "reason": "maintenance"}]}
    reg = parse_suppressions(cfg)
    assert reg.is_suppressed("etl_main", "any_metric") is True


def test_parse_with_duration():
    cfg = {"suppressions": [{"pipeline": "etl_main", "duration_seconds": 3600}]}
    reg = parse_suppressions(cfg)
    rules = reg.active_rules()
    assert len(rules) == 1
    assert rules[0].expires_at is not None
    assert rules[0].expires_at > time.time()


def test_parse_skips_missing_pipeline():
    cfg = {"suppressions": [{"reason": "oops"}]}
    reg = parse_suppressions(cfg)
    assert reg.active_rules() == []
