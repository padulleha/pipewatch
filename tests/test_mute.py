"""Tests for pipewatch.mute."""

from datetime import datetime, timedelta, timezone

import pytest

from pipewatch.mute import MuteRegistry, MuteRule


def utc(**kwargs) -> datetime:
    return datetime.now(timezone.utc) + timedelta(**kwargs)


# ---------------------------------------------------------------------------
# MuteRule.is_active
# ---------------------------------------------------------------------------

class TestMuteRuleIsActive:
    def test_no_expiry_always_active(self):
        rule = MuteRule(pipeline="p1", expires_at=None)
        assert rule.is_active() is True

    def test_future_expiry_is_active(self):
        rule = MuteRule(pipeline="p1", expires_at=utc(hours=1))
        assert rule.is_active() is True

    def test_past_expiry_is_inactive(self):
        rule = MuteRule(pipeline="p1", expires_at=utc(hours=-1))
        assert rule.is_active() is False


# ---------------------------------------------------------------------------
# MuteRule.matches
# ---------------------------------------------------------------------------

class TestMuteRuleMatches:
    def test_matches_pipeline_any_metric(self):
        rule = MuteRule(pipeline="etl", metric=None)
        assert rule.matches("etl", "row_count") is True
        assert rule.matches("etl", "latency") is True

    def test_matches_specific_metric(self):
        rule = MuteRule(pipeline="etl", metric="row_count")
        assert rule.matches("etl", "row_count") is True
        assert rule.matches("etl", "latency") is False

    def test_no_match_wrong_pipeline(self):
        rule = MuteRule(pipeline="etl", metric=None)
        assert rule.matches("other", "row_count") is False


# ---------------------------------------------------------------------------
# MuteRegistry
# ---------------------------------------------------------------------------

@pytest.fixture()
def registry() -> MuteRegistry:
    return MuteRegistry()


def test_not_muted_when_empty(registry):
    assert registry.is_muted("etl", "row_count") is False


def test_muted_by_active_rule(registry):
    registry.add(MuteRule(pipeline="etl", metric="row_count"))
    assert registry.is_muted("etl", "row_count") is True


def test_expired_rule_does_not_mute(registry):
    registry.add(MuteRule(pipeline="etl", metric="row_count", expires_at=utc(hours=-1)))
    assert registry.is_muted("etl", "row_count") is False


def test_wildcard_metric_mutes_all(registry):
    registry.add(MuteRule(pipeline="etl", metric=None))
    assert registry.is_muted("etl", "row_count") is True
    assert registry.is_muted("etl", "latency") is True


def test_remove_returns_count(registry):
    registry.add(MuteRule(pipeline="etl", metric="row_count"))
    registry.add(MuteRule(pipeline="etl", metric="row_count"))
    removed = registry.remove("etl", "row_count")
    assert removed == 2
    assert registry.is_muted("etl", "row_count") is False


def test_purge_expired(registry):
    registry.add(MuteRule(pipeline="etl", expires_at=utc(hours=-1)))
    registry.add(MuteRule(pipeline="etl", expires_at=utc(hours=1)))
    removed = registry.purge_expired()
    assert removed == 1
    assert len(registry.active_rules()) == 1


def test_active_rules_filters_expired(registry):
    registry.add(MuteRule(pipeline="a", expires_at=utc(hours=-2)))
    registry.add(MuteRule(pipeline="b", expires_at=utc(hours=2)))
    active = registry.active_rules()
    assert len(active) == 1
    assert active[0].pipeline == "b"
