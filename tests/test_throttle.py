"""Tests for pipewatch.throttle and pipewatch.throttle_config."""
from datetime import datetime, timedelta, timezone

import pytest

from pipewatch.throttle import ThrottlePolicy, ThrottleRegistry
from pipewatch.throttle_config import parse_throttle_config, throttle_policy_for

UTC = timezone.utc


# ---------------------------------------------------------------------------
# ThrottlePolicy
# ---------------------------------------------------------------------------

def test_policy_default_interval():
    p = ThrottlePolicy()
    assert p.min_interval_seconds == 300


def test_policy_invalid_interval():
    with pytest.raises(ValueError):
        ThrottlePolicy(min_interval_seconds=-1)


# ---------------------------------------------------------------------------
# ThrottleRegistry
# ---------------------------------------------------------------------------

@pytest.fixture
def registry():
    return ThrottleRegistry()


@pytest.fixture
def policy():
    return ThrottlePolicy(min_interval_seconds=60)


def test_first_alert_not_throttled(registry, policy):
    assert not registry.is_throttled("pipe", "rows", "warning", policy)


def test_throttled_after_record(registry, policy):
    now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    registry.record("pipe", "rows", "warning", now=now)
    later = now + timedelta(seconds=30)
    assert registry.is_throttled("pipe", "rows", "warning", policy, now=later)


def test_not_throttled_after_interval_passes(registry, policy):
    now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    registry.record("pipe", "rows", "warning", now=now)
    later = now + timedelta(seconds=61)
    assert not registry.is_throttled("pipe", "rows", "warning", policy, now=later)


def test_different_status_not_throttled(registry, policy):
    now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    registry.record("pipe", "rows", "warning", now=now)
    later = now + timedelta(seconds=10)
    assert not registry.is_throttled("pipe", "rows", "critical", policy, now=later)


def test_record_increments_count(registry):
    now = datetime(2024, 1, 1, tzinfo=UTC)
    registry.record("pipe", "rows", "warning", now=now)
    registry.record("pipe", "rows", "warning", now=now + timedelta(seconds=70))
    stats = registry.stats("pipe", "rows", "warning")
    assert stats["count"] == 2


def test_reset_clears_state(registry, policy):
    now = datetime(2024, 1, 1, tzinfo=UTC)
    registry.record("pipe", "rows", "warning", now=now)
    registry.reset("pipe", "rows", "warning")
    assert not registry.is_throttled("pipe", "rows", "warning", policy, now=now)


def test_stats_none_when_no_state(registry):
    assert registry.stats("pipe", "rows", "warning") is None


# ---------------------------------------------------------------------------
# throttle_config
# ---------------------------------------------------------------------------

def test_parse_empty_config():
    policies = parse_throttle_config({})
    assert "__default__" in policies
    assert policies["__default__"].min_interval_seconds == 300


def test_parse_default_override():
    cfg = {"throttle": {"default": {"min_interval_seconds": 120}}}
    policies = parse_throttle_config(cfg)
    assert policies["__default__"].min_interval_seconds == 120


def test_parse_pipeline_specific():
    cfg = {
        "throttle": {
            "pipelines": {
                "fast_pipe": {"min_interval_seconds": 10}
            }
        }
    }
    policies = parse_throttle_config(cfg)
    assert policies["fast_pipe"].min_interval_seconds == 10


def test_throttle_policy_for_fallback():
    policies = parse_throttle_config({})
    p = throttle_policy_for("unknown_pipe", policies)
    assert p.min_interval_seconds == 300


def test_throttle_policy_for_specific():
    cfg = {"throttle": {"pipelines": {"my_pipe": {"min_interval_seconds": 45}}}}
    policies = parse_throttle_config(cfg)
    p = throttle_policy_for("my_pipe", policies)
    assert p.min_interval_seconds == 45
