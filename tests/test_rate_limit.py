"""Tests for pipewatch.rate_limit."""

import pytest
from pipewatch.rate_limit import RateLimitPolicy, RateLimitRegistry


# ---------------------------------------------------------------------------
# RateLimitPolicy
# ---------------------------------------------------------------------------

def test_policy_defaults():
    p = RateLimitPolicy()
    assert p.max_alerts == 5
    assert p.window_seconds == 300


def test_policy_invalid_max_alerts():
    with pytest.raises(ValueError, match="max_alerts"):
        RateLimitPolicy(max_alerts=0)


def test_policy_invalid_window():
    with pytest.raises(ValueError, match="window_seconds"):
        RateLimitPolicy(window_seconds=0)


# ---------------------------------------------------------------------------
# RateLimitRegistry — helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def registry() -> RateLimitRegistry:
    return RateLimitRegistry()


@pytest.fixture
def policy() -> RateLimitPolicy:
    return RateLimitPolicy(max_alerts=3, window_seconds=60)


# ---------------------------------------------------------------------------
# is_allowed / record
# ---------------------------------------------------------------------------

def test_first_alert_is_allowed(registry, policy):
    assert registry.is_allowed("pipe", "lag", policy, now=0.0) is True


def test_allowed_up_to_max(registry, policy):
    for i in range(3):
        assert registry.is_allowed("pipe", "lag", policy, now=float(i)) is True
        registry.record("pipe", "lag", now=float(i))
    # 4th attempt within same window should be blocked
    assert registry.is_allowed("pipe", "lag", policy, now=3.0) is False


def test_window_reset_allows_again(registry, policy):
    for i in range(3):
        registry.record("pipe", "lag", now=0.0)
    # still blocked inside window
    assert registry.is_allowed("pipe", "lag", policy, now=59.0) is False
    # after window expires, allowed again
    assert registry.is_allowed("pipe", "lag", policy, now=61.0) is True


def test_different_metrics_are_independent(registry, policy):
    for _ in range(3):
        registry.record("pipe", "lag", now=0.0)
    # "errors" metric should still be allowed
    assert registry.is_allowed("pipe", "errors", policy, now=0.0) is True


def test_different_pipelines_are_independent(registry, policy):
    for _ in range(3):
        registry.record("pipe_a", "lag", now=0.0)
    assert registry.is_allowed("pipe_b", "lag", policy, now=0.0) is True


def test_reset_clears_state(registry, policy):
    for _ in range(3):
        registry.record("pipe", "lag", now=0.0)
    assert registry.is_allowed("pipe", "lag", policy, now=1.0) is False
    registry.reset("pipe", "lag")
    assert registry.is_allowed("pipe", "lag", policy, now=1.0) is True


def test_stats_no_history(registry):
    s = registry.stats("pipe", "lag")
    assert s["count"] == 0
    assert s["window_start"] is None


def test_stats_after_records(registry, policy):
    registry.record("pipe", "lag", now=10.0)
    registry.record("pipe", "lag", now=11.0)
    s = registry.stats("pipe", "lag")
    assert s["count"] == 2
    assert s["window_start"] == 10.0
