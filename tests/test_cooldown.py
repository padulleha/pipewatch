"""Tests for pipewatch.cooldown."""

from datetime import datetime, timezone, timedelta

import pytest

from pipewatch.cooldown import CooldownPolicy, CooldownRegistry


UTC = timezone.utc


def _now() -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# CooldownPolicy validation
# ---------------------------------------------------------------------------

def test_policy_default_min_seconds():
    policy = CooldownPolicy()
    assert policy.min_seconds == 300.0


def test_policy_invalid_min_seconds():
    with pytest.raises(ValueError):
        CooldownPolicy(min_seconds=0)


def test_policy_negative_min_seconds():
    with pytest.raises(ValueError):
        CooldownPolicy(min_seconds=-10)


# ---------------------------------------------------------------------------
# CooldownRegistry — basic behaviour
# ---------------------------------------------------------------------------

@pytest.fixture
def registry() -> CooldownRegistry:
    return CooldownRegistry()


def test_first_check_not_cooling_down(registry):
    assert not registry.is_cooling_down("pipe", "row_count", "critical", _now())


def test_after_record_is_cooling_down(registry):
    t = _now()
    registry.record("pipe", "row_count", "critical", now=t)
    # 10 seconds later — still within default 300 s window
    assert registry.is_cooling_down("pipe", "row_count", "critical", t + timedelta(seconds=10))


def test_after_window_expires_not_cooling_down(registry):
    t = _now()
    registry.record("pipe", "row_count", "critical", now=t)
    # 301 seconds later — window has elapsed
    assert not registry.is_cooling_down("pipe", "row_count", "critical", t + timedelta(seconds=301))


def test_suppressed_count_increments(registry):
    t = _now()
    registry.record("pipe", "row_count", "critical", now=t)
    registry.is_cooling_down("pipe", "row_count", "critical", t + timedelta(seconds=5))
    registry.is_cooling_down("pipe", "row_count", "critical", t + timedelta(seconds=10))
    assert registry.suppressed_count("pipe", "row_count", "critical") == 2


def test_suppressed_count_zero_before_any_record(registry):
    assert registry.suppressed_count("pipe", "row_count", "critical") == 0


def test_reset_clears_state(registry):
    t = _now()
    registry.record("pipe", "row_count", "critical", now=t)
    registry.reset("pipe", "row_count", "critical")
    assert not registry.is_cooling_down("pipe", "row_count", "critical", t + timedelta(seconds=5))


def test_custom_policy_respected(registry):
    registry.set_policy("pipe", CooldownPolicy(min_seconds=60))
    t = _now()
    registry.record("pipe", "row_count", "warning", now=t)
    # 61 seconds later — custom window expired
    assert not registry.is_cooling_down("pipe", "row_count", "warning", t + timedelta(seconds=61))
    # 30 seconds later — still within custom window
    registry.reset("pipe", "row_count", "warning")
    registry.record("pipe", "row_count", "warning", now=t)
    assert registry.is_cooling_down("pipe", "row_count", "warning", t + timedelta(seconds=30))


def test_different_statuses_tracked_independently(registry):
    t = _now()
    registry.record("pipe", "row_count", "warning", now=t)
    # critical was never recorded — should not be cooling down
    assert not registry.is_cooling_down("pipe", "row_count", "critical", t + timedelta(seconds=5))
    assert registry.is_cooling_down("pipe", "row_count", "warning", t + timedelta(seconds=5))
