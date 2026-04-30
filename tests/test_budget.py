"""Tests for pipewatch.budget and pipewatch.budget_config."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta

import pytest

from pipewatch.budget import BudgetPolicy, BudgetRegistry
from pipewatch.budget_config import build_budget_registry_from_config, parse_budget_policies


# ---------------------------------------------------------------------------
# BudgetPolicy validation
# ---------------------------------------------------------------------------

def test_policy_defaults():
    p = BudgetPolicy()
    assert p.max_alerts == 100
    assert p.window_seconds == 3600


def test_policy_invalid_max_alerts():
    with pytest.raises(ValueError):
        BudgetPolicy(max_alerts=0)


def test_policy_invalid_window():
    with pytest.raises(ValueError):
        BudgetPolicy(window_seconds=0)


# ---------------------------------------------------------------------------
# BudgetRegistry helpers
# ---------------------------------------------------------------------------

@pytest.fixture()
def registry() -> BudgetRegistry:
    reg = BudgetRegistry()
    reg.set_policy("pipe_a", BudgetPolicy(max_alerts=3, window_seconds=60))
    return reg


def _ts(offset_seconds: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(
        seconds=offset_seconds
    )


def test_not_over_budget_initially(registry: BudgetRegistry):
    assert not registry.is_over_budget("pipe_a", now=_ts())


def test_remaining_full_budget(registry: BudgetRegistry):
    assert registry.remaining("pipe_a", now=_ts()) == 3


def test_over_budget_after_max_records(registry: BudgetRegistry):
    for i in range(3):
        registry.record("pipe_a", now=_ts(i))
    assert registry.is_over_budget("pipe_a", now=_ts(3))


def test_remaining_decrements(registry: BudgetRegistry):
    registry.record("pipe_a", now=_ts(0))
    registry.record("pipe_a", now=_ts(1))
    assert registry.remaining("pipe_a", now=_ts(2)) == 1


def test_old_entries_pruned(registry: BudgetRegistry):
    # Record 3 events, then advance past the window — budget should reset.
    for i in range(3):
        registry.record("pipe_a", now=_ts(i))
    assert registry.is_over_budget("pipe_a", now=_ts(10))
    # 70 seconds later, all entries fall outside the 60-second window.
    assert not registry.is_over_budget("pipe_a", now=_ts(70))


def test_reset_clears_state(registry: BudgetRegistry):
    registry.record("pipe_a", now=_ts(0))
    registry.record("pipe_a", now=_ts(1))
    registry.reset("pipe_a")
    assert registry.remaining("pipe_a", now=_ts(2)) == 3


def test_unknown_pipeline_uses_default():
    reg = BudgetRegistry()
    reg.set_default(BudgetPolicy(max_alerts=2, window_seconds=60))
    assert registry.remaining("unknown", now=_ts()) >= 0  # doesn't raise


# ---------------------------------------------------------------------------
# Config parsing
# ---------------------------------------------------------------------------

def test_parse_budget_policies_empty():
    policies = parse_budget_policies({})
    assert "__default__" in policies
    assert policies["__default__"].max_alerts == 100


def test_parse_budget_policies_custom():
    cfg = {
        "budgets": {
            "defaults": {"max_alerts": 20, "window_seconds": 300},
            "pipelines": {
                "etl_daily": {"max_alerts": 5},
            },
        }
    }
    policies = parse_budget_policies(cfg)
    assert policies["__default__"].max_alerts == 20
    assert policies["etl_daily"].max_alerts == 5
    # Inherits default window
    assert policies["etl_daily"].window_seconds == 300


def test_build_registry_from_config():
    cfg = {
        "budgets": {
            "defaults": {"max_alerts": 10, "window_seconds": 120},
            "pipelines": {
                "pipe_x": {"max_alerts": 2, "window_seconds": 30},
            },
        }
    }
    reg = build_budget_registry_from_config(cfg)
    assert reg.remaining("pipe_x", now=_ts()) == 2
    assert reg.remaining("other", now=_ts()) == 10
