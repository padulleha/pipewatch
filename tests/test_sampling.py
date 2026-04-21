"""Tests for sampling policy and registry."""
from __future__ import annotations

import pytest

from pipewatch.sampling import SamplingPolicy, SamplingRegistry


# ---------------------------------------------------------------------------
# SamplingPolicy validation
# ---------------------------------------------------------------------------

def test_policy_defaults():
    p = SamplingPolicy()
    assert p.strategy == "always"
    assert p.rate == 1.0
    assert p.every_n == 1


def test_policy_invalid_strategy():
    with pytest.raises(ValueError, match="Unknown sampling strategy"):
        SamplingPolicy(strategy="random_walk")


def test_policy_invalid_rate_zero():
    with pytest.raises(ValueError, match="rate must be"):
        SamplingPolicy(strategy="probabilistic", rate=0.0)


def test_policy_invalid_every_n():
    with pytest.raises(ValueError, match="every_n must be"):
        SamplingPolicy(strategy="interval", every_n=0)


# ---------------------------------------------------------------------------
# always strategy
# ---------------------------------------------------------------------------

def test_always_strategy_never_drops():
    reg = SamplingRegistry()
    policy = SamplingPolicy(strategy="always")
    results = [reg.should_sample("pipe", "m", policy) for _ in range(20)]
    assert all(results)


# ---------------------------------------------------------------------------
# interval strategy
# ---------------------------------------------------------------------------

def test_interval_every_3():
    reg = SamplingRegistry()
    policy = SamplingPolicy(strategy="interval", every_n=3)
    results = [reg.should_sample("pipe", "m", policy) for _ in range(9)]
    # Positions 1, 4, 7 (1-indexed) should be True
    assert results == [True, False, False, True, False, False, True, False, False]


def test_interval_resets_per_metric():
    reg = SamplingRegistry()
    policy = SamplingPolicy(strategy="interval", every_n=2)
    # Two independent metrics should each start fresh
    a = reg.should_sample("pipe", "alpha", policy)
    b = reg.should_sample("pipe", "beta", policy)
    assert a is True
    assert b is True


# ---------------------------------------------------------------------------
# probabilistic strategy
# ---------------------------------------------------------------------------

def test_probabilistic_rate_1_always_passes():
    reg = SamplingRegistry()
    policy = SamplingPolicy(strategy="probabilistic", rate=1.0)
    results = [reg.should_sample("pipe", "m", policy) for _ in range(50)]
    assert all(results)


def test_probabilistic_rate_very_low_mostly_drops(monkeypatch):
    import pipewatch.sampling as sm
    monkeypatch.setattr(sm.random, "random", lambda: 0.99)
    reg = SamplingRegistry()
    policy = SamplingPolicy(strategy="probabilistic", rate=0.01)
    results = [reg.should_sample("pipe", "m", policy) for _ in range(10)]
    assert not any(results)


# ---------------------------------------------------------------------------
# reset
# ---------------------------------------------------------------------------

def test_reset_all_clears_state():
    reg = SamplingRegistry()
    policy = SamplingPolicy(strategy="interval", every_n=5)
    for _ in range(3):
        reg.should_sample("pipe", "m", policy)
    reg.reset()
    # After reset, counter restarts so first call returns True again
    assert reg.should_sample("pipe", "m", policy) is True
