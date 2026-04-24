"""Tests for pipewatch.backoff and pipewatch.backoff_config."""
from __future__ import annotations

import pytest

from pipewatch.backoff import BackoffPolicy, BackoffState, execute_with_backoff
from pipewatch.backoff_config import backoff_policy_for, parse_backoff_policy


# ---------------------------------------------------------------------------
# BackoffPolicy validation
# ---------------------------------------------------------------------------

def test_policy_defaults():
    p = BackoffPolicy()
    assert p.base_delay == 1.0
    assert p.multiplier == 2.0
    assert p.max_delay == 60.0
    assert p.max_attempts == 5
    assert p.jitter is True


def test_policy_invalid_base_delay():
    with pytest.raises(ValueError, match="base_delay"):
        BackoffPolicy(base_delay=0)


def test_policy_invalid_multiplier():
    with pytest.raises(ValueError, match="multiplier"):
        BackoffPolicy(multiplier=0.5)


def test_policy_invalid_max_delay():
    with pytest.raises(ValueError, match="max_delay"):
        BackoffPolicy(base_delay=10.0, max_delay=5.0)


def test_policy_invalid_max_attempts():
    with pytest.raises(ValueError, match="max_attempts"):
        BackoffPolicy(max_attempts=0)


def test_delay_for_capped_at_max():
    p = BackoffPolicy(base_delay=1.0, multiplier=10.0, max_delay=5.0, jitter=False)
    assert p.delay_for(0) == 1.0
    assert p.delay_for(1) == 5.0   # 10 > 5, capped
    assert p.delay_for(5) == 5.0


def test_delay_for_grows_exponentially():
    p = BackoffPolicy(base_delay=1.0, multiplier=2.0, max_delay=100.0, jitter=False)
    assert p.delay_for(0) == 1.0
    assert p.delay_for(1) == 2.0
    assert p.delay_for(2) == 4.0
    assert p.delay_for(3) == 8.0


# ---------------------------------------------------------------------------
# BackoffState
# ---------------------------------------------------------------------------

def test_state_exhausted_after_max_attempts():
    policy = BackoffPolicy(max_attempts=3, jitter=False)
    state = BackoffState(policy=policy)
    assert not state.exhausted()
    state.record_attempt()
    state.record_attempt()
    state.record_attempt()
    assert state.exhausted()


def test_state_reset():
    policy = BackoffPolicy(max_attempts=2, jitter=False)
    state = BackoffState(policy=policy)
    state.record_attempt()
    state.record_attempt()
    assert state.exhausted()
    state.reset()
    assert state.attempts == 0
    assert not state.exhausted()


# ---------------------------------------------------------------------------
# execute_with_backoff
# ---------------------------------------------------------------------------

def test_success_on_first_attempt():
    policy = BackoffPolicy(max_attempts=3, jitter=False)
    calls = []
    state = execute_with_backoff(lambda: (calls.append(1) or True), policy, sleep_fn=lambda _: None)
    assert len(calls) == 1
    assert state.attempts == 1


def test_success_after_retries():
    policy = BackoffPolicy(max_attempts=5, jitter=False)
    counter = {"n": 0}

    def fn():
        counter["n"] += 1
        return counter["n"] >= 3

    state = execute_with_backoff(fn, policy, sleep_fn=lambda _: None)
    assert counter["n"] == 3
    assert state.attempts == 3


def test_exhausted_after_all_failures():
    policy = BackoffPolicy(max_attempts=3, jitter=False)
    state = execute_with_backoff(lambda: False, policy, sleep_fn=lambda _: None)
    assert state.exhausted()
    assert state.attempts == 3


# ---------------------------------------------------------------------------
# backoff_config
# ---------------------------------------------------------------------------

def test_parse_backoff_policy_defaults():
    p = parse_backoff_policy({})
    assert p.base_delay == 1.0
    assert p.max_attempts == 5


def test_parse_backoff_policy_custom():
    cfg = {"backoff": {"base_delay": 2.0, "multiplier": 3.0, "max_attempts": 10, "jitter": False}}
    p = parse_backoff_policy(cfg)
    assert p.base_delay == 2.0
    assert p.multiplier == 3.0
    assert p.max_attempts == 10
    assert p.jitter is False


def test_backoff_policy_for_pipeline_specific():
    cfg = {
        "backoff": {"base_delay": 1.0},
        "pipelines": {
            "my_pipe": {"backoff": {"base_delay": 5.0, "max_attempts": 2}}
        },
    }
    p = backoff_policy_for("my_pipe", cfg)
    assert p.base_delay == 5.0
    assert p.max_attempts == 2


def test_backoff_policy_for_falls_back_to_global():
    cfg = {
        "backoff": {"base_delay": 3.0, "max_attempts": 7},
        "pipelines": {"other_pipe": {}},
    }
    p = backoff_policy_for("other_pipe", cfg)
    assert p.base_delay == 3.0
    assert p.max_attempts == 7
