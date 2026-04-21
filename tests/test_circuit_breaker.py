"""Tests for pipewatch.circuit_breaker."""
import time
import pytest
from pipewatch.circuit_breaker import (
    CircuitBreakerPolicy,
    CircuitBreakerRegistry,
    CircuitState,
)


@pytest.fixture
def registry() -> CircuitBreakerRegistry:
    return CircuitBreakerRegistry()


@pytest.fixture
def policy() -> CircuitBreakerPolicy:
    return CircuitBreakerPolicy(failure_threshold=3, recovery_timeout=60.0, success_threshold=1)


# --- Policy validation ---

def test_policy_defaults():
    p = CircuitBreakerPolicy()
    assert p.failure_threshold == 3
    assert p.recovery_timeout == 60.0
    assert p.success_threshold == 1


def test_policy_invalid_failure_threshold():
    with pytest.raises(ValueError):
        CircuitBreakerPolicy(failure_threshold=0)


def test_policy_invalid_recovery_timeout():
    with pytest.raises(ValueError):
        CircuitBreakerPolicy(recovery_timeout=0)


def test_policy_invalid_success_threshold():
    with pytest.raises(ValueError):
        CircuitBreakerPolicy(success_threshold=0)


# --- Normal closed-circuit behaviour ---

def test_initial_state_is_closed(registry):
    assert registry.circuit_state("email") == CircuitState.CLOSED


def test_allowed_when_closed(registry):
    assert registry.is_allowed("email") is True


def test_success_keeps_circuit_closed(registry):
    registry.record_success("email")
    assert registry.circuit_state("email") == CircuitState.CLOSED


# --- Opening the circuit ---

def test_circuit_opens_after_threshold(registry, policy):
    registry.set_policy("email", policy)
    for _ in range(3):
        registry.record_failure("email")
    assert registry.circuit_state("email") == CircuitState.OPEN


def test_circuit_not_open_below_threshold(registry, policy):
    registry.set_policy("email", policy)
    for _ in range(2):
        registry.record_failure("email")
    assert registry.circuit_state("email") == CircuitState.CLOSED


def test_open_circuit_blocks_dispatch(registry, policy):
    registry.set_policy("email", policy)
    for _ in range(3):
        registry.record_failure("email")
    assert registry.is_allowed("email") is False


# --- Half-open recovery ---

def test_half_open_after_recovery_timeout(registry, monkeypatch):
    fast_policy = CircuitBreakerPolicy(failure_threshold=1, recovery_timeout=1.0)
    registry.set_policy("slack", fast_policy)
    registry.record_failure("slack")
    assert registry.circuit_state("slack") == CircuitState.OPEN

    # Simulate time passing
    start = time.monotonic()
    monkeypatch.setattr("pipewatch.circuit_breaker.time",
                        type("T", (), {"monotonic": staticmethod(lambda: start + 2.0)})())
    allowed = registry.is_allowed("slack")
    assert allowed is True
    assert registry.circuit_state("slack") == CircuitState.HALF_OPEN


def test_success_in_half_open_closes_circuit(registry, policy):
    registry.set_policy("email", policy)
    for _ in range(3):
        registry.record_failure("email")
    st = registry._state("email")
    st.state = CircuitState.HALF_OPEN  # force half-open
    registry.record_success("email")
    assert registry.circuit_state("email") == CircuitState.CLOSED


def test_failure_in_half_open_reopens_circuit(registry, policy):
    registry.set_policy("email", policy)
    st = registry._state("email")
    st.state = CircuitState.HALF_OPEN
    registry.record_failure("email")
    assert registry.circuit_state("email") == CircuitState.OPEN


# --- Reset ---

def test_reset_clears_state(registry, policy):
    registry.set_policy("email", policy)
    for _ in range(3):
        registry.record_failure("email")
    registry.reset("email")
    assert registry.circuit_state("email") == CircuitState.CLOSED
    assert registry.is_allowed("email") is True
