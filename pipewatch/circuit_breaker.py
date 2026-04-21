"""Circuit breaker for pipeline alert channels.

Prevents repeated dispatching to a failing channel by tracking consecutive
failures and opening the circuit after a configurable threshold.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional


class CircuitState(str, Enum):
    CLOSED = "closed"      # normal operation
    OPEN = "open"          # failing; requests blocked
    HALF_OPEN = "half_open"  # probe allowed


@dataclass
class CircuitBreakerPolicy:
    failure_threshold: int = 3       # consecutive failures to open
    recovery_timeout: float = 60.0   # seconds before moving to HALF_OPEN
    success_threshold: int = 1       # successes in HALF_OPEN to close

    def __post_init__(self) -> None:
        if self.failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if self.recovery_timeout <= 0:
            raise ValueError("recovery_timeout must be > 0")
        if self.success_threshold < 1:
            raise ValueError("success_threshold must be >= 1")


@dataclass
class _BreakerState:
    state: CircuitState = CircuitState.CLOSED
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    opened_at: Optional[float] = None


class CircuitBreakerRegistry:
    def __init__(self) -> None:
        self._states: Dict[str, _BreakerState] = {}
        self._policies: Dict[str, CircuitBreakerPolicy] = {}
        self._default_policy = CircuitBreakerPolicy()

    def set_policy(self, channel: str, policy: CircuitBreakerPolicy) -> None:
        self._policies[channel] = policy

    def _policy(self, channel: str) -> CircuitBreakerPolicy:
        return self._policies.get(channel, self._default_policy)

    def _state(self, channel: str) -> _BreakerState:
        if channel not in self._states:
            self._states[channel] = _BreakerState()
        return self._states[channel]

    def is_allowed(self, channel: str) -> bool:
        """Return True if a dispatch attempt is permitted."""
        st = self._state(channel)
        policy = self._policy(channel)
        if st.state == CircuitState.CLOSED:
            return True
        if st.state == CircuitState.OPEN:
            if st.opened_at is not None and (time.monotonic() - st.opened_at) >= policy.recovery_timeout:
                st.state = CircuitState.HALF_OPEN
                st.consecutive_successes = 0
                return True
            return False
        # HALF_OPEN: allow one probe
        return True

    def record_success(self, channel: str) -> None:
        st = self._state(channel)
        policy = self._policy(channel)
        if st.state == CircuitState.HALF_OPEN:
            st.consecutive_successes += 1
            if st.consecutive_successes >= policy.success_threshold:
                st.state = CircuitState.CLOSED
                st.consecutive_failures = 0
                st.opened_at = None
        elif st.state == CircuitState.CLOSED:
            st.consecutive_failures = 0

    def record_failure(self, channel: str) -> None:
        st = self._state(channel)
        policy = self._policy(channel)
        st.consecutive_failures += 1
        st.consecutive_successes = 0
        if st.state in (CircuitState.CLOSED, CircuitState.HALF_OPEN):
            if st.consecutive_failures >= policy.failure_threshold:
                st.state = CircuitState.OPEN
                st.opened_at = time.monotonic()

    def circuit_state(self, channel: str) -> CircuitState:
        return self._state(channel).state

    def reset(self, channel: str) -> None:
        self._states.pop(channel, None)
