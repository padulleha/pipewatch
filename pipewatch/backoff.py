"""Exponential backoff policy for alert retry scheduling."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class BackoffPolicy:
    base_delay: float = 1.0        # seconds
    multiplier: float = 2.0
    max_delay: float = 60.0
    max_attempts: int = 5
    jitter: bool = True

    def __post_init__(self) -> None:
        if self.base_delay <= 0:
            raise ValueError("base_delay must be positive")
        if self.multiplier < 1.0:
            raise ValueError("multiplier must be >= 1.0")
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay must be >= base_delay")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

    def delay_for(self, attempt: int) -> float:
        """Return delay in seconds for the given attempt (0-indexed)."""
        delay = min(self.base_delay * (self.multiplier ** attempt), self.max_delay)
        if self.jitter:
            import random
            delay *= 0.5 + random.random() * 0.5
        return delay


@dataclass
class BackoffState:
    policy: BackoffPolicy
    attempts: int = 0
    _last_attempt: Optional[float] = field(default=None, repr=False)

    def record_attempt(self) -> None:
        self.attempts += 1
        self._last_attempt = time.monotonic()

    def next_delay(self) -> float:
        return self.policy.delay_for(self.attempts)

    def exhausted(self) -> bool:
        return self.attempts >= self.policy.max_attempts

    def reset(self) -> None:
        self.attempts = 0
        self._last_attempt = None


def execute_with_backoff(
    fn: Callable[[], bool],
    policy: BackoffPolicy,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> BackoffState:
    """Call fn repeatedly with backoff until it returns True or attempts exhausted."""
    state = BackoffState(policy=policy)
    while not state.exhausted():
        state.record_attempt()
        if fn():
            return state
        if not state.exhausted():
            sleep_fn(state.next_delay())
    return state
