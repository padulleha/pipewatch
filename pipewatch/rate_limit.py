"""Rate limiting for alert dispatch — prevents alert floods within a time window."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple


@dataclass
class RateLimitPolicy:
    max_alerts: int = 5
    window_seconds: int = 300  # 5 minutes

    def __post_init__(self) -> None:
        if self.max_alerts < 1:
            raise ValueError("max_alerts must be >= 1")
        if self.window_seconds < 1:
            raise ValueError("window_seconds must be >= 1")


@dataclass
class _WindowState:
    count: int = 0
    window_start: float = field(default_factory=time.monotonic)


class RateLimitRegistry:
    """Tracks per-(pipeline, metric) alert counts within a rolling time window."""

    def __init__(self) -> None:
        self._states: Dict[Tuple[str, str], _WindowState] = {}

    def _key(self, pipeline: str, metric: str) -> Tuple[str, str]:
        return (pipeline, metric)

    def _get_state(self, key: Tuple[str, str], now: float) -> _WindowState:
        state = self._states.get(key)
        if state is None:
            state = _WindowState(window_start=now)
            self._states[key] = state
        return state

    def is_allowed(
        self,
        pipeline: str,
        metric: str,
        policy: RateLimitPolicy,
        now: Optional[float] = None,
    ) -> bool:
        """Return True if an alert may be sent; False if the rate limit is exceeded."""
        if now is None:
            now = time.monotonic()
        key = self._key(pipeline, metric)
        state = self._get_state(key, now)
        elapsed = now - state.window_start
        if elapsed >= policy.window_seconds:
            state.count = 0
            state.window_start = now
        return state.count < policy.max_alerts

    def record(
        self,
        pipeline: str,
        metric: str,
        now: Optional[float] = None,
    ) -> None:
        """Increment the alert counter for the given pipeline/metric pair."""
        if now is None:
            now = time.monotonic()
        key = self._key(pipeline, metric)
        state = self._get_state(key, now)
        state.count += 1

    def reset(self, pipeline: str, metric: str) -> None:
        self._states.pop(self._key(pipeline, metric), None)

    def stats(self, pipeline: str, metric: str) -> Dict[str, object]:
        state = self._states.get(self._key(pipeline, metric))
        if state is None:
            return {"count": 0, "window_start": None}
        return {"count": state.count, "window_start": state.window_start}
