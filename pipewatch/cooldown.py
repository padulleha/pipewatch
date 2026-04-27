"""Cooldown enforcement: suppress repeated alerts for a pipeline/metric
until a minimum quiet period has elapsed since the last firing."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple


@dataclass
class CooldownPolicy:
    """Configuration for a cooldown period."""

    min_seconds: float = 300.0  # 5 minutes default

    def __post_init__(self) -> None:
        if self.min_seconds <= 0:
            raise ValueError("min_seconds must be positive")


@dataclass
class _CooldownState:
    last_fired: datetime
    suppressed_count: int = 0


class CooldownRegistry:
    """Tracks last-fired timestamps and enforces cooldown windows."""

    def __init__(self) -> None:
        self._states: Dict[Tuple[str, str, str], _CooldownState] = {}
        self._default_policy = CooldownPolicy()
        self._policies: Dict[str, CooldownPolicy] = {}

    def set_policy(self, pipeline: str, policy: CooldownPolicy) -> None:
        self._policies[pipeline] = policy

    def _policy_for(self, pipeline: str) -> CooldownPolicy:
        return self._policies.get(pipeline, self._default_policy)

    def _key(self, pipeline: str, metric: str, status: str) -> Tuple[str, str, str]:
        return (pipeline, metric, status)

    def is_cooling_down(
        self,
        pipeline: str,
        metric: str,
        status: str,
        now: Optional[datetime] = None,
    ) -> bool:
        """Return True if the alert is still within the cooldown window."""
        if now is None:
            now = datetime.now(timezone.utc)
        key = self._key(pipeline, metric, status)
        state = self._states.get(key)
        if state is None:
            return False
        policy = self._policy_for(pipeline)
        elapsed = (now - state.last_fired).total_seconds()
        if elapsed < policy.min_seconds:
            state.suppressed_count += 1
            return True
        return False

    def record(
        self,
        pipeline: str,
        metric: str,
        status: str,
        now: Optional[datetime] = None,
    ) -> None:
        """Record a firing event, resetting the cooldown window."""
        if now is None:
            now = datetime.now(timezone.utc)
        key = self._key(pipeline, metric, status)
        existing = self._states.get(key)
        suppressed = existing.suppressed_count if existing else 0
        self._states[key] = _CooldownState(last_fired=now, suppressed_count=suppressed)

    def suppressed_count(self, pipeline: str, metric: str, status: str) -> int:
        state = self._states.get(self._key(pipeline, metric, status))
        return state.suppressed_count if state else 0

    def reset(self, pipeline: str, metric: str, status: str) -> None:
        self._states.pop(self._key(pipeline, metric, status), None)
