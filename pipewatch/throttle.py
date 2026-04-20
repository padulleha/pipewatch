"""Alert throttling: limit how often alerts fire per pipeline/metric."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple


@dataclass
class ThrottlePolicy:
    """Defines the minimum interval (seconds) between repeated alerts."""
    min_interval_seconds: int = 300  # 5 minutes default

    def __post_init__(self) -> None:
        if self.min_interval_seconds < 0:
            raise ValueError("min_interval_seconds must be >= 0")


@dataclass
class _ThrottleState:
    last_sent: datetime
    count: int = 1


class ThrottleRegistry:
    """Tracks last-sent times to suppress rapid repeated alerts."""

    def __init__(self) -> None:
        self._states: Dict[Tuple[str, str, str], _ThrottleState] = {}

    def _key(self, pipeline: str, metric: str, status: str) -> Tuple[str, str, str]:
        return (pipeline, metric, status)

    def is_throttled(
        self,
        pipeline: str,
        metric: str,
        status: str,
        policy: ThrottlePolicy,
        now: Optional[datetime] = None,
    ) -> bool:
        """Return True if this alert should be suppressed due to throttling."""
        if now is None:
            now = datetime.now(timezone.utc)
        key = self._key(pipeline, metric, status)
        state = self._states.get(key)
        if state is None:
            return False
        elapsed = (now - state.last_sent).total_seconds()
        return elapsed < policy.min_interval_seconds

    def record(
        self,
        pipeline: str,
        metric: str,
        status: str,
        now: Optional[datetime] = None,
    ) -> None:
        """Record that an alert was sent right now."""
        if now is None:
            now = datetime.now(timezone.utc)
        key = self._key(pipeline, metric, status)
        existing = self._states.get(key)
        if existing:
            existing.last_sent = now
            existing.count += 1
        else:
            self._states[key] = _ThrottleState(last_sent=now)

    def reset(self, pipeline: str, metric: str, status: str) -> None:
        """Clear throttle state for a specific key."""
        self._states.pop(self._key(pipeline, metric, status), None)

    def stats(self, pipeline: str, metric: str, status: str) -> Optional[Dict]:
        """Return throttle state info or None if no state exists."""
        state = self._states.get(self._key(pipeline, metric, status))
        if state is None:
            return None
        return {"last_sent": state.last_sent.isoformat(), "count": state.count}
