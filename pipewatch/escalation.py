"""Alert escalation policy: re-alert after a metric stays in a bad state."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional


@dataclass
class EscalationPolicy:
    """Defines how often to re-fire an alert for a persistently failing metric."""
    interval_minutes: int = 30
    max_escalations: int = 3
    enabled: bool = True


@dataclass
class EscalationState:
    pipeline: str
    metric_name: str
    first_fired: datetime = field(default_factory=datetime.utcnow)
    last_fired: datetime = field(default_factory=datetime.utcnow)
    count: int = 1

    def should_escalate(self, policy: EscalationPolicy) -> bool:
        if not policy.enabled:
            return False
        if self.count >= policy.max_escalations:
            return False
        elapsed = datetime.utcnow() - self.last_fired
        return elapsed >= timedelta(minutes=policy.interval_minutes)

    def record_escalation(self) -> None:
        self.last_fired = datetime.utcnow()
        self.count += 1


class EscalationRegistry:
    def __init__(self) -> None:
        self._states: Dict[tuple, EscalationState] = {}

    def _key(self, pipeline: str, metric_name: str) -> tuple:
        return (pipeline, metric_name)

    def get_or_create(self, pipeline: str, metric_name: str) -> EscalationState:
        key = self._key(pipeline, metric_name)
        if key not in self._states:
            self._states[key] = EscalationState(pipeline=pipeline, metric_name=metric_name)
        return self._states[key]

    def clear(self, pipeline: str, metric_name: str) -> None:
        """Call when metric returns to OK to reset escalation tracking."""
        self._states.pop(self._key(pipeline, metric_name), None)

    def check_and_escalate(
        self,
        pipeline: str,
        metric_name: str,
        policy: EscalationPolicy,
    ) -> bool:
        """Return True if an escalation alert should be fired now."""
        state = self.get_or_create(pipeline, metric_name)
        if state.count == 1 and state.first_fired == state.last_fired:
            # First occurrence already handled by normal alert path
            return False
        if state.should_escalate(policy):
            state.record_escalation()
            return True
        return False

    def mark_initial_fire(self, pipeline: str, metric_name: str) -> EscalationState:
        """Register the first alert fire for a metric."""
        key = self._key(pipeline, metric_name)
        state = EscalationState(pipeline=pipeline, metric_name=metric_name)
        self._states[key] = state
        return state
