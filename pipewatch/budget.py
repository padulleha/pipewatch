"""Alert budget tracking: limits total alerts fired within a rolling window."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Tuple


@dataclass
class BudgetPolicy:
    max_alerts: int = 100
    window_seconds: int = 3600

    def __post_init__(self) -> None:
        if self.max_alerts < 1:
            raise ValueError("max_alerts must be >= 1")
        if self.window_seconds < 1:
            raise ValueError("window_seconds must be >= 1")


@dataclass
class _BudgetState:
    timestamps: List[float] = field(default_factory=list)


@dataclass
class BudgetRegistry:
    _policies: Dict[str, BudgetPolicy] = field(default_factory=dict)
    _states: Dict[str, _BudgetState] = field(default_factory=dict)
    _default: BudgetPolicy = field(default_factory=BudgetPolicy)

    def set_policy(self, pipeline: str, policy: BudgetPolicy) -> None:
        self._policies[pipeline] = policy

    def set_default(self, policy: BudgetPolicy) -> None:
        self._default = policy

    def _policy_for(self, pipeline: str) -> BudgetPolicy:
        return self._policies.get(pipeline, self._default)

    def _state_for(self, pipeline: str) -> _BudgetState:
        if pipeline not in self._states:
            self._states[pipeline] = _BudgetState()
        return self._states[pipeline]

    def _prune(self, state: _BudgetState, policy: BudgetPolicy, now: float) -> None:
        cutoff = now - policy.window_seconds
        state.timestamps = [t for t in state.timestamps if t >= cutoff]

    def is_over_budget(self, pipeline: str, now: datetime | None = None) -> bool:
        ts = (now or datetime.now(timezone.utc)).timestamp()
        policy = self._policy_for(pipeline)
        state = self._state_for(pipeline)
        self._prune(state, policy, ts)
        return len(state.timestamps) >= policy.max_alerts

    def record(self, pipeline: str, now: datetime | None = None) -> None:
        ts = (now or datetime.now(timezone.utc)).timestamp()
        policy = self._policy_for(pipeline)
        state = self._state_for(pipeline)
        self._prune(state, policy, ts)
        state.timestamps.append(ts)

    def remaining(self, pipeline: str, now: datetime | None = None) -> int:
        ts = (now or datetime.now(timezone.utc)).timestamp()
        policy = self._policy_for(pipeline)
        state = self._state_for(pipeline)
        self._prune(state, policy, ts)
        return max(0, policy.max_alerts - len(state.timestamps))

    def reset(self, pipeline: str) -> None:
        self._states.pop(pipeline, None)
