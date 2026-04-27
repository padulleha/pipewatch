"""Debounce middleware: suppress alerts until a condition persists for N consecutive checks."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, Optional, Tuple

from pipewatch.metrics import EvaluationResult


@dataclass
class DebouncePolicy:
    min_consecutive: int = 2  # fire only after this many consecutive non-OK results

    def __post_init__(self) -> None:
        if self.min_consecutive < 1:
            raise ValueError("min_consecutive must be >= 1")


@dataclass
class _DebounceState:
    consecutive: int = 0
    last_status: Optional[str] = None


@dataclass
class DebounceRegistry:
    _states: Dict[Tuple[str, str], _DebounceState] = field(default_factory=dict)

    def _key(self, pipeline: str, metric: str) -> Tuple[str, str]:
        return (pipeline, metric)

    def _state(self, pipeline: str, metric: str) -> _DebounceState:
        k = self._key(pipeline, metric)
        if k not in self._states:
            self._states[k] = _DebounceState()
        return self._states[k]

    def should_fire(self, result: EvaluationResult, policy: DebouncePolicy) -> bool:
        """Return True if the alert should be forwarded downstream."""
        pipeline = result.metric.pipeline
        metric = result.metric.name
        state = self._state(pipeline, metric)

        if result.status == "ok":
            state.consecutive = 0
            state.last_status = "ok"
            return False

        if result.status != state.last_status:
            state.consecutive = 1
        else:
            state.consecutive += 1

        state.last_status = result.status
        return state.consecutive >= policy.min_consecutive

    def reset(self, pipeline: str, metric: str) -> None:
        k = self._key(pipeline, metric)
        self._states.pop(k, None)

    def consecutive_count(self, pipeline: str, metric: str) -> int:
        return self._state(pipeline, metric).consecutive
