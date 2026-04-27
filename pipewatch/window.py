"""Sliding window aggregation for pipeline metrics."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewatch.metrics import EvaluationResult


@dataclass
class WindowPolicy:
    window_seconds: int = 300  # 5 minutes
    min_events: int = 1
    aggregate: str = "count"  # count | avg | max | min

    def __post_init__(self) -> None:
        if self.window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if self.min_events < 1:
            raise ValueError("min_events must be >= 1")
        if self.aggregate not in ("count", "avg", "max", "min"):
            raise ValueError(f"Unknown aggregate: {self.aggregate}")


@dataclass
class _WindowEntry:
    result: EvaluationResult
    ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class WindowState:
    policy: WindowPolicy
    _entries: List[_WindowEntry] = field(default_factory=list)

    def record(self, result: EvaluationResult) -> None:
        self._entries.append(_WindowEntry(result=result))
        self._prune()

    def _prune(self) -> None:
        cutoff = datetime.now(timezone.utc).timestamp() - self.policy.window_seconds
        self._entries = [e for e in self._entries if e.ts.timestamp() >= cutoff]

    def aggregate_value(self) -> Optional[float]:
        self._prune()
        values = [e.result.metric.value for e in self._entries]
        if not values:
            return None
        agg = self.policy.aggregate
        if agg == "count":
            return float(len(values))
        if agg == "avg":
            return sum(values) / len(values)
        if agg == "max":
            return max(values)
        if agg == "min":
            return min(values)
        return None  # pragma: no cover

    def has_min_events(self) -> bool:
        self._prune()
        return len(self._entries) >= self.policy.min_events


class WindowRegistry:
    def __init__(self) -> None:
        self._states: Dict[str, WindowState] = {}

    def _key(self, pipeline: str, metric: str) -> str:
        return f"{pipeline}::{metric}"

    def get_or_create(self, pipeline: str, metric: str, policy: WindowPolicy) -> WindowState:
        key = self._key(pipeline, metric)
        if key not in self._states:
            self._states[key] = WindowState(policy=policy)
        return self._states[key]

    def reset(self, pipeline: str, metric: str) -> None:
        self._states.pop(self._key(pipeline, metric), None)
