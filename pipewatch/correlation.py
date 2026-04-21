"""Correlation tracking: detect when multiple pipelines fail together."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewatch.metrics import EvaluationResult


@dataclass
class CorrelationWindow:
    """Holds recent alert events within a rolling time window (seconds)."""

    window_seconds: int = 300
    _events: List[tuple[datetime, str, str]] = field(default_factory=list, repr=False)

    def record(self, pipeline: str, metric: str, ts: Optional[datetime] = None) -> None:
        now = ts or datetime.now(timezone.utc)
        self._events.append((now, pipeline, metric))
        self._prune(now)

    def _prune(self, now: datetime) -> None:
        cutoff = now.timestamp() - self.window_seconds
        self._events = [(t, p, m) for t, p, m in self._events if t.timestamp() >= cutoff]

    def active_pipelines(self, ts: Optional[datetime] = None) -> List[str]:
        now = ts or datetime.now(timezone.utc)
        self._prune(now)
        return list({p for _, p, _ in self._events})

    def active_metrics(self, ts: Optional[datetime] = None) -> List[str]:
        now = ts or datetime.now(timezone.utc)
        self._prune(now)
        return list({m for _, _, m in self._events})

    def event_count(self, ts: Optional[datetime] = None) -> int:
        now = ts or datetime.now(timezone.utc)
        self._prune(now)
        return len(self._events)


@dataclass
class CorrelationAlert:
    pipelines: List[str]
    metrics: List[str]
    event_count: int
    window_seconds: int
    detected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def summary(self) -> str:
        pipes = ", ".join(sorted(self.pipelines))
        return (
            f"[CORRELATION] {self.event_count} alerts across "
            f"{len(self.pipelines)} pipeline(s) in {self.window_seconds}s: {pipes}"
        )


def check_correlation(
    window: CorrelationWindow,
    result: EvaluationResult,
    min_pipelines: int = 2,
    ts: Optional[datetime] = None,
) -> Optional[CorrelationAlert]:
    """Record result and return a CorrelationAlert if threshold is met."""
    if result.status in ("warning", "critical"):
        window.record(result.pipeline, result.metric_name, ts=ts)

    active = window.active_pipelines(ts=ts)
    if len(active) >= min_pipelines:
        return CorrelationAlert(
            pipelines=active,
            metrics=window.active_metrics(ts=ts),
            event_count=window.event_count(ts=ts),
            window_seconds=window.window_seconds,
            detected_at=ts or datetime.now(timezone.utc),
        )
    return None
