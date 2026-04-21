"""Replay historical metrics through the alert pipeline for testing and debugging."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, Optional

from pipewatch.history import get_history
from pipewatch.metrics import PipelineMetric, ThresholdRule, evaluate, EvaluationResult


@dataclass
class ReplayResult:
    pipeline: str
    metric_name: str
    total: int
    fired: int
    results: List[EvaluationResult] = field(default_factory=list)

    @property
    def fire_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return self.fired / self.total

    def summary(self) -> str:
        return (
            f"[{self.pipeline}/{self.metric_name}] "
            f"{self.fired}/{self.total} fired "
            f"({self.fire_rate:.1%})"
        )


def replay_pipeline(
    pipeline: str,
    metric_name: str,
    rule: ThresholdRule,
    limit: int = 100,
    history_path: Optional[str] = None,
    on_result: Optional[Callable[[EvaluationResult], None]] = None,
) -> ReplayResult:
    """Replay stored history for a metric through a threshold rule."""
    entries = get_history(pipeline, metric_name, limit=limit, path=history_path)
    results: List[EvaluationResult] = []
    fired = 0

    for entry in entries:
        metric = PipelineMetric(
            pipeline=pipeline,
            name=metric_name,
            value=entry["value"],
            timestamp=entry.get("timestamp"),
            tags=entry.get("tags", {}),
        )
        result = evaluate(metric, rule)
        results.append(result)
        if result.status != "ok":
            fired += 1
        if on_result is not None:
            on_result(result)

    return ReplayResult(
        pipeline=pipeline,
        metric_name=metric_name,
        total=len(results),
        fired=fired,
        results=results,
    )
