"""Middleware that optionally records results for later replay."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, Optional

from pipewatch.history import record_metric
from pipewatch.metrics import EvaluationResult

# Downstream handler type
Handler = Callable[[EvaluationResult], None]


@dataclass
class ReplayMiddleware:
    """Records every evaluation result into the history store before
    forwarding it downstream.  Allows later replay via replay_pipeline()."""

    downstream: Handler
    history_path: Optional[str] = None
    _stats: Dict[str, int] = field(default_factory=lambda: {"recorded": 0, "forwarded": 0})

    def process(self, result: EvaluationResult) -> None:
        """Record the metric value then forward to the downstream handler."""
        record_metric(
            pipeline=result.metric.pipeline,
            metric_name=result.metric.name,
            value=result.metric.value,
            tags=result.metric.tags,
            path=self.history_path,
        )
        self._stats["recorded"] += 1
        self.downstream(result)
        self._stats["forwarded"] += 1

    def stats(self) -> Dict[str, int]:
        return dict(self._stats)

    def reset_stats(self) -> None:
        self._stats = {"recorded": 0, "forwarded": 0}
