"""Middleware that checks SLA compliance after each evaluation result."""
from __future__ import annotations

import logging
from typing import Callable, List, Optional

from pipewatch.metrics import EvaluationResult
from pipewatch.sla import SLATracker, SLAViolation

log = logging.getLogger(__name__)

ViolationCallback = Callable[[SLAViolation], None]


class SLAMiddleware:
    """Records evaluation results and fires callbacks on SLA violations."""

    def __init__(
        self,
        tracker: SLATracker,
        downstream: Optional[Callable[[EvaluationResult], None]] = None,
        on_violation: Optional[ViolationCallback] = None,
    ) -> None:
        self._tracker = tracker
        self._downstream = downstream
        self._on_violation = on_violation
        self._violations_fired: int = 0
        self._processed: int = 0

    def process(self, result: EvaluationResult) -> None:
        self._processed += 1
        self._tracker.record(result)

        violation = self._tracker.check(
            result.metric.pipeline, result.metric.name
        )
        if violation is not None:
            self._violations_fired += 1
            log.warning(
                "SLA violation for %s/%s: %s",
                violation.pipeline,
                violation.metric,
                violation.summary(),
            )
            if self._on_violation:
                self._on_violation(violation)

        if self._downstream:
            self._downstream(result)

    def stats(self) -> dict:
        return {
            "processed": self._processed,
            "violations_fired": self._violations_fired,
        }

    def reset_stats(self) -> None:
        self._violations_fired = 0
        self._processed = 0
