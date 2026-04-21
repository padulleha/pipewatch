"""Middleware that integrates routing with dedup/throttle before dispatch."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import EvaluationResult
from pipewatch.routing import AlertRouter
from pipewatch.dedup import DedupRegistry
from pipewatch.throttle import ThrottleRegistry


@dataclass
class RoutingMiddleware:
    """Wraps an AlertRouter with optional dedup and throttle guards."""
    router: AlertRouter
    dedup: Optional[DedupRegistry] = None
    throttle: Optional[ThrottleRegistry] = None

    _suppressed_dedup: int = field(default=0, init=False)
    _suppressed_throttle: int = field(default=0, init=False)

    def process(self, result: EvaluationResult) -> bool:
        """Process a result through middleware and route it.

        Returns True if the alert was dispatched, False if suppressed.
        """
        pipeline = result.metric.pipeline
        metric = result.metric.name
        status = result.status

        if self.dedup is not None:
            if self.dedup.is_duplicate(pipeline, metric, status):
                self._suppressed_dedup += 1
                return False
            self.dedup.record(pipeline, metric, status)

        if self.throttle is not None:
            key = f"{pipeline}:{metric}"
            policy = self.throttle.policy_for(key)
            state = self.throttle._states.get(key)  # type: ignore[attr-defined]
            from pipewatch.throttle import _ThrottleState
            import time
            now = time.time()
            if state and (now - state.last_sent) < policy.min_interval_seconds:
                self._suppressed_throttle += 1
                return False
            if state:
                state.last_sent = now
            else:
                self.throttle._states[key] = _ThrottleState(last_sent=now)  # type: ignore[attr-defined]

        self.router.dispatch(result)
        return True

    def stats(self) -> dict:
        return {
            "suppressed_dedup": self._suppressed_dedup,
            "suppressed_throttle": self._suppressed_throttle,
        }

    def reset_stats(self) -> None:
        self._suppressed_dedup = 0
        self._suppressed_throttle = 0
