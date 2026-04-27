"""Middleware that gates alert dispatch behind a sliding window."""
from __future__ import annotations

from typing import Callable, Dict, Optional

from pipewatch.metrics import EvaluationResult
from pipewatch.window import WindowPolicy, WindowRegistry

_DispatchFn = Callable[[EvaluationResult], None]


class WindowMiddleware:
    """Only forwards results once the window has enough events."""

    def __init__(
        self,
        downstream: _DispatchFn,
        default_policy: Optional[WindowPolicy] = None,
    ) -> None:
        self._downstream = downstream
        self._default_policy = default_policy or WindowPolicy()
        self._registry = WindowRegistry()
        self._forwarded: int = 0
        self._suppressed: int = 0

    def set_policy(self, pipeline: str, metric: str, policy: WindowPolicy) -> None:
        state = self._registry.get_or_create(pipeline, metric, policy)
        state.policy = policy

    def _policy_for(self, result: EvaluationResult) -> WindowPolicy:
        return self._default_policy

    def process(self, result: EvaluationResult) -> None:
        p = result.metric.pipeline
        m = result.metric.name
        policy = self._policy_for(result)
        state = self._registry.get_or_create(p, m, policy)
        state.record(result)

        if state.has_min_events():
            self._forwarded += 1
            self._downstream(result)
        else:
            self._suppressed += 1

    def stats(self) -> Dict[str, int]:
        return {"forwarded": self._forwarded, "suppressed": self._suppressed}

    def reset_stats(self) -> None:
        self._forwarded = 0
        self._suppressed = 0
