"""Middleware that applies debounce logic before forwarding alerts."""
from __future__ import annotations

from typing import Callable, Dict, Optional

from pipewatch.debounce import DebouncePolicy, DebounceRegistry
from pipewatch.metrics import EvaluationResult


class DebounceMiddleware:
    """Wraps a downstream dispatch callable with debounce suppression."""

    def __init__(
        self,
        downstream: Callable[[EvaluationResult], None],
        default_policy: Optional[DebouncePolicy] = None,
    ) -> None:
        self._downstream = downstream
        self._default_policy = default_policy or DebouncePolicy()
        self._registry = DebounceRegistry()
        self._pipeline_policies: Dict[str, DebouncePolicy] = {}
        self._suppressed = 0
        self._forwarded = 0

    def set_policy(self, pipeline: str, policy: DebouncePolicy) -> None:
        self._pipeline_policies[pipeline] = policy

    def _policy_for(self, pipeline: str) -> DebouncePolicy:
        return self._pipeline_policies.get(pipeline, self._default_policy)

    def process(self, result: EvaluationResult) -> None:
        policy = self._policy_for(result.metric.pipeline)
        if self._registry.should_fire(result, policy):
            self._forwarded += 1
            self._downstream(result)
        else:
            self._suppressed += 1

    def stats(self) -> Dict[str, int]:
        return {"forwarded": self._forwarded, "suppressed": self._suppressed}

    def reset_stats(self) -> None:
        self._suppressed = 0
        self._forwarded = 0
