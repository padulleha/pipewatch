"""Middleware that applies sampling before forwarding alerts downstream."""
from __future__ import annotations

from typing import Callable, Dict, Optional

from pipewatch.metrics import EvaluationResult
from pipewatch.sampling import SamplingPolicy, SamplingRegistry


DispatchFn = Callable[[EvaluationResult], None]


class SamplingMiddleware:
    """Wraps a downstream dispatcher and applies per-pipeline sampling policies."""

    def __init__(
        self,
        downstream: DispatchFn,
        default_policy: Optional[SamplingPolicy] = None,
    ) -> None:
        self._downstream = downstream
        self._default_policy = default_policy or SamplingPolicy()
        self._registry = SamplingRegistry()
        self._policies: Dict[str, SamplingPolicy] = {}
        self._total = 0
        self._sampled = 0
        self._dropped = 0

    def set_policy(self, pipeline: str, policy: SamplingPolicy) -> None:
        """Register a pipeline-specific sampling policy."""
        self._policies[pipeline] = policy

    def process(self, result: EvaluationResult) -> None:
        """Apply sampling; call downstream only if the result is selected."""
        self._total += 1
        policy = self._policies.get(result.metric.pipeline, self._default_policy)
        if self._registry.should_sample(result.metric.pipeline, result.metric.name, policy):
            self._sampled += 1
            self._downstream(result)
        else:
            self._dropped += 1

    def stats(self) -> Dict[str, int]:
        return {
            "total": self._total,
            "sampled": self._sampled,
            "dropped": self._dropped,
        }

    def reset_stats(self) -> None:
        self._total = 0
        self._sampled = 0
        self._dropped = 0
        self._registry.reset()
