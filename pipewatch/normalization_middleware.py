"""Middleware that normalizes metric values before forwarding to downstream handlers."""
from __future__ import annotations

from typing import Callable, Optional

from pipewatch.metrics import PipelineMetric, ThresholdRule, EvaluationResult
from pipewatch.normalization import NormalizationRegistry


DispatchFn = Callable[[PipelineMetric, Optional[ThresholdRule], EvaluationResult], None]


class NormalizationMiddleware:
    """Applies normalization rules to incoming metrics before passing them downstream."""

    def __init__(
        self,
        registry: NormalizationRegistry,
        downstream: DispatchFn,
    ) -> None:
        self._registry = registry
        self._downstream = downstream
        self._normalized_count = 0
        self._passthrough_count = 0

    def process(
        self,
        metric: PipelineMetric,
        rule: Optional[ThresholdRule],
        result: EvaluationResult,
    ) -> None:
        original_value = metric.value
        normalized_metric = self._registry.normalize(metric)

        if normalized_metric.value != original_value:
            self._normalized_count += 1
        else:
            self._passthrough_count += 1

        self._downstream(normalized_metric, rule, result)

    def stats(self) -> dict[str, int]:
        return {
            "normalized": self._normalized_count,
            "passthrough": self._passthrough_count,
        }

    def reset_stats(self) -> None:
        self._normalized_count = 0
        self._passthrough_count = 0
