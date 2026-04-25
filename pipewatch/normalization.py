"""Metric value normalization: scale, clamp, and unit conversion."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class NormalizationRule:
    """Defines how a metric's value should be normalized before evaluation."""
    pipeline: Optional[str] = None   # None = match any
    metric: Optional[str] = None     # None = match any
    scale: float = 1.0               # multiply raw value by this factor
    offset: float = 0.0              # add after scaling
    clamp_min: Optional[float] = None
    clamp_max: Optional[float] = None

    def matches(self, metric: PipelineMetric) -> bool:
        if self.pipeline and metric.pipeline != self.pipeline:
            return False
        if self.metric and metric.name != self.metric:
            return False
        return True

    def apply(self, value: float) -> float:
        result = value * self.scale + self.offset
        if self.clamp_min is not None:
            result = max(self.clamp_min, result)
        if self.clamp_max is not None:
            result = min(self.clamp_max, result)
        return result


@dataclass
class NormalizationRegistry:
    _rules: list[NormalizationRule] = field(default_factory=list)

    def add_rule(self, rule: NormalizationRule) -> None:
        self._rules.append(rule)

    def normalize(self, metric: PipelineMetric) -> PipelineMetric:
        """Return a new PipelineMetric with the value normalized by the first matching rule."""
        for rule in self._rules:
            if rule.matches(metric):
                normalized_value = rule.apply(metric.value)
                return PipelineMetric(
                    pipeline=metric.pipeline,
                    name=metric.name,
                    value=normalized_value,
                    unit=metric.unit,
                    tags=metric.tags,
                    timestamp=metric.timestamp,
                )
        return metric

    def all_rules(self) -> list[NormalizationRule]:
        return list(self._rules)
