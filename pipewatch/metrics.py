"""Core metric data structures and threshold evaluation."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class PipelineMetric:
    pipeline: str
    metric_name: str
    value: float
    unit: str = ""
    tags: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "value": self.value,
            "unit": self.unit,
            "tags": self.tags,
        }


@dataclass
class ThresholdRule:
    metric_name: str
    warning: Optional[float] = None
    critical: Optional[float] = None

    def evaluate(self, metric: PipelineMetric) -> str:
        if self.critical is not None and metric.value >= self.critical:
            return "critical"
        if self.warning is not None and metric.value >= self.warning:
            return "warning"
        return "ok"


@dataclass
class EvaluationResult:
    metric: PipelineMetric
    status: str
    rule: Optional[ThresholdRule] = None


def evaluate(metric: PipelineMetric, rule: ThresholdRule) -> EvaluationResult:
    status = rule.evaluate(metric)
    return EvaluationResult(metric=metric, status=status, rule=rule)


def evaluate_metrics(
    metrics: List[PipelineMetric], rules: List[ThresholdRule]
) -> List[EvaluationResult]:
    rule_map = {r.metric_name: r for r in rules}
    results = []
    for m in metrics:
        rule = rule_map.get(m.metric_name)
        if rule:
            results.append(evaluate(m, rule))
        else:
            results.append(EvaluationResult(metric=m, status="ok"))
    return results
