"""Core metrics collection and evaluation for pipeline health."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class PipelineMetric:
    name: str
    value: float
    unit: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    tags: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "value": self.value,
            "unit": self.unit,
            "timestamp": self.timestamp.isoformat(),
            "tags": self.tags,
        }


@dataclass
class ThresholdRule:
    metric_name: str
    warning_threshold: Optional[float] = None
    critical_threshold: Optional[float] = None
    operator: str = "gt"  # gt, lt, gte, lte

    def evaluate(self, metric: PipelineMetric) -> str:
        """Return 'ok', 'warning', or 'critical'."""
        ops = {
            "gt": lambda a, b: a > b,
            "lt": lambda a, b: a < b,
            "gte": lambda a, b: a >= b,
            "lte": lambda a, b: a <= b,
        }
        compare = ops.get(self.operator)
        if compare is None:
            raise ValueError(f"Unknown operator: {self.operator}")

        if self.critical_threshold is not None and compare(metric.value, self.critical_threshold):
            return "critical"
        if self.warning_threshold is not None and compare(metric.value, self.warning_threshold):
            return "warning"
        return "ok"


def evaluate_metrics(metrics: list[PipelineMetric], rules: list[ThresholdRule]) -> list[dict]:
    """Evaluate a list of metrics against threshold rules."""
    rule_map = {r.metric_name: r for r in rules}
    results = []
    for metric in metrics:
        rule = rule_map.get(metric.name)
        status = rule.evaluate(metric) if rule else "ok"
        results.append({"metric": metric.to_dict(), "status": status})
    return results
