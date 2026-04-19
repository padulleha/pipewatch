"""Report generation for pipeline health summaries."""
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from pipewatch.metrics import PipelineMetric, ThresholdRule, evaluate
from pipewatch.trend import trend_direction, is_anomaly
from pipewatch.history import get_history


@dataclass
class PipelineReport:
    pipeline: str
    generated_at: datetime = field(default_factory=datetime.utcnow)
    entries: List[dict] = field(default_factory=list)

    def summary_lines(self) -> List[str]:
        lines = [f"Pipeline: {self.pipeline}", f"Generated: {self.generated_at.isoformat()}"]
        for e in self.entries:
            status = e.get("status", "unknown").upper()
            metric = e.get("metric")
            value = e.get("value")
            trend = e.get("trend", "stable")
            anomaly = " [ANOMALY]" if e.get("anomaly") else ""
            lines.append(f"  {metric}: {value} | {status} | trend={trend}{anomaly}")
        return lines

    def to_text(self) -> str:
        return "\n".join(self.summary_lines())


def build_report(
    pipeline: str,
    metrics: List[PipelineMetric],
    rules: List[ThresholdRule],
    history_limit: int = 20,
) -> PipelineReport:
    report = PipelineReport(pipeline=pipeline)
    rule_map = {r.metric_name: r for r in rules}

    for metric in metrics:
        history = get_history(pipeline, metric.name, limit=history_limit)
        values = [h["value"] for h in history] + [metric.value]

        rule = rule_map.get(metric.name)
        result = evaluate(metric, rule) if rule else None
        status = result.status if result else "unknown"

        trend = trend_direction(values) if len(values) >= 3 else "stable"
        anomaly = is_anomaly(metric.value, values[:-1]) if len(values) > 3 else False

        report.entries.append({
            "metric": metric.name,
            "value": metric.value,
            "status": status,
            "trend": trend,
            "anomaly": anomaly,
        })

    return report
