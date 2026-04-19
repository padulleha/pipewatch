"""Tag filtering and grouping for pipeline metrics."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.metrics import PipelineMetric


@dataclass
class TagFilter:
    required: Dict[str, str] = field(default_factory=dict)
    excluded: Dict[str, str] = field(default_factory=dict)

    def matches(self, metric: PipelineMetric) -> bool:
        tags = metric.tags or {}
        for k, v in self.required.items():
            if tags.get(k) != v:
                return False
        for k, v in self.excluded.items():
            if tags.get(k) == v:
                return False
        return True


def filter_metrics(metrics: List[PipelineMetric], tag_filter: TagFilter) -> List[PipelineMetric]:
    return [m for m in metrics if tag_filter.matches(m)]


def group_by_tag(metrics: List[PipelineMetric], tag_key: str) -> Dict[Optional[str], List[PipelineMetric]]:
    groups: Dict[Optional[str], List[PipelineMetric]] = {}
    for m in metrics:
        val = (m.tags or {}).get(tag_key)
        groups.setdefault(val, []).append(m)
    return groups


def all_tag_values(metrics: List[PipelineMetric], tag_key: str) -> List[str]:
    seen = set()
    result = []
    for m in metrics:
        v = (m.tags or {}).get(tag_key)
        if v is not None and v not in seen:
            seen.add(v)
            result.append(v)
    return result
