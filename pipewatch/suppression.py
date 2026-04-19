"""Alert suppression rules — silence alerts for a pipeline/metric during maintenance or cooldown."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class SuppressionRule:
    pipeline: str
    metric_name: Optional[str] = None  # None means all metrics
    reason: str = ""
    expires_at: Optional[float] = None  # Unix timestamp; None = permanent

    def is_active(self) -> bool:
        if self.expires_at is None:
            return True
        return time.time() < self.expires_at

    def matches(self, pipeline: str, metric_name: str) -> bool:
        if not self.is_active():
            return False
        if self.pipeline != pipeline:
            return False
        if self.metric_name is not None and self.metric_name != metric_name:
            return False
        return True


@dataclass
class SuppressionRegistry:
    _rules: List[SuppressionRule] = field(default_factory=list)

    def add(self, rule: SuppressionRule) -> None:
        self._rules.append(rule)

    def remove_expired(self) -> int:
        before = len(self._rules)
        self._rules = [r for r in self._rules if r.is_active()]
        return before - len(self._rules)

    def is_suppressed(self, pipeline: str, metric_name: str) -> bool:
        return any(r.matches(pipeline, metric_name) for r in self._rules)

    def active_rules(self) -> List[SuppressionRule]:
        return [r for r in self._rules if r.is_active()]


def parse_suppressions(config: dict) -> SuppressionRegistry:
    """Build a SuppressionRegistry from a config dict.

    Expected format:
        suppressions:
          - pipeline: my_pipeline
            metric_name: row_count   # optional
            reason: "maintenance window"
            duration_seconds: 3600   # optional; omit for permanent
    """
    registry = SuppressionRegistry()
    entries = config.get("suppressions", [])
    for entry in entries:
        pipeline = entry.get("pipeline")
        if not pipeline:
            continue
        expires_at: Optional[float] = None
        if "duration_seconds" in entry:
            expires_at = time.time() + float(entry["duration_seconds"])
        rule = SuppressionRule(
            pipeline=pipeline,
            metric_name=entry.get("metric_name"),
            reason=entry.get("reason", ""),
            expires_at=expires_at,
        )
        registry.add(rule)
    return registry
