"""Mute rules: temporarily silence alerts for specific pipelines/metrics."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional


@dataclass
class MuteRule:
    pipeline: str
    metric: Optional[str] = None  # None means all metrics for the pipeline
    reason: str = ""
    expires_at: Optional[datetime] = None  # None means muted indefinitely

    def is_active(self, now: Optional[datetime] = None) -> bool:
        """Return True if the mute rule is currently active."""
        if now is None:
            now = datetime.now(timezone.utc)
        if self.expires_at is None:
            return True
        return now < self.expires_at

    def matches(self, pipeline: str, metric: str) -> bool:
        """Return True if this rule applies to the given pipeline and metric."""
        if self.pipeline != pipeline:
            return False
        if self.metric is not None and self.metric != metric:
            return False
        return True


@dataclass
class MuteRegistry:
    _rules: List[MuteRule] = field(default_factory=list)

    def add(self, rule: MuteRule) -> None:
        self._rules.append(rule)

    def remove(self, pipeline: str, metric: Optional[str] = None) -> int:
        """Remove matching rules. Returns the number of rules removed."""
        before = len(self._rules)
        self._rules = [
            r for r in self._rules
            if not (r.pipeline == pipeline and r.metric == metric)
        ]
        return before - len(self._rules)

    def is_muted(
        self,
        pipeline: str,
        metric: str,
        now: Optional[datetime] = None,
    ) -> bool:
        """Return True if any active rule mutes the given pipeline/metric."""
        if now is None:
            now = datetime.now(timezone.utc)
        return any(
            rule.is_active(now) and rule.matches(pipeline, metric)
            for rule in self._rules
        )

    def active_rules(self, now: Optional[datetime] = None) -> List[MuteRule]:
        if now is None:
            now = datetime.now(timezone.utc)
        return [r for r in self._rules if r.is_active(now)]

    def purge_expired(self, now: Optional[datetime] = None) -> int:
        """Remove expired rules. Returns count removed."""
        if now is None:
            now = datetime.now(timezone.utc)
        before = len(self._rules)
        self._rules = [r for r in self._rules if r.is_active(now)]
        return before - len(self._rules)
