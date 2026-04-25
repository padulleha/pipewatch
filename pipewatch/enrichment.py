"""Metric enrichment: attach contextual metadata to alert events before dispatch."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from pipewatch.alerts import AlertEvent


@dataclass
class EnrichmentRule:
    """A single enrichment rule that adds key/value metadata to matching events."""

    pipeline: Optional[str]  # None means match all pipelines
    metric: Optional[str]    # None means match all metrics
    metadata: Dict[str, Any] = field(default_factory=dict)

    def matches(self, event: AlertEvent) -> bool:
        if self.pipeline is not None and event.result.metric.pipeline != self.pipeline:
            return False
        if self.metric is not None and event.result.metric.name != self.metric:
            return False
        return True

    def apply(self, event: AlertEvent) -> AlertEvent:
        """Return a new AlertEvent with metadata merged in."""
        merged = {**event.metadata, **self.metadata}
        return AlertEvent(
            result=event.result,
            rule=event.rule,
            metadata=merged,
        )


@dataclass
class EnrichmentRegistry:
    """Holds ordered enrichment rules applied to every alert event."""

    _rules: List[EnrichmentRule] = field(default_factory=list)
    _custom: List[Callable[[AlertEvent], AlertEvent]] = field(default_factory=list)

    def add_rule(self, rule: EnrichmentRule) -> None:
        self._rules.append(rule)

    def add_enricher(self, fn: Callable[[AlertEvent], AlertEvent]) -> None:
        """Register an arbitrary callable enricher."""
        self._custom.append(fn)

    def enrich(self, event: AlertEvent) -> AlertEvent:
        """Apply all matching rules and custom enrichers in order."""
        for rule in self._rules:
            if rule.matches(event):
                event = rule.apply(event)
        for fn in self._custom:
            event = fn(event)
        return event

    def clear(self) -> None:
        self._rules.clear()
        self._custom.clear()
