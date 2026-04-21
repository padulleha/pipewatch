"""Alert routing: direct evaluation results to channels based on rules."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import EvaluationResult
from pipewatch.alerts import AlertChannel, AlertEvent


@dataclass
class RoutingRule:
    """Maps a filter predicate to a list of channel names."""
    channels: List[str]
    pipeline: Optional[str] = None      # None means match any
    metric: Optional[str] = None        # None means match any
    min_status: str = "warning"         # warning | critical

    _STATUS_RANK = {"ok": 0, "warning": 1, "critical": 2}

    def matches(self, result: EvaluationResult) -> bool:
        if self.pipeline and result.metric.pipeline != self.pipeline:
            return False
        if self.metric and result.metric.name != self.metric:
            return False
        rank = self._STATUS_RANK.get(result.status, 0)
        min_rank = self._STATUS_RANK.get(self.min_status, 1)
        return rank >= min_rank


@dataclass
class AlertRouter:
    """Routes EvaluationResults to the appropriate AlertChannels."""
    rules: List[RoutingRule] = field(default_factory=list)
    channels: dict = field(default_factory=dict)  # name -> AlertChannel
    _default_channels: List[str] = field(default_factory=list)

    def add_channel(self, name: str, channel: AlertChannel) -> None:
        self.channels[name] = channel

    def set_default_channels(self, names: List[str]) -> None:
        self._default_channels = names

    def route(self, result: EvaluationResult) -> List[str]:
        """Return channel names that should receive this result."""
        matched: List[str] = []
        for rule in self.rules:
            if rule.matches(result):
                matched.extend(rule.channels)
        if not matched:
            matched = list(self._default_channels)
        return list(dict.fromkeys(matched))  # deduplicate, preserve order

    def dispatch(self, result: EvaluationResult) -> None:
        """Send an AlertEvent to all matched channels."""
        if result.status == "ok":
            return
        event = AlertEvent(
            pipeline=result.metric.pipeline,
            metric=result.metric.name,
            status=result.status,
            value=result.metric.value,
            threshold=result.threshold,
            message=result.message,
        )
        for name in self.route(result):
            ch = self.channels.get(name)
            if ch is not None:
                ch.send(event)
