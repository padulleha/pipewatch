"""SLA (Service Level Agreement) tracking for pipeline metrics."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewatch.metrics import EvaluationResult


@dataclass
class SLAPolicy:
    """Defines acceptable SLA bounds for a pipeline/metric pair."""
    pipeline: str
    metric: str
    max_critical_rate: float = 0.05   # 5% of evaluations may be CRITICAL
    max_warning_rate: float = 0.20    # 20% of evaluations may be WARNING
    window_minutes: int = 60

    def __post_init__(self) -> None:
        if not 0.0 <= self.max_critical_rate <= 1.0:
            raise ValueError("max_critical_rate must be between 0 and 1")
        if not 0.0 <= self.max_warning_rate <= 1.0:
            raise ValueError("max_warning_rate must be between 0 and 1")
        if self.window_minutes <= 0:
            raise ValueError("window_minutes must be positive")


@dataclass
class SLAViolation:
    pipeline: str
    metric: str
    critical_rate: float
    warning_rate: float
    policy: SLAPolicy
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def summary(self) -> str:
        parts = []
        if self.critical_rate > self.policy.max_critical_rate:
            parts.append(
                f"critical rate {self.critical_rate:.1%} > allowed {self.policy.max_critical_rate:.1%}"
            )
        if self.warning_rate > self.policy.max_warning_rate:
            parts.append(
                f"warning rate {self.warning_rate:.1%} > allowed {self.policy.max_warning_rate:.1%}"
            )
        return "; ".join(parts) if parts else "no violation"


@dataclass
class SLATracker:
    """Tracks evaluation results and checks against SLA policies."""
    _policies: Dict[str, SLAPolicy] = field(default_factory=dict)
    _events: List[tuple] = field(default_factory=list)  # (datetime, EvaluationResult)

    def add_policy(self, policy: SLAPolicy) -> None:
        key = f"{policy.pipeline}:{policy.metric}"
        self._policies[key] = policy

    def record(self, result: EvaluationResult) -> None:
        self._events.append((datetime.now(timezone.utc), result))

    def check(self, pipeline: str, metric: str) -> Optional[SLAViolation]:
        key = f"{pipeline}:{metric}"
        policy = self._policies.get(key)
        if policy is None:
            return None

        cutoff = datetime.now(timezone.utc).timestamp() - policy.window_minutes * 60
        relevant = [
            r for ts, r in self._events
            if ts.timestamp() >= cutoff
            and r.metric.pipeline == pipeline
            and r.metric.name == metric
        ]
        if not relevant:
            return None

        total = len(relevant)
        critical_rate = sum(1 for r in relevant if r.status == "critical") / total
        warning_rate = sum(1 for r in relevant if r.status == "warning") / total

        if critical_rate > policy.max_critical_rate or warning_rate > policy.max_warning_rate:
            return SLAViolation(
                pipeline=pipeline,
                metric=metric,
                critical_rate=critical_rate,
                warning_rate=warning_rate,
                policy=policy,
            )
        return None
