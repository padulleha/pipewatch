"""Middleware that wraps alert dispatch with throttle enforcement."""
from __future__ import annotations

from typing import Callable, Dict, List, Optional

from pipewatch.alerts import AlertEvent
from pipewatch.throttle import ThrottlePolicy, ThrottleRegistry


class ThrottleDispatcher:
    """Dispatches alerts while enforcing per-pipeline throttle policies.

    Alerts that fire within the configured min_interval are dropped and
    counted so callers can observe suppression activity.
    """

    def __init__(
        self,
        send_fn: Callable[[AlertEvent], None],
        policies: Optional[Dict[str, ThrottlePolicy]] = None,
        default_policy: Optional[ThrottlePolicy] = None,
    ) -> None:
        self._send = send_fn
        self._policies: Dict[str, ThrottlePolicy] = policies or {}
        self._default = default_policy or ThrottlePolicy()
        self._registry = ThrottleRegistry()
        self._suppressed: int = 0

    def _policy_for(self, pipeline: str) -> ThrottlePolicy:
        return self._policies.get(pipeline, self._default)

    def dispatch(self, event: AlertEvent) -> bool:
        """Send *event* unless throttled.  Returns True if alert was sent."""
        pipeline = event.pipeline
        metric = event.metric_name
        status = event.status
        policy = self._policy_for(pipeline)

        if self._registry.is_throttled(pipeline, metric, status, policy):
            self._suppressed += 1
            return False

        self._registry.record(pipeline, metric, status)
        self._send(event)
        return True

    def reset_suppressed_count(self) -> None:
        """Reset the running suppressed-alert counter."""
        self._suppressed = 0

    @property
    def suppressed_count(self) -> int:
        """Number of alerts suppressed since last reset."""
        return self._suppressed

    def stats(self, pipeline: str, metric: str, status: str) -> Optional[Dict]:
        """Expose throttle state for a specific key (for debugging/CLI)."""
        return self._registry.stats(pipeline, metric, status)
