"""Middleware that dispatches alert events to configured webhook channels."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, Optional

from pipewatch.alerts import AlertEvent
from pipewatch.metrics import EvaluationResult
from pipewatch.webhook import WebhookChannel, WebhookDispatchResult, dispatch_to_webhooks


@dataclass
class WebhookMiddleware:
    """Wraps a downstream dispatcher and fans out to webhook channels."""

    channels: List[WebhookChannel]
    downstream: Optional[Callable[[AlertEvent], None]] = None
    _success_count: int = field(default=0, init=False, repr=False)
    _failure_count: int = field(default=0, init=False, repr=False)

    def process(self, event: AlertEvent) -> List[WebhookDispatchResult]:
        """Dispatch *event* to all webhook channels.

        Calls the optional downstream callable regardless of webhook outcomes.
        Returns the list of :class:`WebhookDispatchResult` objects.
        """
        results = dispatch_to_webhooks(event, self.channels)
        for r in results:
            if r.success:
                self._success_count += 1
            else:
                self._failure_count += 1

        if self.downstream is not None:
            self.downstream(event)

        return results

    def stats(self) -> dict:
        total = self._success_count + self._failure_count
        return {
            "total_dispatched": total,
            "success": self._success_count,
            "failure": self._failure_count,
        }

    def reset_stats(self) -> None:
        self._success_count = 0
        self._failure_count = 0
