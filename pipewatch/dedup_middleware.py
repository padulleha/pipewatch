"""Middleware helper: wrap alert dispatch with deduplication."""

from __future__ import annotations

from typing import Callable, List

from pipewatch.alerts import AlertEvent
from pipewatch.dedup import DedupRegistry


DispatchFn = Callable[[AlertEvent], None]


class DedupDispatcher:
    """Wraps one or more dispatch callables with dedup logic."""

    def __init__(
        self,
        registry: DedupRegistry,
        dispatchers: List[DispatchFn],
    ) -> None:
        self.registry = registry
        self.dispatchers = dispatchers
        self.suppressed_count = 0
        self.dispatched_count = 0

    def dispatch(self, event: AlertEvent) -> bool:
        """Send *event* through all dispatchers unless it is a duplicate.

        Returns True if the event was dispatched, False if suppressed.
        """
        pipeline = event.pipeline
        metric = event.metric_name
        status = event.status

        if self.registry.is_duplicate(pipeline, metric, status):
            self.suppressed_count += 1
            return False

        self.registry.record(pipeline, metric, status)
        for fn in self.dispatchers:
            fn(event)
        self.dispatched_count += 1
        return True

    def reset_suppressed_count(self) -> None:
        self.suppressed_count = 0

    def stats(self) -> dict:
        """Return a snapshot of dispatch statistics.

        Returns a dict with ``dispatched`` and ``suppressed`` counts.
        """
        return {
            "dispatched": self.dispatched_count,
            "suppressed": self.suppressed_count,
        }
