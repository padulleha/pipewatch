"""Webhook alert channel for pipewatch."""
from __future__ import annotations

import json
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pipewatch.alerts import AlertEvent


@dataclass
class WebhookChannel:
    """Sends alert events as JSON POST requests to a URL."""

    url: str
    headers: Dict[str, str] = field(default_factory=dict)
    timeout: int = 10
    name: str = "webhook"

    def _build_payload(self, event: AlertEvent) -> Dict[str, Any]:
        return {
            "pipeline": event.pipeline,
            "metric": event.metric,
            "status": event.status,
            "value": event.value,
            "message": event.summary(),
        }

    def send(self, event: AlertEvent) -> bool:
        """POST the event payload to the configured URL.

        Returns True on success, False on failure.
        """
        payload = json.dumps(self._build_payload(event)).encode("utf-8")
        headers = {"Content-Type": "application/json", **self.headers}
        req = urllib.request.Request(self.url, data=payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout):
                return True
        except (urllib.error.URLError, OSError):
            return False


@dataclass
class WebhookDispatchResult:
    url: str
    success: bool
    pipeline: str
    metric: str

    def __str__(self) -> str:
        status = "OK" if self.success else "FAILED"
        return f"[webhook:{status}] {self.pipeline}/{self.metric} -> {self.url}"


def dispatch_to_webhooks(
    event: AlertEvent, channels: List[WebhookChannel]
) -> List[WebhookDispatchResult]:
    """Send an event to all webhook channels, returning results."""
    results: List[WebhookDispatchResult] = []
    for ch in channels:
        ok = ch.send(event)
        results.append(
            WebhookDispatchResult(
                url=ch.url,
                success=ok,
                pipeline=event.pipeline,
                metric=event.metric,
            )
        )
    return results
