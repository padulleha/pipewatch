"""Parse webhook channel configuration for pipewatch."""
from __future__ import annotations

from typing import Any, Dict, List

from pipewatch.webhook import WebhookChannel


def parse_webhook_channels(config: Dict[str, Any]) -> List[WebhookChannel]:
    """Parse a list of webhook channel definitions from a config dict.

    Expected config shape::

        webhooks:
          - url: https://hooks.example.com/notify
            headers:
              Authorization: Bearer token
            timeout: 5
            name: my-hook
          - url: https://other.example.com/alert

    Returns a list of :class:`WebhookChannel` instances.
    """
    entries: List[Dict[str, Any]] = config.get("webhooks", [])
    if not isinstance(entries, list):
        return []

    channels: List[WebhookChannel] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        url = entry.get("url", "").strip()
        if not url:
            continue
        channels.append(
            WebhookChannel(
                url=url,
                headers=entry.get("headers", {}),
                timeout=int(entry.get("timeout", 10)),
                name=entry.get("name", "webhook"),
            )
        )
    return channels
