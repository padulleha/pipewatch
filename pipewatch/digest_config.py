"""Parse digest configuration from a config dict."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class DigestConfig:
    enabled: bool = True
    window_seconds: int = 3600
    channels: list = None  # list of channel names to send the digest to

    def __post_init__(self):
        if self.channels is None:
            self.channels = ["log"]
        if self.window_seconds <= 0:
            raise ValueError("digest window_seconds must be positive")


def parse_digest_config(raw: Dict[str, Any]) -> DigestConfig:
    """Build a DigestConfig from the *digest* section of pipewatch config.

    Example YAML section::

        digest:
          enabled: true
          window_seconds: 1800
          channels:
            - log
            - email
    """
    section: Dict[str, Any] = raw.get("digest", {})
    return DigestConfig(
        enabled=bool(section.get("enabled", True)),
        window_seconds=int(section.get("window_seconds", 3600)),
        channels=list(section.get("channels", ["log"])),
    )
