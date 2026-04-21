"""Parse correlation configuration from a config dict."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from pipewatch.correlation import CorrelationWindow


@dataclass
class CorrelationConfig:
    window_seconds: int = 300
    min_pipelines: int = 2
    enabled: bool = True


def parse_correlation_config(cfg: Dict[str, Any]) -> CorrelationConfig:
    """Parse the ``correlation`` section of a pipewatch config dict."""
    section = cfg.get("correlation", {})

    window = int(section.get("window_seconds", 300))
    if window <= 0:
        raise ValueError(f"correlation.window_seconds must be positive, got {window}")

    min_pipes = int(section.get("min_pipelines", 2))
    if min_pipes < 2:
        raise ValueError(
            f"correlation.min_pipelines must be >= 2, got {min_pipes}"
        )

    enabled = bool(section.get("enabled", True))
    return CorrelationConfig(
        window_seconds=window,
        min_pipelines=min_pipes,
        enabled=enabled,
    )


def build_correlation_window(cfg: CorrelationConfig) -> CorrelationWindow:
    """Construct a CorrelationWindow from a CorrelationConfig."""
    return CorrelationWindow(window_seconds=cfg.window_seconds)
