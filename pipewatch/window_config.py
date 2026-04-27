"""Config parsing for sliding window policies."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.window import WindowPolicy

_DEFAULTS: Dict[str, Any] = {
    "window_seconds": 300,
    "min_events": 1,
    "aggregate": "count",
}


def parse_window_policy(cfg: Dict[str, Any]) -> WindowPolicy:
    """Parse a window policy from a config dict section."""
    window_cfg = cfg.get("window", {})
    return WindowPolicy(
        window_seconds=int(window_cfg.get("window_seconds", _DEFAULTS["window_seconds"])),
        min_events=int(window_cfg.get("min_events", _DEFAULTS["min_events"])),
        aggregate=str(window_cfg.get("aggregate", _DEFAULTS["aggregate"])),
    )


def window_policy_for(pipeline: str, cfg: Dict[str, Any]) -> WindowPolicy:
    """Return the window policy for a specific pipeline, falling back to global."""
    pipelines = cfg.get("pipelines", {})
    pipeline_cfg = pipelines.get(pipeline, {})
    if "window" in pipeline_cfg:
        return parse_window_policy(pipeline_cfg)
    return parse_window_policy(cfg)
