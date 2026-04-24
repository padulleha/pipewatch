"""Parse backoff policy configuration from YAML config dicts."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backoff import BackoffPolicy

_DEFAULTS: Dict[str, Any] = {
    "base_delay": 1.0,
    "multiplier": 2.0,
    "max_delay": 60.0,
    "max_attempts": 5,
    "jitter": True,
}


def parse_backoff_policy(cfg: Dict[str, Any]) -> BackoffPolicy:
    """Build a BackoffPolicy from a config dict, filling in defaults."""
    section = cfg.get("backoff", {})
    return BackoffPolicy(
        base_delay=float(section.get("base_delay", _DEFAULTS["base_delay"])),
        multiplier=float(section.get("multiplier", _DEFAULTS["multiplier"])),
        max_delay=float(section.get("max_delay", _DEFAULTS["max_delay"])),
        max_attempts=int(section.get("max_attempts", _DEFAULTS["max_attempts"])),
        jitter=bool(section.get("jitter", _DEFAULTS["jitter"])),
    )


def backoff_policy_for(pipeline: str, full_cfg: Dict[str, Any]) -> BackoffPolicy:
    """Return the BackoffPolicy for a specific pipeline, falling back to global."""
    pipelines = full_cfg.get("pipelines", {})
    pipeline_cfg = pipelines.get(pipeline, {})
    if "backoff" in pipeline_cfg:
        return parse_backoff_policy(pipeline_cfg)
    return parse_backoff_policy(full_cfg)
