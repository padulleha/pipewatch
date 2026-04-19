"""Parse deduplication configuration from a config dict."""

from __future__ import annotations

from typing import Any, Dict

from pipewatch.dedup import DedupRegistry

_DEFAULT_COOLDOWN = 300.0


def parse_dedup_config(config: Dict[str, Any]) -> DedupRegistry:
    """Build a DedupRegistry from the top-level config dict.

    Expected config shape::

        dedup:
          cooldown_seconds: 600
    """
    dedup_cfg = config.get("dedup", {})
    cooldown = float(dedup_cfg.get("cooldown_seconds", _DEFAULT_COOLDOWN))
    return DedupRegistry(cooldown_seconds=cooldown)


def cooldown_for_pipeline(
    pipeline: str, config: Dict[str, Any]
) -> float:
    """Return cooldown override for a specific pipeline, falling back to global."""
    global_cooldown = float(
        config.get("dedup", {}).get("cooldown_seconds", _DEFAULT_COOLDOWN)
    )
    pipelines = config.get("pipelines", {})
    pipeline_cfg = pipelines.get(pipeline, {})
    return float(
        pipeline_cfg.get("dedup", {}).get("cooldown_seconds", global_cooldown)
    )
