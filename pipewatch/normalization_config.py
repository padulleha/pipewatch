"""Parse normalization rules from config dictionaries."""
from __future__ import annotations

from typing import Any

from pipewatch.normalization import NormalizationRule, NormalizationRegistry


def parse_normalization_rules(config: dict[str, Any]) -> NormalizationRegistry:
    """Build a NormalizationRegistry from a config dict.

    Expected shape::

        normalization:
          rules:
            - pipeline: my_pipeline
              metric: row_count
              scale: 0.001
              offset: 0.0
              clamp_min: 0.0
              clamp_max: 1000.0
    """
    registry = NormalizationRegistry()
    section = config.get("normalization", {})
    rules_raw = section.get("rules", [])

    for entry in rules_raw:
        if not isinstance(entry, dict):
            continue
        rule = NormalizationRule(
            pipeline=entry.get("pipeline") or None,
            metric=entry.get("metric") or None,
            scale=float(entry.get("scale", 1.0)),
            offset=float(entry.get("offset", 0.0)),
            clamp_min=float(entry["clamp_min"]) if "clamp_min" in entry else None,
            clamp_max=float(entry["clamp_max"]) if "clamp_max" in entry else None,
        )
        registry.add_rule(rule)

    return registry
