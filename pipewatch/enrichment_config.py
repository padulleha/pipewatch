"""Parse enrichment rules from pipewatch config dict."""
from __future__ import annotations

from typing import Any, Dict, List

from pipewatch.enrichment import EnrichmentRegistry, EnrichmentRule


def parse_enrichment_rules(config: Dict[str, Any]) -> EnrichmentRegistry:
    """Build an EnrichmentRegistry from a config dict.

    Expected config structure::

        enrichment:
          - pipeline: my_pipeline   # optional
            metric: row_count       # optional
            metadata:
              team: data-eng
              env: production
    """
    registry = EnrichmentRegistry()
    entries: List[Dict[str, Any]] = config.get("enrichment", []) or []
    for entry in entries:
        rule = EnrichmentRule(
            pipeline=entry.get("pipeline"),
            metric=entry.get("metric"),
            metadata=dict(entry.get("metadata") or {}),
        )
        registry.add_rule(rule)
    return registry
