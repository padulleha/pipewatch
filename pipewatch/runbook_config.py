"""Parse runbook configuration from a YAML config dict."""
from __future__ import annotations

from typing import Any

from pipewatch.runbook import RunbookEntry, RunbookRegistry


def parse_runbook_entries(config: dict[str, Any]) -> RunbookRegistry:
    """Build a RunbookRegistry from the top-level config dict.

    Expected shape::

        runbooks:
          - pipeline: my_pipeline
            metric: row_count
            url: https://wiki.example.com/runbooks/row_count
            notes: "Check source table for ingestion failures."
            tags:
              - data-quality
    """
    registry = RunbookRegistry()
    entries_raw = config.get("runbooks", [])
    if not isinstance(entries_raw, list):
        return registry

    for raw in entries_raw:
        if not isinstance(raw, dict):
            continue
        pipeline = raw.get("pipeline", "")
        metric = raw.get("metric", "")
        if not pipeline or not metric:
            continue
        entry = RunbookEntry(
            pipeline=pipeline,
            metric=metric,
            url=raw.get("url"),
            notes=raw.get("notes"),
            tags=raw.get("tags") or [],
        )
        registry.add(entry)

    return registry
