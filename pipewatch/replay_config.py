"""Parse replay configuration from a config dict."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from pipewatch.metrics import ThresholdRule


@dataclass
class ReplayConfig:
    pipeline: str
    metric_name: str
    rule: ThresholdRule
    limit: int = 100
    history_path: Optional[str] = None


def parse_replay_configs(raw: Dict[str, Any]) -> List[ReplayConfig]:
    """Parse a list of replay entries from a config mapping.

    Expected structure::

        replay:
          - pipeline: my_pipeline
            metric: row_count
            warning_above: 1000
            critical_above: 5000
            limit: 50
    """
    entries = raw.get("replay", [])
    configs: List[ReplayConfig] = []

    for entry in entries:
        pipeline = entry.get("pipeline", "")
        metric_name = entry.get("metric", "")
        if not pipeline or not metric_name:
            continue

        rule = ThresholdRule(
            metric_name=metric_name,
            warning_above=entry.get("warning_above"),
            critical_above=entry.get("critical_above"),
            warning_below=entry.get("warning_below"),
            critical_below=entry.get("critical_below"),
        )

        configs.append(
            ReplayConfig(
                pipeline=pipeline,
                metric_name=metric_name,
                rule=rule,
                limit=int(entry.get("limit", 100)),
                history_path=entry.get("history_path"),
            )
        )

    return configs
