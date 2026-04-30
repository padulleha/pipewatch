"""Parse SLA policy configuration from a config dict."""
from __future__ import annotations

from typing import Any, Dict, List

from pipewatch.sla import SLAPolicy, SLATracker


def parse_sla_policies(config: Dict[str, Any]) -> List[SLAPolicy]:
    """Parse a list of SLA policy definitions from the top-level config dict.

    Expected config shape::

        sla:
          - pipeline: my_pipeline
            metric: row_count
            max_critical_rate: 0.02
            max_warning_rate: 0.10
            window_minutes: 30
    """
    raw = config.get("sla", [])
    if not isinstance(raw, list):
        return []

    policies: List[SLAPolicy] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        pipeline = entry.get("pipeline", "").strip()
        metric = entry.get("metric", "").strip()
        if not pipeline or not metric:
            continue
        kwargs: Dict[str, Any] = {"pipeline": pipeline, "metric": metric}
        if "max_critical_rate" in entry:
            kwargs["max_critical_rate"] = float(entry["max_critical_rate"])
        if "max_warning_rate" in entry:
            kwargs["max_warning_rate"] = float(entry["max_warning_rate"])
        if "window_minutes" in entry:
            kwargs["window_minutes"] = int(entry["window_minutes"])
        policies.append(SLAPolicy(**kwargs))
    return policies


def build_sla_tracker_from_config(config: Dict[str, Any]) -> SLATracker:
    """Build a ready-to-use SLATracker populated with policies from config."""
    tracker = SLATracker()
    for policy in parse_sla_policies(config):
        tracker.add_policy(policy)
    return tracker
