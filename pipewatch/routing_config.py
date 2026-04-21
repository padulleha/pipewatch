"""Parse routing configuration from a config dict."""
from __future__ import annotations

from typing import Any, Dict, List

from pipewatch.routing import RoutingRule, AlertRouter
from pipewatch.alert_config import parse_alert_channels


def parse_routing_rules(cfg: Dict[str, Any]) -> List[RoutingRule]:
    """Parse a list of routing rule dicts from config.

    Expected shape inside cfg::

        routing:
          rules:
            - channels: [email]
              pipeline: sales
              min_status: critical
            - channels: [log]
              min_status: warning
    """
    rules_cfg = cfg.get("routing", {}).get("rules", [])
    rules: List[RoutingRule] = []
    for entry in rules_cfg:
        channels = entry.get("channels", [])
        if not channels:
            continue
        rules.append(
            RoutingRule(
                channels=channels,
                pipeline=entry.get("pipeline"),
                metric=entry.get("metric"),
                min_status=entry.get("min_status", "warning"),
            )
        )
    return rules


def build_router_from_config(cfg: Dict[str, Any]) -> AlertRouter:
    """Build a fully wired AlertRouter from a config dict."""
    router = AlertRouter()

    # Wire channels
    for name, channel in parse_alert_channels(cfg).items():
        router.add_channel(name, channel)

    # Wire rules
    router.rules = parse_routing_rules(cfg)

    # Default channels fallback
    defaults = cfg.get("routing", {}).get("default_channels", [])
    router.set_default_channels(defaults)

    return router
