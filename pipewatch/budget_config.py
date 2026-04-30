"""Config parsing for alert budget policies."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.budget import BudgetPolicy, BudgetRegistry


def parse_budget_policies(
    config: Dict[str, Any],
) -> Dict[str, BudgetPolicy]:
    """Parse a ``budgets`` mapping from the top-level config dict.

    Expected shape::

        budgets:
          defaults:
            max_alerts: 50
            window_seconds: 1800
          pipelines:
            my_pipeline:
              max_alerts: 10
              window_seconds: 600
    """
    section = config.get("budgets", {})
    policies: Dict[str, BudgetPolicy] = {}

    defaults_raw = section.get("defaults", {})
    default_policy = BudgetPolicy(
        max_alerts=int(defaults_raw.get("max_alerts", 100)),
        window_seconds=int(defaults_raw.get("window_seconds", 3600)),
    )
    policies["__default__"] = default_policy

    for pipeline, raw in section.get("pipelines", {}).items():
        policies[pipeline] = BudgetPolicy(
            max_alerts=int(raw.get("max_alerts", default_policy.max_alerts)),
            window_seconds=int(
                raw.get("window_seconds", default_policy.window_seconds)
            ),
        )

    return policies


def build_budget_registry_from_config(
    config: Dict[str, Any],
) -> BudgetRegistry:
    """Build a :class:`BudgetRegistry` from the top-level config dict."""
    policies = parse_budget_policies(config)
    registry = BudgetRegistry()

    default = policies.pop("__default__", BudgetPolicy())
    registry.set_default(default)

    for pipeline, policy in policies.items():
        registry.set_policy(pipeline, policy)

    return registry
