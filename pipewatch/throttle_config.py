"""Parse throttle configuration from a config dict."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.throttle import ThrottlePolicy

_DEFAULT_INTERVAL = 300


def parse_throttle_config(config: Dict[str, Any]) -> Dict[str, ThrottlePolicy]:
    """Parse the 'throttle' section of a pipewatch config.

    Expected structure::

        throttle:
          default:
            min_interval_seconds: 300
          pipelines:
            my_pipeline:
              min_interval_seconds: 60

    Returns a mapping of pipeline name -> ThrottlePolicy.
    'default' key maps to '__default__'.
    """
    section = config.get("throttle", {})
    default_interval = (
        section.get("default", {}).get("min_interval_seconds", _DEFAULT_INTERVAL)
    )
    default_policy = ThrottlePolicy(min_interval_seconds=default_interval)

    policies: Dict[str, ThrottlePolicy] = {"__default__": default_policy}

    for pipeline, cfg in section.get("pipelines", {}).items():
        interval = cfg.get("min_interval_seconds", default_interval)
        policies[pipeline] = ThrottlePolicy(min_interval_seconds=interval)

    return policies


def throttle_policy_for(
    pipeline: str, policies: Dict[str, ThrottlePolicy]
) -> ThrottlePolicy:
    """Return the ThrottlePolicy for a pipeline, falling back to default."""
    return policies.get(pipeline, policies.get("__default__", ThrottlePolicy()))
