"""Load and validate pipewatch configuration from a YAML file."""

import os
from typing import Any

import yaml

from pipewatch.metrics import ThresholdRule

DEFAULT_CONFIG_PATH = os.path.expanduser("~/.pipewatch/config.yaml")


def load_config(path: str = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    """Load YAML config from disk."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r") as f:
        data = yaml.safe_load(f) or {}
    return data


def parse_rules(config: dict[str, Any]) -> list[ThresholdRule]:
    """Parse threshold rules from config dict."""
    rules = []
    for entry in config.get("rules", []):
        rule = ThresholdRule(
            metric_name=entry["metric"],
            warning_threshold=entry.get("warning"),
            critical_threshold=entry.get("critical"),
            operator=entry.get("operator", "gt"),
        )
        rules.append(rule)
    return rules


def get_pipelines(config: dict[str, Any]) -> list[dict]:
    """Return list of pipeline definitions from config."""
    return config.get("pipelines", [])


EXAMPLE_CONFIG = """
pipelines:
  - name: daily_etl
    source: postgres
    schedule: "0 2 * * *"

rules:
  - metric: row_count
    warning: 1000
    critical: 500
    operator: lt
  - metric: error_rate
    warning: 0.05
    critical: 0.10
    operator: gt

alerts:
  slack_webhook: https://hooks.slack.com/services/XXX
"""
