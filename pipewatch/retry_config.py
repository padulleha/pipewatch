"""Parse retry policy from config dict."""
from __future__ import annotations
from typing import Any, Dict
from pipewatch.retry import RetryPolicy

_DEFAULTS = {
    "max_attempts": 3,
    "delay_seconds": 1.0,
    "backoff_factor": 2.0,
}


def parse_retry_policy(config: Dict[str, Any]) -> RetryPolicy:
    """Build a RetryPolicy from a config block (e.g. from YAML).

    Example config block::

        retry:
          max_attempts: 5
          delay_seconds: 0.5
          backoff_factor: 1.5
    """
    raw = config.get("retry", {})
    return RetryPolicy(
        max_attempts=int(raw.get("max_attempts", _DEFAULTS["max_attempts"])),
        delay_seconds=float(raw.get("delay_seconds", _DEFAULTS["delay_seconds"])),
        backoff_factor=float(raw.get("backoff_factor", _DEFAULTS["backoff_factor"])),
    )


def retry_policy_for_pipeline(pipeline_cfg: Dict[str, Any]) -> RetryPolicy:
    """Return the retry policy for a specific pipeline config block."""
    return parse_retry_policy(pipeline_cfg)
