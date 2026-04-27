"""Parse debounce configuration from a config dict."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.debounce import DebouncePolicy

_DEFAULT_MIN_CONSECUTIVE = 2


def parse_debounce_policy(cfg: Dict[str, Any]) -> DebouncePolicy:
    """Build a DebouncePolicy from a raw config mapping.

    Expected shape::

        debounce:
          min_consecutive: 3
    """
    debounce_cfg = cfg.get("debounce", {})
    min_consecutive = int(
        debounce_cfg.get("min_consecutive", _DEFAULT_MIN_CONSECUTIVE)
    )
    return DebouncePolicy(min_consecutive=min_consecutive)


def debounce_policy_for(pipeline: str, cfg: Dict[str, Any]) -> DebouncePolicy:
    """Return a DebouncePolicy for a specific pipeline, falling back to global config."""
    pipelines_cfg: Dict[str, Any] = cfg.get("pipelines", {})
    pipeline_cfg: Dict[str, Any] = pipelines_cfg.get(pipeline, {})

    if "debounce" in pipeline_cfg:
        return parse_debounce_policy(pipeline_cfg)
    return parse_debounce_policy(cfg)
