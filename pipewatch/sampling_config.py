"""Parse sampling configuration from a config dict."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.sampling import SamplingPolicy

_DEFAULTS: Dict[str, Any] = {
    "strategy": "always",
    "rate": 1.0,
    "every_n": 1,
}


def parse_sampling_policy(cfg: Dict[str, Any]) -> SamplingPolicy:
    """Build a SamplingPolicy from a raw config mapping."""
    strategy = cfg.get("strategy", _DEFAULTS["strategy"])
    rate = float(cfg.get("rate", _DEFAULTS["rate"]))
    every_n = int(cfg.get("every_n", _DEFAULTS["every_n"]))
    return SamplingPolicy(strategy=strategy, rate=rate, every_n=every_n)


def sampling_policy_for(pipeline: str, config: Dict[str, Any]) -> SamplingPolicy:
    """Return the SamplingPolicy for a given pipeline, falling back to global defaults."""
    sampling_cfg: Dict[str, Any] = config.get("sampling", {})

    # Pipeline-level override takes priority
    pipelines_cfg = config.get("pipelines", {})
    pipeline_sampling = pipelines_cfg.get(pipeline, {}).get("sampling", {})

    merged = {**_DEFAULTS, **sampling_cfg, **pipeline_sampling}
    return parse_sampling_policy(merged)
