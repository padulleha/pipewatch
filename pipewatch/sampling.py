"""Metric sampling: probabilistic and interval-based sampling strategies."""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Dict, Optional

from pipewatch.metrics import EvaluationResult


@dataclass
class SamplingPolicy:
    """Controls how frequently evaluation results are forwarded downstream."""
    strategy: str = "always"          # always | probabilistic | interval
    rate: float = 1.0                 # for probabilistic: 0.0-1.0
    every_n: int = 1                  # for interval: forward every N-th result

    def __post_init__(self) -> None:
        if self.strategy not in ("always", "probabilistic", "interval"):
            raise ValueError(f"Unknown sampling strategy: {self.strategy!r}")
        if not (0.0 < self.rate <= 1.0):
            raise ValueError("rate must be in (0.0, 1.0]")
        if self.every_n < 1:
            raise ValueError("every_n must be >= 1")


@dataclass
class _SamplerState:
    counter: int = 0


class SamplingRegistry:
    """Tracks per-pipeline sampler state."""

    def __init__(self) -> None:
        self._states: Dict[str, _SamplerState] = {}

    def _state(self, key: str) -> _SamplerState:
        if key not in self._states:
            self._states[key] = _SamplerState()
        return self._states[key]

    def should_sample(self, pipeline: str, metric: str, policy: SamplingPolicy) -> bool:
        """Return True if this result should be forwarded based on the policy."""
        if policy.strategy == "always":
            return True

        key = f"{pipeline}:{metric}"
        state = self._state(key)
        state.counter += 1

        if policy.strategy == "probabilistic":
            return random.random() < policy.rate

        if policy.strategy == "interval":
            return (state.counter % policy.every_n) == 1

        return True  # fallback

    def reset(self, pipeline: Optional[str] = None, metric: Optional[str] = None) -> None:
        if pipeline is None:
            self._states.clear()
        elif metric is None:
            prefix = f"{pipeline}:"
            self._states = {k: v for k, v in self._states.items() if not k.startswith(prefix)}
        else:
            self._states.pop(f"{pipeline}:{metric}", None)
