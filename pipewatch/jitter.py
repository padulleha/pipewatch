"""Jitter middleware: adds randomised delay before forwarding alerts to
downstream channels, preventing thundering-herd spikes when many pipelines
fire simultaneously."""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from pipewatch.metrics import EvaluationResult


@dataclass
class JitterPolicy:
    min_seconds: float = 0.0
    max_seconds: float = 1.0

    def __post_init__(self) -> None:
        if self.min_seconds < 0:
            raise ValueError("min_seconds must be >= 0")
        if self.max_seconds < self.min_seconds:
            raise ValueError("max_seconds must be >= min_seconds")

    def sample(self, rng: Optional[random.Random] = None) -> float:
        r = rng or random
        return r.uniform(self.min_seconds, self.max_seconds)


@dataclass
class JitterStats:
    total: int = 0
    total_delay_seconds: float = 0.0

    @property
    def average_delay(self) -> float:
        return self.total_delay_seconds / self.total if self.total else 0.0


DispatchFn = Callable[[EvaluationResult], None]


class JitterMiddleware:
    """Wraps a downstream dispatch function with a configurable random delay."""

    def __init__(
        self,
        downstream: DispatchFn,
        policy: Optional[JitterPolicy] = None,
        *,
        sleep_fn: Callable[[float], None] = time.sleep,
        rng: Optional[random.Random] = None,
    ) -> None:
        self._downstream = downstream
        self._policy = policy or JitterPolicy()
        self._sleep = sleep_fn
        self._rng = rng
        self._stats = JitterStats()

    def set_policy(self, policy: JitterPolicy) -> None:
        self._policy = policy

    def process(self, result: EvaluationResult) -> None:
        delay = self._policy.sample(self._rng)
        if delay > 0:
            self._sleep(delay)
        self._stats.total += 1
        self._stats.total_delay_seconds += delay
        self._downstream(result)

    def stats(self) -> dict:
        return {
            "total": self._stats.total,
            "total_delay_seconds": round(self._stats.total_delay_seconds, 6),
            "average_delay_seconds": round(self._stats.average_delay, 6),
        }

    def reset_stats(self) -> None:
        self._stats = JitterStats()
