"""Shadow mode middleware — processes alerts without forwarding them downstream.

Useful for testing new routing rules, thresholds, or channels in production
without affecting live alert delivery.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, Optional

from pipewatch.metrics import EvaluationResult


@dataclass
class ShadowResult:
    """Outcome of a single shadow-mode evaluation."""
    result: EvaluationResult
    would_fire: bool
    reason: str

    def __str__(self) -> str:
        fired = "WOULD FIRE" if self.would_fire else "suppressed"
        return f"[shadow] {self.result.metric.pipeline}/{self.result.metric.name} -> {fired}: {self.reason}"


@dataclass
class ShadowMiddleware:
    """Runs a probe callable in shadow mode and records outcomes without
    forwarding to the real downstream handler.

    Args:
        probe:      Callable that accepts an EvaluationResult and returns
                    (would_fire: bool, reason: str).  Mimics the logic of a
                    real channel or routing rule.
        downstream: Optional real handler to call *after* shadow evaluation.
                    When None the middleware is fully dark (no side-effects).
        enabled:    Master switch; when False the middleware is a transparent
                    pass-through to downstream.
    """
    probe: Callable[[EvaluationResult], tuple[bool, str]]
    downstream: Optional[Callable[[EvaluationResult], None]] = None
    enabled: bool = True
    _log: List[ShadowResult] = field(default_factory=list, init=False, repr=False)
    _total: int = field(default=0, init=False, repr=False)
    _would_fire_count: int = field(default=0, init=False, repr=False)

    def process(self, result: EvaluationResult) -> None:
        """Evaluate *result* through the probe and optionally forward downstream."""
        if self.enabled:
            try:
                would_fire, reason = self.probe(result)
            except Exception as exc:  # pragma: no cover
                would_fire, reason = False, f"probe error: {exc}"

            entry = ShadowResult(result=result, would_fire=would_fire, reason=reason)
            self._log.append(entry)
            self._total += 1
            if would_fire:
                self._would_fire_count += 1

        if self.downstream is not None:
            self.downstream(result)

    def stats(self) -> dict:
        """Return aggregate shadow-run statistics."""
        return {
            "total": self._total,
            "would_fire": self._would_fire_count,
            "suppressed": self._total - self._would_fire_count,
        }

    def log(self) -> List[ShadowResult]:
        """Return a copy of all recorded shadow results."""
        return list(self._log)

    def reset_stats(self) -> None:
        """Clear counters and log."""
        self._log.clear()
        self._total = 0
        self._would_fire_count = 0
