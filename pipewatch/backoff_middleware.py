"""Middleware that wraps alert dispatch with exponential backoff on failure."""
from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from pipewatch.backoff import BackoffPolicy, execute_with_backoff
from pipewatch.metrics import EvaluationResult


class BackoffMiddleware:
    """Retries downstream dispatch using exponential backoff when it fails."""

    def __init__(
        self,
        downstream: Callable[[EvaluationResult], bool],
        default_policy: Optional[BackoffPolicy] = None,
    ) -> None:
        self._downstream = downstream
        self._default_policy = default_policy or BackoffPolicy()
        self._stats: Dict[str, int] = {
            "processed": 0,
            "succeeded": 0,
            "exhausted": 0,
            "retries": 0,
        }

    def set_policy(self, policy: BackoffPolicy) -> None:
        self._default_policy = policy

    def process(self, result: EvaluationResult) -> bool:
        self._stats["processed"] += 1
        attempts_before = [0]

        def attempt() -> bool:
            ok = self._downstream(result)
            attempts_before[0] += 1
            return ok

        state = execute_with_backoff(
            attempt,
            self._default_policy,
            sleep_fn=lambda _: None,  # non-blocking in middleware context
        )
        extra_retries = max(0, state.attempts - 1)
        self._stats["retries"] += extra_retries

        if state.exhausted() and state.attempts >= self._default_policy.max_attempts:
            # Check if last attempt succeeded by re-examining state
            succeeded = not state.exhausted() or extra_retries < self._default_policy.max_attempts - 1
        else:
            succeeded = True

        # Simpler: track via a flag
        return self._finalize(result, state)

    def _finalize(self, result: EvaluationResult, state: Any) -> bool:
        # Re-run one final check: did the last attempt succeed?
        # We trust execute_with_backoff returned after success or exhaustion.
        if state.attempts < state.policy.max_attempts:
            self._stats["succeeded"] += 1
            return True
        self._stats["exhausted"] += 1
        return False

    def stats(self) -> Dict[str, int]:
        return dict(self._stats)

    def reset_stats(self) -> None:
        for k in self._stats:
            self._stats[k] = 0
