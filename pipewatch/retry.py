"""Retry policy configuration and execution for pipeline checks."""
from __future__ import annotations
import time
from dataclasses import dataclass, field
from typing import Callable, Any, Optional


@dataclass
class RetryPolicy:
    max_attempts: int = 3
    delay_seconds: float = 1.0
    backoff_factor: float = 2.0
    exceptions: tuple = (Exception,)


@dataclass
class RetryResult:
    success: bool
    value: Any
    attempts: int
    last_error: Optional[str] = None

    def __str__(self) -> str:
        if self.success:
            return f"OK after {self.attempts} attempt(s)"
        return f"FAILED after {self.attempts} attempt(s): {self.last_error}"


def execute_with_retry(fn: Callable, policy: RetryPolicy, *args, **kwargs) -> RetryResult:
    """Execute fn according to policy, retrying on allowed exceptions."""
    delay = policy.delay_seconds
    last_error = None
    for attempt in range(1, policy.max_attempts + 1):
        try:
            result = fn(*args, **kwargs)
            return RetryResult(success=True, value=result, attempts=attempt)
        except policy.exceptions as exc:
            last_error = str(exc)
            if attempt < policy.max_attempts:
                time.sleep(delay)
                delay *= policy.backoff_factor
    return RetryResult(success=False, value=None, attempts=policy.max_attempts, last_error=last_error)
