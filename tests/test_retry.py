"""Tests for retry execution and config parsing."""
import pytest
from unittest.mock import MagicMock
from pipewatch.retry import RetryPolicy, RetryResult, execute_with_retry
from pipewatch.retry_config import parse_retry_policy, retry_policy_for_pipeline


# ---------------------------------------------------------------------------
# execute_with_retry
# ---------------------------------------------------------------------------

def test_success_on_first_attempt():
    fn = MagicMock(return_value=42)
    policy = RetryPolicy(max_attempts=3, delay_seconds=0, backoff_factor=1.0)
    result = execute_with_retry(fn, policy)
    assert result.success is True
    assert result.value == 42
    assert result.attempts == 1
    fn.assert_called_once()


def test_success_after_retries():
    fn = MagicMock(side_effect=[ValueError("bad"), ValueError("bad"), 99])
    policy = RetryPolicy(max_attempts=3, delay_seconds=0, backoff_factor=1.0)
    result = execute_with_retry(fn, policy)
    assert result.success is True
    assert result.value == 99
    assert result.attempts == 3


def test_failure_exhausted():
    fn = MagicMock(side_effect=RuntimeError("boom"))
    policy = RetryPolicy(max_attempts=3, delay_seconds=0, backoff_factor=1.0)
    result = execute_with_retry(fn, policy)
    assert result.success is False
    assert result.attempts == 3
    assert "boom" in result.last_error


def test_non_matching_exception_propagates():
    fn = MagicMock(side_effect=KeyError("missing"))
    policy = RetryPolicy(max_attempts=3, delay_seconds=0, backoff_factor=1.0, exceptions=(ValueError,))
    with pytest.raises(KeyError):
        execute_with_retry(fn, policy)


def test_retry_result_str_success():
    r = RetryResult(success=True, value=1, attempts=2)
    assert "OK" in str(r)
    assert "2" in str(r)


def test_retry_result_str_failure():
    r = RetryResult(success=False, value=None, attempts=3, last_error="oops")
    assert "FAILED" in str(r)
    assert "oops" in str(r)


# ---------------------------------------------------------------------------
# retry_config
# ---------------------------------------------------------------------------

def test_parse_defaults():
    policy = parse_retry_policy({})
    assert policy.max_attempts == 3
    assert policy.delay_seconds == 1.0
    assert policy.backoff_factor == 2.0


def test_parse_custom_values():
    cfg = {"retry": {"max_attempts": 5, "delay_seconds": 0.5, "backoff_factor": 1.5}}
    policy = parse_retry_policy(cfg)
    assert policy.max_attempts == 5
    assert policy.delay_seconds == 0.5
    assert policy.backoff_factor == 1.5


def test_retry_policy_for_pipeline():
    pipeline_cfg = {"name": "my_pipe", "retry": {"max_attempts": 2}}
    policy = retry_policy_for_pipeline(pipeline_cfg)
    assert policy.max_attempts == 2
    assert policy.delay_seconds == 1.0  # default
