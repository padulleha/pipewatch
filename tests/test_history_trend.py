"""Tests for pipewatch.history and pipewatch.trend."""

import json
import os
import tempfile

import pytest

from pipewatch.history import record_metric, get_history, clear_history
from pipewatch.trend import average, moving_average, trend_direction, is_anomaly


@pytest.fixture()
def tmp_path_hist(tmp_path):
    return str(tmp_path / "history.json")


# --- history tests ---

def test_record_and_retrieve(tmp_path_hist):
    record_metric("pipe1", "row_count", 100.0, path=tmp_path_hist)
    record_metric("pipe1", "row_count", 120.0, path=tmp_path_hist)
    entries = get_history("pipe1", "row_count", path=tmp_path_hist)
    assert len(entries) == 2
    assert entries[0]["value"] == 100.0
    assert entries[1]["value"] == 120.0


def test_get_history_limit(tmp_path_hist):
    for i in range(10):
        record_metric("pipe1", "latency", float(i), path=tmp_path_hist)
    entries = get_history("pipe1", "latency", limit=3, path=tmp_path_hist)
    assert len(entries) == 3
    assert entries[-1]["value"] == 9.0


def test_get_history_empty(tmp_path_hist):
    assert get_history("missing", "metric", path=tmp_path_hist) == []


def test_clear_specific_metric(tmp_path_hist):
    record_metric("pipe1", "row_count", 50.0, path=tmp_path_hist)
    record_metric("pipe1", "latency", 1.5, path=tmp_path_hist)
    clear_history("pipe1", "row_count", path=tmp_path_hist)
    assert get_history("pipe1", "row_count", path=tmp_path_hist) == []
    assert len(get_history("pipe1", "latency", path=tmp_path_hist)) == 1


def test_clear_all_pipeline(tmp_path_hist):
    record_metric("pipe1", "row_count", 50.0, path=tmp_path_hist)
    record_metric("pipe1", "latency", 1.5, path=tmp_path_hist)
    clear_history("pipe1", path=tmp_path_hist)
    assert get_history("pipe1", "row_count", path=tmp_path_hist) == []
    assert get_history("pipe1", "latency", path=tmp_path_hist) == []


# --- trend tests ---

def _entries(values):
    return [{"value": v, "timestamp": "2024-01-01T00:00:00"} for v in values]


def test_average():
    assert average(_entries([10, 20, 30])) == 20.0


def test_average_empty():
    assert average([]) is None


def test_moving_average():
    result = moving_average(_entries([1, 2, 3, 4, 5, 100]), window=3)
    assert result == pytest.approx((4 + 5 + 100) / 3)


def test_trend_direction_up():
    assert trend_direction(_entries([10, 11, 12, 13, 20])) == "up"


def test_trend_direction_down():
    assert trend_direction(_entries([20, 18, 15, 12, 10])) == "down"


def test_trend_direction_stable():
    assert trend_direction(_entries([10, 10, 10, 10, 10])) == "stable"


def test_is_anomaly_true():
    base = [10.0] * 20
    assert is_anomaly(_entries(base), 50.0)


def test_is_anomaly_false():
    base = [10.0] * 20
    assert not is_anomaly(_entries(base), 10.5)
