"""Tests for pipewatch.replay and pipewatch.replay_config."""

from __future__ import annotations

import pytest

from pipewatch.metrics import ThresholdRule
from pipewatch.replay import ReplayResult, replay_pipeline
from pipewatch.replay_config import parse_replay_configs


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_history(tmp_path, pipeline, metric, values):
    """Seed the history store with a list of float values."""
    from pipewatch.history import record_metric, clear_history
    clear_history(pipeline, metric, path=str(tmp_path))
    for v in values:
        record_metric(pipeline, metric, v, path=str(tmp_path))


# ---------------------------------------------------------------------------
# ReplayResult
# ---------------------------------------------------------------------------

class TestReplayResult:
    def test_fire_rate_zero_total(self):
        r = ReplayResult(pipeline="p", metric_name="m", total=0, fired=0)
        assert r.fire_rate == 0.0

    def test_fire_rate_partial(self):
        r = ReplayResult(pipeline="p", metric_name="m", total=4, fired=1)
        assert r.fire_rate == pytest.approx(0.25)

    def test_summary_format(self):
        r = ReplayResult(pipeline="etl", metric_name="rows", total=10, fired=3)
        s = r.summary()
        assert "etl/rows" in s
        assert "3/10" in s
        assert "30.0%" in s


# ---------------------------------------------------------------------------
# replay_pipeline
# ---------------------------------------------------------------------------

def test_replay_empty_history(tmp_path):
    rule = ThresholdRule(metric_name="rows", warning_above=100)
    result = replay_pipeline("p", "rows", rule, history_path=str(tmp_path))
    assert result.total == 0
    assert result.fired == 0


def test_replay_no_alerts(tmp_path):
    _make_history(tmp_path, "p", "rows", [10, 20, 30])
    rule = ThresholdRule(metric_name="rows", warning_above=50)
    result = replay_pipeline("p", "rows", rule, history_path=str(tmp_path))
    assert result.total == 3
    assert result.fired == 0


def test_replay_some_alerts(tmp_path):
    _make_history(tmp_path, "p", "rows", [10, 60, 80, 20])
    rule = ThresholdRule(metric_name="rows", warning_above=50)
    result = replay_pipeline("p", "rows", rule, history_path=str(tmp_path))
    assert result.total == 4
    assert result.fired == 2


def test_replay_on_result_callback(tmp_path):
    _make_history(tmp_path, "p", "m", [5, 200])
    rule = ThresholdRule(metric_name="m", critical_above=100)
    seen = []
    replay_pipeline("p", "m", rule, history_path=str(tmp_path), on_result=seen.append)
    assert len(seen) == 2


def test_replay_respects_limit(tmp_path):
    _make_history(tmp_path, "p", "m", list(range(20)))
    rule = ThresholdRule(metric_name="m", warning_above=999)
    result = replay_pipeline("p", "m", rule, limit=5, history_path=str(tmp_path))
    assert result.total == 5


# ---------------------------------------------------------------------------
# parse_replay_configs
# ---------------------------------------------------------------------------

def test_parse_empty_config():
    assert parse_replay_configs({}) == []


def test_parse_skips_missing_fields():
    raw = {"replay": [{"pipeline": "p"}]}  # missing metric
    assert parse_replay_configs(raw) == []


def test_parse_basic_entry():
    raw = {
        "replay": [
            {
                "pipeline": "etl",
                "metric": "row_count",
                "warning_above": 500,
                "critical_above": 1000,
                "limit": 25,
            }
        ]
    }
    configs = parse_replay_configs(raw)
    assert len(configs) == 1
    c = configs[0]
    assert c.pipeline == "etl"
    assert c.metric_name == "row_count"
    assert c.rule.warning_above == 500
    assert c.rule.critical_above == 1000
    assert c.limit == 25


def test_parse_default_limit():
    raw = {"replay": [{"pipeline": "p", "metric": "m"}]}
    configs = parse_replay_configs(raw)
    assert configs[0].limit == 100
