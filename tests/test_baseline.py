"""Tests for pipewatch.baseline and pipewatch.baseline_config."""

from __future__ import annotations

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.baseline import (
    save_baseline,
    load_baseline,
    compare_to_baseline,
    clear_baseline,
)
from pipewatch.baseline_config import parse_baseline_config


def make_metric(pipeline="pipe_a", name="row_count", value=100.0) -> PipelineMetric:
    return PipelineMetric(pipeline=pipeline, name=name, value=value)


# ── baseline store/load ────────────────────────────────────────────────────────

def test_save_and_load(tmp_path):
    m = make_metric(value=42.0)
    save_baseline(m, directory=str(tmp_path))
    result = load_baseline(m.pipeline, m.name, directory=str(tmp_path))
    assert result == 42.0


def test_load_missing_returns_none(tmp_path):
    assert load_baseline("no_pipe", "no_metric", directory=str(tmp_path)) is None


def test_overwrite_baseline(tmp_path):
    m = make_metric(value=10.0)
    save_baseline(m, directory=str(tmp_path))
    save_baseline(make_metric(value=99.0), directory=str(tmp_path))
    assert load_baseline(m.pipeline, m.name, directory=str(tmp_path)) == 99.0


# ── compare_to_baseline ────────────────────────────────────────────────────────

def test_compare_no_baseline(tmp_path):
    assert compare_to_baseline(make_metric(), directory=str(tmp_path)) is None


def test_compare_delta(tmp_path):
    save_baseline(make_metric(value=100.0), directory=str(tmp_path))
    result = compare_to_baseline(make_metric(value=120.0), directory=str(tmp_path))
    assert result["delta"] == pytest.approx(20.0)
    assert result["delta_pct"] == pytest.approx(20.0)
    assert result["baseline"] == 100.0
    assert result["current"] == 120.0


def test_compare_zero_baseline(tmp_path):
    save_baseline(make_metric(value=0.0), directory=str(tmp_path))
    result = compare_to_baseline(make_metric(value=5.0), directory=str(tmp_path))
    assert result["delta_pct"] is None


# ── clear_baseline ─────────────────────────────────────────────────────────────

def test_clear_existing(tmp_path):
    m = make_metric()
    save_baseline(m, directory=str(tmp_path))
    assert clear_baseline(m.pipeline, m.name, directory=str(tmp_path)) is True
    assert load_baseline(m.pipeline, m.name, directory=str(tmp_path)) is None


def test_clear_missing(tmp_path):
    assert clear_baseline("x", "y", directory=str(tmp_path)) is False


# ── baseline_config ────────────────────────────────────────────────────────────

def test_default_config():
    cfg = parse_baseline_config({})
    assert cfg["auto_save"] is False
    assert cfg["pipelines"] == []
    assert "baselines" in cfg["directory"]


def test_custom_config():
    raw = {"baseline": {"auto_save": True, "pipelines": ["p1", "p2"], "directory": "/tmp/bl"}}
    cfg = parse_baseline_config(raw)
    assert cfg["auto_save"] is True
    assert cfg["pipelines"] == ["p1", "p2"]
    assert cfg["directory"] == "/tmp/bl"
