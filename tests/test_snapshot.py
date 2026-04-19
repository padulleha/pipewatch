"""Tests for pipewatch.snapshot."""
from __future__ import annotations

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.snapshot import (
    clear_snapshot,
    diff_snapshot,
    load_snapshot,
    save_snapshot,
)


def make_metric(name: str, value: float, unit: str = "") -> PipelineMetric:
    return PipelineMetric(pipeline="pipe", name=name, value=value, unit=unit)


def test_save_and_load(tmp_path):
    metrics = [make_metric("row_count", 100), make_metric("error_rate", 0.02)]
    save_snapshot("pipe_a", metrics, directory=str(tmp_path))
    snap = load_snapshot("pipe_a", directory=str(tmp_path))
    assert snap is not None
    assert snap["pipeline"] == "pipe_a"
    assert len(snap["metrics"]) == 2
    assert snap["metrics"][0]["name"] == "row_count"
    assert snap["metrics"][0]["value"] == 100


def test_load_missing_returns_none(tmp_path):
    assert load_snapshot("nonexistent", directory=str(tmp_path)) is None


def test_diff_no_previous(tmp_path):
    metrics = [make_metric("row_count", 50)]
    result = diff_snapshot("pipe_b", metrics, directory=str(tmp_path))
    assert result == []


def test_diff_with_previous(tmp_path):
    old = [make_metric("row_count", 80), make_metric("error_rate", 0.01)]
    save_snapshot("pipe_c", old, directory=str(tmp_path))
    new = [make_metric("row_count", 100), make_metric("error_rate", 0.005)]
    diffs = diff_snapshot("pipe_c", new, directory=str(tmp_path))
    assert len(diffs) == 2
    rc = next(d for d in diffs if d["name"] == "row_count")
    assert rc["delta"] == pytest.approx(20)
    er = next(d for d in diffs if d["name"] == "error_rate")
    assert er["delta"] == pytest.approx(-0.005)


def test_clear_snapshot(tmp_path):
    save_snapshot("pipe_d", [make_metric("x", 1)], directory=str(tmp_path))
    assert clear_snapshot("pipe_d", directory=str(tmp_path)) is True
    assert load_snapshot("pipe_d", directory=str(tmp_path)) is None


def test_clear_missing_returns_false(tmp_path):
    assert clear_snapshot("ghost", directory=str(tmp_path)) is False


def test_overwrite_snapshot(tmp_path):
    save_snapshot("pipe_e", [make_metric("rows", 10)], directory=str(tmp_path))
    save_snapshot("pipe_e", [make_metric("rows", 99)], directory=str(tmp_path))
    snap = load_snapshot("pipe_e", directory=str(tmp_path))
    assert snap["metrics"][0]["value"] == 99
