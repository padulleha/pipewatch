"""Tests for pipewatch.snapshot_report."""
from __future__ import annotations

from pipewatch.snapshot_report import (
    format_diff_json,
    format_diff_lines,
    format_diff_text,
)


def _diff(name, prev, curr):
    return {"name": name, "previous": prev, "current": curr, "delta": curr - prev}


def test_format_diff_lines_empty():
    lines = format_diff_lines([])
    assert len(lines) == 1
    assert "no previous" in lines[0]


def test_format_diff_lines_increase():
    lines = format_diff_lines([_diff("row_count", 80, 100)])
    assert len(lines) == 1
    assert "▲" in lines[0]
    assert "row_count" in lines[0]


def test_format_diff_lines_decrease():
    lines = format_diff_lines([_diff("error_rate", 0.05, 0.02)])
    assert "▼" in lines[0]


def test_format_diff_lines_unchanged():
    lines = format_diff_lines([_diff("latency", 5.0, 5.0)])
    assert "=" in lines[0]


def test_format_diff_text_header():
    text = format_diff_text("my_pipe", [_diff("rows", 10, 20)])
    assert text.startswith("Snapshot diff for 'my_pipe':")
    assert "rows" in text


def test_format_diff_json_structure():
    diffs = [_diff("rows", 10, 15)]
    result = format_diff_json("pipe_x", diffs)
    assert result["pipeline"] == "pipe_x"
    assert result["diffs"] == diffs
