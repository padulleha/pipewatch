"""Tests for report generation and formatters."""
import json
import pytest
from unittest.mock import patch

from pipewatch.metrics import PipelineMetric, ThresholdRule
from pipewatch.report import build_report, PipelineReport
from pipewatch.formatters import format_text, format_json, format_markdown, get_formatter


PIPELINE = "test_pipe"


def make_metric(name="row_count", value=100.0):
    return PipelineMetric(pipeline=PIPELINE, name=name, value=value)


def make_rule(name="row_count", warn=50.0, crit=10.0):
    return ThresholdRule(metric_name=name, warning_below=warn, critical_below=crit)


@patch("pipewatch.report.get_history", return_value=[])
def test_build_report_basic(mock_hist):
    metrics = [make_metric()]
    rules = [make_rule()]
    report = build_report(PIPELINE, metrics, rules)
    assert report.pipeline == PIPELINE
    assert len(report.entries) == 1
    entry = report.entries[0]
    assert entry["metric"] == "row_count"
    assert entry["value"] == 100.0
    assert entry["status"] == "ok"


@patch("pipewatch.report.get_history", return_value=[])
def test_build_report_no_rule(mock_hist):
    metrics = [make_metric()]
    report = build_report(PIPELINE, metrics, rules=[])
    assert report.entries[0]["status"] == "unknown"


@patch("pipewatch.report.get_history", return_value=[{"value": v} for v in [80, 60, 40, 20]])
def test_build_report_trend_and_anomaly(mock_hist):
    metrics = [make_metric(value=5.0)]
    rules = [make_rule()]
    report = build_report(PIPELINE, metrics, rules)
    entry = report.entries[0]
    assert entry["trend"] in ("up", "down", "stable")


def _sample_report():
    r = PipelineReport(pipeline="pipe1")
    r.entries = [{"metric": "rows", "value": 99, "status": "ok", "trend": "up", "anomaly": False}]
    return r


def test_format_text():
    out = format_text(_sample_report())
    assert "pipe1" in out
    assert "rows" in out
    assert "OK" in out


def test_format_json():
    out = format_json(_sample_report())
    data = json.loads(out)
    assert data["pipeline"] == "pipe1"
    assert data["entries"][0]["metric"] == "rows"


def test_format_markdown():
    out = format_markdown(_sample_report())
    assert "| rows |" in out
    assert "# Pipeline Report" in out


def test_get_formatter_invalid():
    with pytest.raises(ValueError, match="Unknown format"):
        get_formatter("xml")


def test_get_formatter_valid():
    fmt = get_formatter("json")
    assert callable(fmt)
