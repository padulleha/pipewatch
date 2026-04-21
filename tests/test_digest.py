"""Tests for pipewatch.digest and pipewatch.digest_config."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta

import pytest

from pipewatch.alerts import AlertEvent
from pipewatch.digest import build_digest, DigestReport
from pipewatch.digest_config import parse_digest_config, DigestConfig


def _evt(pipeline: str, metric: str, status: str, seconds_ago: float = 0.0) -> AlertEvent:
    ts = datetime.now(tz=timezone.utc) - timedelta(seconds=seconds_ago)
    return AlertEvent(
        pipeline=pipeline,
        metric_name=metric,
        status=status,
        value=1.0,
        threshold=0.5,
        timestamp=ts,
        message=f"{pipeline}/{metric} is {status}",
    )


class TestBuildDigest:
    def test_empty_events_returns_empty_report(self):
        report = build_digest([])
        assert isinstance(report, DigestReport)
        assert report.entries == []
        assert report.total_alerts == 0

    def test_single_event_creates_one_entry(self):
        report = build_digest([_evt("pipe_a", "row_count", "warning")])
        assert len(report.entries) == 1
        e = report.entries[0]
        assert e.pipeline == "pipe_a"
        assert e.metric_name == "row_count"
        assert e.status == "warning"
        assert e.count == 1

    def test_duplicate_events_are_aggregated(self):
        events = [_evt("pipe_a", "row_count", "warning", seconds_ago=i) for i in range(5)]
        report = build_digest(events)
        assert len(report.entries) == 1
        assert report.entries[0].count == 5
        assert report.total_alerts == 5

    def test_different_statuses_create_separate_entries(self):
        events = [
            _evt("pipe_a", "row_count", "warning"),
            _evt("pipe_a", "row_count", "critical"),
        ]
        report = build_digest(events)
        assert len(report.entries) == 2

    def test_events_outside_window_are_excluded(self):
        old_event = _evt("pipe_a", "row_count", "warning", seconds_ago=7200)
        recent_event = _evt("pipe_a", "row_count", "warning", seconds_ago=10)
        report = build_digest([old_event, recent_event], window_seconds=3600)
        assert len(report.entries) == 1
        assert report.entries[0].count == 1

    def test_to_text_contains_pipeline_name(self):
        report = build_digest([_evt("my_pipeline", "latency", "critical")])
        text = report.to_text()
        assert "my_pipeline" in text
        assert "CRITICAL" in text

    def test_to_text_empty_window(self):
        report = build_digest([])
        text = report.to_text()
        assert "No alerts" in text

    def test_window_seconds_stored(self):
        report = build_digest([], window_seconds=1800)
        assert report.window_seconds == 1800


class TestParseDigestConfig:
    def test_defaults_when_section_missing(self):
        cfg = parse_digest_config({})
        assert cfg.enabled is True
        assert cfg.window_seconds == 3600
        assert cfg.channels == ["log"]

    def test_custom_values(self):
        raw = {"digest": {"enabled": False, "window_seconds": 900, "channels": ["email"]}}
        cfg = parse_digest_config(raw)
        assert cfg.enabled is False
        assert cfg.window_seconds == 900
        assert cfg.channels == ["email"]

    def test_invalid_window_raises(self):
        with pytest.raises(ValueError):
            DigestConfig(window_seconds=-1)

    def test_multiple_channels(self):
        raw = {"digest": {"channels": ["log", "email", "slack"]}}
        cfg = parse_digest_config(raw)
        assert len(cfg.channels) == 3
