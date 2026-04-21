"""Tests for pipewatch.runbook and pipewatch.runbook_config."""
from __future__ import annotations

import pytest

from pipewatch.metrics import EvaluationResult
from pipewatch.runbook import RunbookEntry, RunbookRegistry
from pipewatch.runbook_config import parse_runbook_entries


def make_result(pipeline: str = "pipe_a", metric: str = "row_count", status: str = "warning") -> EvaluationResult:
    return EvaluationResult(
        pipeline=pipeline,
        metric_name=metric,
        value=42.0,
        status=status,
        message="test",
    )


class TestRunbookEntry:
    def test_matches_exact(self):
        entry = RunbookEntry(pipeline="pipe_a", metric="row_count", url="http://x")
        assert entry.matches(make_result("pipe_a", "row_count"))

    def test_no_match_wrong_pipeline(self):
        entry = RunbookEntry(pipeline="pipe_b", metric="row_count")
        assert not entry.matches(make_result("pipe_a", "row_count"))

    def test_no_match_wrong_metric(self):
        entry = RunbookEntry(pipeline="pipe_a", metric="latency")
        assert not entry.matches(make_result("pipe_a", "row_count"))

    def test_to_dict_contains_fields(self):
        entry = RunbookEntry(pipeline="p", metric="m", url="http://u", notes="n", tags=["t1"])
        d = entry.to_dict()
        assert d["url"] == "http://u"
        assert d["notes"] == "n"
        assert d["tags"] == ["t1"]


class TestRunbookRegistry:
    def test_lookup_returns_none_when_empty(self):
        reg = RunbookRegistry()
        assert reg.lookup(make_result()) is None

    def test_lookup_returns_first_match(self):
        reg = RunbookRegistry()
        reg.add(RunbookEntry(pipeline="pipe_a", metric="row_count", url="http://first"))
        reg.add(RunbookEntry(pipeline="pipe_a", metric="row_count", url="http://second"))
        entry = reg.lookup(make_result())
        assert entry is not None
        assert entry.url == "http://first"

    def test_annotate_no_match(self):
        reg = RunbookRegistry()
        ann = reg.annotate(make_result())
        assert ann == {"runbook_url": None, "runbook_notes": None}

    def test_annotate_with_match(self):
        reg = RunbookRegistry()
        reg.add(RunbookEntry(pipeline="pipe_a", metric="row_count", url="http://wiki", notes="Check logs"))
        ann = reg.annotate(make_result())
        assert ann["runbook_url"] == "http://wiki"
        assert ann["runbook_notes"] == "Check logs"


class TestParseRunbookEntries:
    def test_empty_config(self):
        reg = parse_runbook_entries({})
        assert reg.lookup(make_result()) is None

    def test_parses_valid_entry(self):
        cfg = {
            "runbooks": [
                {"pipeline": "pipe_a", "metric": "row_count", "url": "http://w", "notes": "fix it", "tags": ["dq"]}
            ]
        }
        reg = parse_runbook_entries(cfg)
        entry = reg.lookup(make_result())
        assert entry is not None
        assert entry.url == "http://w"
        assert entry.tags == ["dq"]

    def test_skips_entry_missing_pipeline(self):
        cfg = {"runbooks": [{"metric": "row_count", "url": "http://w"}]}
        reg = parse_runbook_entries(cfg)
        assert reg.lookup(make_result()) is None

    def test_skips_entry_missing_metric(self):
        cfg = {"runbooks": [{"pipeline": "pipe_a", "url": "http://w"}]}
        reg = parse_runbook_entries(cfg)
        assert reg.lookup(make_result()) is None

    def test_non_list_runbooks_ignored(self):
        cfg = {"runbooks": "not-a-list"}
        reg = parse_runbook_entries(cfg)
        assert reg.lookup(make_result()) is None
