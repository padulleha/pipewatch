"""Tests for pipewatch.dedup and pipewatch.dedup_config."""

from __future__ import annotations

import time

import pytest

from pipewatch.dedup import DedupRegistry
from pipewatch.dedup_config import parse_dedup_config, cooldown_for_pipeline


# ---------------------------------------------------------------------------
# DedupRegistry
# ---------------------------------------------------------------------------

def test_first_occurrence_not_duplicate():
    reg = DedupRegistry(cooldown_seconds=60)
    assert reg.is_duplicate("pipe", "lag", "warning") is False


def test_record_then_duplicate():
    reg = DedupRegistry(cooldown_seconds=60)
    reg.record("pipe", "lag", "warning")
    assert reg.is_duplicate("pipe", "lag", "warning") is True


def test_expired_entry_not_duplicate(monkeypatch):
    reg = DedupRegistry(cooldown_seconds=10)
    reg.record("pipe", "lag", "warning")
    # advance time beyond cooldown
    monkeypatch.setattr(time, "time", lambda: time.time() + 20)
    # re-import after monkeypatch won't help here; use manual manipulation
    entry = reg._store["pipe::lag::warning"]
    entry.last_seen -= 20
    assert reg.is_duplicate("pipe", "lag", "warning") is False


def test_record_increments_count():
    reg = DedupRegistry(cooldown_seconds=60)
    reg.record("pipe", "lag", "warning")
    reg.record("pipe", "lag", "warning")
    entry = reg._store["pipe::lag::warning"]
    assert entry.count == 2


def test_different_status_not_duplicate():
    reg = DedupRegistry(cooldown_seconds=60)
    reg.record("pipe", "lag", "warning")
    assert reg.is_duplicate("pipe", "lag", "critical") is False


def test_clear_all():
    reg = DedupRegistry(cooldown_seconds=60)
    reg.record("pipe", "lag", "warning")
    reg.clear()
    assert reg.all_entries() == []


def test_clear_specific_pipeline():
    reg = DedupRegistry(cooldown_seconds=60)
    reg.record("pipe_a", "lag", "warning")
    reg.record("pipe_b", "lag", "warning")
    reg.clear(pipeline="pipe_a")
    keys = [e.pipeline for e in reg.all_entries()]
    assert keys == ["pipe_b"]


# ---------------------------------------------------------------------------
# dedup_config
# ---------------------------------------------------------------------------

def test_parse_dedup_default():
    reg = parse_dedup_config({})
    assert reg.cooldown_seconds == 300.0


def test_parse_dedup_custom():
    reg = parse_dedup_config({"dedup": {"cooldown_seconds": 120}})
    assert reg.cooldown_seconds == 120.0


def test_cooldown_for_pipeline_global_fallback():
    config = {"dedup": {"cooldown_seconds": 600}}
    assert cooldown_for_pipeline("my_pipe", config) == 600.0


def test_cooldown_for_pipeline_override():
    config = {
        "dedup": {"cooldown_seconds": 300},
        "pipelines": {
            "fast_pipe": {"dedup": {"cooldown_seconds": 60}}
        },
    }
    assert cooldown_for_pipeline("fast_pipe", config) == 60.0
    assert cooldown_for_pipeline("other_pipe", config) == 300.0
