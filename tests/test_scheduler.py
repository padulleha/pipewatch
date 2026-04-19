"""Tests for pipewatch.scheduler and pipewatch.schedule_config."""

import time
import pytest
from unittest.mock import MagicMock

from pipewatch.scheduler import PipelineScheduler, ScheduledJob
from pipewatch.schedule_config import parse_schedule_entries, build_scheduler_from_config


# ---------------------------------------------------------------------------
# schedule_config tests
# ---------------------------------------------------------------------------

def test_parse_empty_config():
    assert parse_schedule_entries({}) == []


def test_parse_basic_entries():
    cfg = {"schedules": [{"pipeline": "etl_main", "interval": 30}]}
    entries = parse_schedule_entries(cfg)
    assert len(entries) == 1
    assert entries[0]["pipeline"] == "etl_main"
    assert entries[0]["interval"] == 30
    assert entries[0]["enabled"] is True


def test_parse_defaults_interval():
    cfg = {"schedules": [{"pipeline": "etl_main"}]}
    entries = parse_schedule_entries(cfg)
    assert entries[0]["interval"] == 60


def test_parse_skips_missing_pipeline():
    cfg = {"schedules": [{"interval": 10}]}
    assert parse_schedule_entries(cfg) == []


def test_parse_enabled_false():
    cfg = {"schedules": [{"pipeline": "p", "enabled": False}]}
    assert parse_schedule_entries(cfg)[0]["enabled"] is False


# ---------------------------------------------------------------------------
# PipelineScheduler unit tests
# ---------------------------------------------------------------------------

def test_register_and_job_info():
    sched = PipelineScheduler()
    cb = MagicMock()
    sched.register("myjob", 10, cb)
    job = sched.job_info("myjob")
    assert isinstance(job, ScheduledJob)
    assert job.interval_seconds == 10


def test_unregister():
    sched = PipelineScheduler()
    sched.register("j", 5, MagicMock())
    sched.unregister("j")
    assert sched.job_info("j") is None


def test_job_runs_after_start():
    sched = PipelineScheduler()
    cb = MagicMock()
    sched.register("fast", 0, cb)  # interval=0 → runs every tick
    sched.start()
    time.sleep(0.15)
    sched.stop()
    assert cb.call_count >= 1


def test_disabled_job_does_not_run():
    sched = PipelineScheduler()
    cb = MagicMock()
    sched.register("disabled", 0, cb, enabled=False)
    sched.start()
    time.sleep(0.12)
    sched.stop()
    cb.assert_not_called()


def test_build_scheduler_from_config():
    cfg = {"schedules": [{"pipeline": "pipe_a", "interval": 5}]}
    factory = lambda name: MagicMock()
    sched = build_scheduler_from_config(cfg, factory)
    assert sched.job_info("pipe_a") is not None
