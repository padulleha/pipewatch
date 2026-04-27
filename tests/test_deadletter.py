"""Tests for pipewatch.deadletter."""
import pytest
from pathlib import Path
from pipewatch.deadletter import (
    DeadLetterEntry,
    push_entry,
    list_entries,
    purge,
    increment_retry,
)


def _entry(pipeline="pipe_a", metric="row_count", channel="email", error="timeout") -> DeadLetterEntry:
    return DeadLetterEntry(
        pipeline=pipeline,
        metric=metric,
        status="critical",
        channel=channel,
        error=error,
        payload={"value": 0},
        timestamp=1_000_000.0,
    )


@pytest.fixture()
def dl_path(tmp_path) -> Path:
    return tmp_path / "deadletter.json"


def test_push_and_list(dl_path):
    e = _entry()
    push_entry(e, path=dl_path)
    entries = list_entries(path=dl_path)
    assert len(entries) == 1
    assert entries[0].pipeline == "pipe_a"
    assert entries[0].channel == "email"


def test_list_empty_when_no_file(dl_path):
    entries = list_entries(path=dl_path)
    assert entries == []


def test_push_multiple(dl_path):
    push_entry(_entry(pipeline="a"), path=dl_path)
    push_entry(_entry(pipeline="b"), path=dl_path)
    assert len(list_entries(path=dl_path)) == 2


def test_filter_by_pipeline(dl_path):
    push_entry(_entry(pipeline="a"), path=dl_path)
    push_entry(_entry(pipeline="b"), path=dl_path)
    results = list_entries(path=dl_path, pipeline="a")
    assert len(results) == 1
    assert results[0].pipeline == "a"


def test_filter_by_channel(dl_path):
    push_entry(_entry(channel="email"), path=dl_path)
    push_entry(_entry(channel="slack"), path=dl_path)
    results = list_entries(path=dl_path, channel="slack")
    assert len(results) == 1
    assert results[0].channel == "slack"


def test_purge_all(dl_path):
    push_entry(_entry(pipeline="a"), path=dl_path)
    push_entry(_entry(pipeline="b"), path=dl_path)
    removed = purge(path=dl_path)
    assert removed == 2
    assert list_entries(path=dl_path) == []


def test_purge_by_pipeline(dl_path):
    push_entry(_entry(pipeline="a"), path=dl_path)
    push_entry(_entry(pipeline="b"), path=dl_path)
    removed = purge(path=dl_path, pipeline="a")
    assert removed == 1
    remaining = list_entries(path=dl_path)
    assert len(remaining) == 1
    assert remaining[0].pipeline == "b"


def test_increment_retry(dl_path):
    e = _entry()
    push_entry(e, path=dl_path)
    increment_retry(e, path=dl_path)
    entries = list_entries(path=dl_path)
    assert entries[0].retry_count == 1


def test_str_representation():
    e = _entry()
    s = str(e)
    assert "email" in s
    assert "pipe_a" in s
    assert "row_count" in s
    assert "timeout" in s


def test_to_dict_and_from_dict():
    e = _entry()
    d = e.to_dict()
    restored = DeadLetterEntry.from_dict(d)
    assert restored.pipeline == e.pipeline
    assert restored.metric == e.metric
    assert restored.retry_count == 0
