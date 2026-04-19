"""Tests for pipewatch.tags and pipewatch.tag_config."""
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.tags import TagFilter, filter_metrics, group_by_tag, all_tag_values
from pipewatch.tag_config import parse_tag_filter


def make_metric(name: str, value: float, tags=None) -> PipelineMetric:
    return PipelineMetric(pipeline=name, metric_name="latency", value=value, tags=tags or {})


class TestTagFilter:
    def test_matches_no_constraints(self):
        f = TagFilter()
        assert f.matches(make_metric("p", 1.0, {"env": "prod"}))

    def test_matches_required_present(self):
        f = TagFilter(required={"env": "prod"})
        assert f.matches(make_metric("p", 1.0, {"env": "prod"}))

    def test_fails_required_missing(self):
        f = TagFilter(required={"env": "prod"})
        assert not f.matches(make_metric("p", 1.0, {"env": "staging"}))

    def test_fails_excluded_present(self):
        f = TagFilter(excluded={"status": "disabled"})
        assert not f.matches(make_metric("p", 1.0, {"status": "disabled"}))

    def test_passes_excluded_absent(self):
        f = TagFilter(excluded={"status": "disabled"})
        assert f.matches(make_metric("p", 1.0, {"status": "active"}))

    def test_no_tags_on_metric(self):
        f = TagFilter(required={"env": "prod"})
        assert not f.matches(make_metric("p", 1.0, None))


def test_filter_metrics():
    metrics = [
        make_metric("a", 1.0, {"env": "prod"}),
        make_metric("b", 2.0, {"env": "staging"}),
        make_metric("c", 3.0, {"env": "prod"}),
    ]
    f = TagFilter(required={"env": "prod"})
    result = filter_metrics(metrics, f)
    assert [m.pipeline for m in result] == ["a", "c"]


def test_group_by_tag():
    metrics = [
        make_metric("a", 1.0, {"env": "prod"}),
        make_metric("b", 2.0, {"env": "staging"}),
        make_metric("c", 3.0, {"env": "prod"}),
    ]
    groups = group_by_tag(metrics, "env")
    assert len(groups["prod"]) == 2
    assert len(groups["staging"]) == 1


def test_group_by_tag_missing_key():
    metrics = [make_metric("a", 1.0, {}), make_metric("b", 2.0, {"env": "prod"})]
    groups = group_by_tag(metrics, "env")
    assert None in groups
    assert len(groups[None]) == 1


def test_all_tag_values():
    metrics = [
        make_metric("a", 1.0, {"env": "prod"}),
        make_metric("b", 2.0, {"env": "staging"}),
        make_metric("c", 3.0, {"env": "prod"}),
    ]
    vals = all_tag_values(metrics, "env")
    assert set(vals) == {"prod", "staging"}


def test_parse_tag_filter_full():
    cfg = {"tags": {"require": {"env": "prod"}, "exclude": {"status": "disabled"}}}
    f = parse_tag_filter(cfg)
    assert f.required == {"env": "prod"}
    assert f.excluded == {"status": "disabled"}


def test_parse_tag_filter_empty():
    f = parse_tag_filter({})
    assert f.required == {}
    assert f.excluded == {}
