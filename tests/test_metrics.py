"""Tests for pipewatch metrics evaluation."""

import pytest
from pipewatch.metrics import PipelineMetric, ThresholdRule, evaluate_metrics


def make_metric(name: str, value: float, unit: str = "count") -> PipelineMetric:
    return PipelineMetric(name=name, value=value, unit=unit)


class TestThresholdRule:
    def test_ok_status(self):
        rule = ThresholdRule("error_rate", warning_threshold=0.05, critical_threshold=0.10, operator="gt")
        metric = make_metric("error_rate", 0.02)
        assert rule.evaluate(metric) == "ok"

    def test_warning_status(self):
        rule = ThresholdRule("error_rate", warning_threshold=0.05, critical_threshold=0.10, operator="gt")
        metric = make_metric("error_rate", 0.07)
        assert rule.evaluate(metric) == "warning"

    def test_critical_status(self):
        rule = ThresholdRule("error_rate", warning_threshold=0.05, critical_threshold=0.10, operator="gt")
        metric = make_metric("error_rate", 0.15)
        assert rule.evaluate(metric) == "critical"

    def test_lt_operator_critical(self):
        rule = ThresholdRule("row_count", warning_threshold=1000, critical_threshold=500, operator="lt")
        metric = make_metric("row_count", 400)
        assert rule.evaluate(metric) == "critical"

    def test_lt_operator_warning(self):
        rule = ThresholdRule("row_count", warning_threshold=1000, critical_threshold=500, operator="lt")
        metric = make_metric("row_count", 800)
        assert rule.evaluate(metric) == "warning"

    def test_invalid_operator_raises(self):
        rule = ThresholdRule("row_count", warning_threshold=100, operator="eq")
        with pytest.raises(ValueError, match="Unknown operator"):
            rule.evaluate(make_metric("row_count", 50))


class TestEvaluateMetrics:
    def test_no_matching_rule_returns_ok(self):
        metrics = [make_metric("latency", 200)]
        results = evaluate_metrics(metrics, [])
        assert results[0]["status"] == "ok"

    def test_multiple_metrics_evaluated(self):
        metrics = [
            make_metric("error_rate", 0.12),
            make_metric("row_count", 300),
        ]
        rules = [
            ThresholdRule("error_rate", warning_threshold=0.05, critical_threshold=0.10, operator="gt"),
            ThresholdRule("row_count", warning_threshold=1000, critical_threshold=500, operator="lt"),
        ]
        results = evaluate_metrics(metrics, rules)
        statuses = {r["metric"]["name"]: r["status"] for r in results}
        assert statuses["error_rate"] == "critical"
        assert statuses["row_count"] == "critical"
