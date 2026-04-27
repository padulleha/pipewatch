"""Tests for pipewatch.fingerprint."""
from __future__ import annotations

import pytest

from pipewatch.fingerprint import (
    AlertFingerprint,
    FingerprintConfig,
    compute_fingerprint,
)
from pipewatch.metrics import EvaluationResult, PipelineMetric, ThresholdRule


def make_metric(pipeline="pipe-a", name="row_count", value=100.0, tags=None):
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        tags=tags or {},
    )


def make_rule(warning=50.0, critical=10.0):
    return ThresholdRule(metric="row_count", warning=warning, critical=critical)


def make_result(pipeline="pipe-a", name="row_count", value=100.0, status="ok", tags=None, rule=None):
    metric = make_metric(pipeline=pipeline, name=name, value=value, tags=tags)
    return EvaluationResult(metric=metric, status=status, rule=rule)


# ---------------------------------------------------------------------------
# Basic smoke tests
# ---------------------------------------------------------------------------

def test_returns_alert_fingerprint():
    result = make_result()
    fp = compute_fingerprint(result)
    assert isinstance(fp, AlertFingerprint)
    assert len(fp.hex) == 64  # SHA-256 hex


def test_short_prefix_length():
    fp = compute_fingerprint(make_result())
    assert len(fp.short()) == 8
    assert len(fp.short(12)) == 12


def test_str_returns_hex():
    fp = compute_fingerprint(make_result())
    assert str(fp) == fp.hex


# ---------------------------------------------------------------------------
# Stability: same inputs → same fingerprint
# ---------------------------------------------------------------------------

def test_deterministic_same_inputs():
    r = make_result()
    assert compute_fingerprint(r).hex == compute_fingerprint(r).hex


def test_different_pipeline_different_fingerprint():
    fp1 = compute_fingerprint(make_result(pipeline="pipe-a"))
    fp2 = compute_fingerprint(make_result(pipeline="pipe-b"))
    assert fp1.hex != fp2.hex


def test_different_metric_different_fingerprint():
    fp1 = compute_fingerprint(make_result(name="row_count"))
    fp2 = compute_fingerprint(make_result(name="latency_ms"))
    assert fp1.hex != fp2.hex


def test_different_status_different_fingerprint():
    fp1 = compute_fingerprint(make_result(status="ok"))
    fp2 = compute_fingerprint(make_result(status="critical"))
    assert fp1.hex != fp2.hex


# ---------------------------------------------------------------------------
# Optional fields
# ---------------------------------------------------------------------------

def test_exclude_status_same_for_different_statuses():
    cfg = FingerprintConfig(include_status=False)
    fp1 = compute_fingerprint(make_result(status="ok"), cfg)
    fp2 = compute_fingerprint(make_result(status="critical"), cfg)
    assert fp1.hex == fp2.hex


def test_include_threshold_changes_fingerprint():
    rule_a = make_rule(warning=50.0, critical=10.0)
    rule_b = make_rule(warning=80.0, critical=20.0)
    cfg = FingerprintConfig(include_threshold=True)
    fp1 = compute_fingerprint(make_result(rule=rule_a), cfg)
    fp2 = compute_fingerprint(make_result(rule=rule_b), cfg)
    assert fp1.hex != fp2.hex


def test_include_tags_changes_fingerprint():
    cfg = FingerprintConfig(include_tags=True)
    fp1 = compute_fingerprint(make_result(tags={"env": "prod"}))
    fp2 = compute_fingerprint(make_result(tags={"env": "staging"}), cfg)
    assert fp1.hex != fp2.hex


def test_salt_changes_fingerprint():
    cfg_a = FingerprintConfig(salt="tenant-a")
    cfg_b = FingerprintConfig(salt="tenant-b")
    r = make_result()
    assert compute_fingerprint(r, cfg_a).hex != compute_fingerprint(r, cfg_b).hex


def test_components_populated():
    cfg = FingerprintConfig(include_pipeline=True, include_metric=True, include_status=True)
    fp = compute_fingerprint(make_result(pipeline="p", name="m", status="warning"), cfg)
    assert fp.components["pipeline"] == "p"
    assert fp.components["metric"] == "m"
    assert fp.components["status"] == "warning"
