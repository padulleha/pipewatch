"""Baseline management: save and compare metric baselines."""

from __future__ import annotations

import json
import os
from dataclasses import asdict
from typing import Optional

from pipewatch.metrics import PipelineMetric

_DEFAULT_DIR = os.path.join(os.path.expanduser("~"), ".pipewatch", "baselines")


def _baseline_path(pipeline: str, metric: str, directory: str = _DEFAULT_DIR) -> str:
    safe_pipeline = pipeline.replace("/", "_")
    safe_metric = metric.replace("/", "_")
    return os.path.join(directory, f"{safe_pipeline}__{safe_metric}.json")


def save_baseline(metric: PipelineMetric, directory: str = _DEFAULT_DIR) -> None:
    """Persist a metric value as the baseline for future comparisons."""
    os.makedirs(directory, exist_ok=True)
    path = _baseline_path(metric.pipeline, metric.name, directory)
    with open(path, "w") as fh:
        json.dump({"pipeline": metric.pipeline, "name": metric.name, "value": metric.value}, fh)


def load_baseline(pipeline: str, metric: str, directory: str = _DEFAULT_DIR) -> Optional[float]:
    """Return the stored baseline value, or None if not found."""
    path = _baseline_path(pipeline, metric, directory)
    if not os.path.exists(path):
        return None
    with open(path) as fh:
        data = json.load(fh)
    return float(data["value"])


def compare_to_baseline(
    metric: PipelineMetric, directory: str = _DEFAULT_DIR
) -> Optional[dict]:
    """Return a dict describing deviation from baseline, or None if no baseline exists."""
    baseline = load_baseline(metric.pipeline, metric.name, directory)
    if baseline is None:
        return None
    delta = metric.value - baseline
    pct = (delta / baseline * 100) if baseline != 0 else None
    return {
        "pipeline": metric.pipeline,
        "metric": metric.name,
        "baseline": baseline,
        "current": metric.value,
        "delta": delta,
        "delta_pct": pct,
    }


def clear_baseline(pipeline: str, metric: str, directory: str = _DEFAULT_DIR) -> bool:
    """Delete a stored baseline. Returns True if deleted, False if not found."""
    path = _baseline_path(pipeline, metric, directory)
    if os.path.exists(path):
        os.remove(path)
        return True
    return False
