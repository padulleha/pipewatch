"""Snapshot: capture and persist pipeline metric snapshots for comparison."""
from __future__ import annotations

import json
import os
import time
from typing import Any

from pipewatch.metrics import PipelineMetric

DEFAULT_SNAPSHOT_DIR = os.path.join(os.path.expanduser("~"), ".pipewatch", "snapshots")


def _snapshot_path(pipeline: str, directory: str = DEFAULT_SNAPSHOT_DIR) -> str:
    os.makedirs(directory, exist_ok=True)
    safe = pipeline.replace("/", "_").replace(" ", "_")
    return os.path.join(directory, f"{safe}.json")


def save_snapshot(
    pipeline: str,
    metrics: list[PipelineMetric],
    directory: str = DEFAULT_SNAPSHOT_DIR,
) -> None:
    """Persist the latest metric values for a pipeline."""
    path = _snapshot_path(pipeline, directory)
    data: dict[str, Any] = {
        "pipeline": pipeline,
        "timestamp": time.time(),
        "metrics": [
            {"name": m.name, "value": m.value, "unit": m.unit}
            for m in metrics
        ],
    }
    with open(path, "w") as fh:
        json.dump(data, fh, indent=2)


def load_snapshot(
    pipeline: str,
    directory: str = DEFAULT_SNAPSHOT_DIR,
) -> dict[str, Any] | None:
    """Return the last saved snapshot for *pipeline*, or None if absent."""
    path = _snapshot_path(pipeline, directory)
    if not os.path.exists(path):
        return None
    with open(path) as fh:
        return json.load(fh)


def diff_snapshot(
    pipeline: str,
    current: list[PipelineMetric],
    directory: str = DEFAULT_SNAPSHOT_DIR,
) -> list[dict[str, Any]]:
    """Return per-metric diffs between current values and last snapshot."""
    previous = load_snapshot(pipeline, directory)
    if previous is None:
        return []
    prev_map = {m["name"]: m["value"] for m in previous["metrics"]}
    diffs = []
    for m in current:
        if m.name in prev_map:
            delta = m.value - prev_map[m.name]
            diffs.append({"name": m.name, "previous": prev_map[m.name], "current": m.value, "delta": delta})
    return diffs


def clear_snapshot(pipeline: str, directory: str = DEFAULT_SNAPSHOT_DIR) -> bool:
    path = _snapshot_path(pipeline, directory)
    if os.path.exists(path):
        os.remove(path)
        return True
    return False
