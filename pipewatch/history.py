"""Pipeline metric history storage and retrieval using a simple JSON file."""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import List, Optional

DEFAULT_HISTORY_PATH = os.path.expanduser("~/.pipewatch/history.json")


def _load_raw(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, "r") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"History file at '{path}' contains invalid JSON: {exc}"
            ) from exc


def _save_raw(data: dict, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def record_metric(pipeline: str, metric_name: str, value: float,
                  timestamp: Optional[str] = None,
                  path: str = DEFAULT_HISTORY_PATH) -> None:
    """Append a metric reading to the history store."""
    data = _load_raw(path)
    key = f"{pipeline}.{metric_name}"
    entry = {
        "value": value,
        "timestamp": timestamp or datetime.utcnow().isoformat(),
    }
    data.setdefault(key, []).append(entry)
    _save_raw(data, path)


def get_history(pipeline: str, metric_name: str,
                limit: int = 50,
                path: str = DEFAULT_HISTORY_PATH) -> List[dict]:
    """Return recent history entries for a pipeline metric."""
    data = _load_raw(path)
    key = f"{pipeline}.{metric_name}"
    entries = data.get(key, [])
    return entries[-limit:]


def clear_history(pipeline: str, metric_name: Optional[str] = None,
                  path: str = DEFAULT_HISTORY_PATH) -> None:
    """Clear history for a pipeline, or a specific metric within it."""
    data = _load_raw(path)
    if metric_name:
        key = f"{pipeline}.{metric_name}"
        data.pop(key, None)
    else:
        keys_to_remove = [k for k in data if k.startswith(f"{pipeline}.")]
        for k in keys_to_remove:
            del data[k]
    _save_raw(data, path)
