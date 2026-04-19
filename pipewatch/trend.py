"""Trend analysis helpers derived from metric history."""

from __future__ import annotations

from typing import List, Optional


def _values(entries: List[dict]) -> List[float]:
    return [e["value"] for e in entries]


def average(entries: List[dict]) -> Optional[float]:
    """Return the mean value of history entries."""
    vals = _values(entries)
    if not vals:
        return None
    return sum(vals) / len(vals)


def moving_average(entries: List[dict], window: int = 5) -> Optional[float]:
    """Return moving average over the last *window* entries."""
    vals = _values(entries)
    if not vals:
        return None
    subset = vals[-window:]
    return sum(subset) / len(subset)


def trend_direction(entries: List[dict], window: int = 5) -> str:
    """Return 'up', 'down', or 'stable' based on recent slope."""
    vals = _values(entries)[-window:]
    if len(vals) < 2:
        return "stable"
    delta = vals[-1] - vals[0]
    threshold = 0.05 * abs(vals[0]) if vals[0] != 0 else 0.01
    if delta > threshold:
        return "up"
    if delta < -threshold:
        return "down"
    return "stable"


def is_anomaly(entries: List[dict], value: float, sigma: float = 2.0) -> bool:
    """Return True if *value* is more than *sigma* std-devs from the mean."""
    vals = _values(entries)
    if len(vals) < 3:
        return False
    mean = sum(vals) / len(vals)
    variance = sum((v - mean) ** 2 for v in vals) / len(vals)
    std = variance ** 0.5
    if std == 0:
        return False
    return abs(value - mean) > sigma * std


def percent_change(entries: List[dict], window: int = 5) -> Optional[float]:
    """Return the percentage change between the oldest and newest value in the
    last *window* entries, or ``None`` if there are fewer than two entries or
    the baseline value is zero.
    """
    vals = _values(entries)[-window:]
    if len(vals) < 2:
        return None
    baseline = vals[0]
    if baseline == 0:
        return None
    return (vals[-1] - baseline) / abs(baseline) * 100
