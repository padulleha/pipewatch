"""Format snapshot diffs for display in reports or CLI output."""
from __future__ import annotations

from typing import Any


def _arrow(delta: float) -> str:
    if delta > 0:
        return "▲"
    if delta < 0:
        return "▼"
    return "="


def format_diff_lines(diffs: list[dict[str, Any]]) -> list[str]:
    """Return human-readable lines describing each metric delta."""
    if not diffs:
        return ["  (no previous snapshot to compare)"]
    lines = []
    for d in diffs:
        arrow = _arrow(d["delta"])
        lines.append(
            f"  {d['name']}: {d['previous']} → {d['current']} "
            f"({arrow} {abs(d['delta']):.4g})"
        )
    return lines


def format_diff_text(pipeline: str, diffs: list[dict[str, Any]]) -> str:
    """Return a full text block for snapshot diff output."""
    header = f"Snapshot diff for '{pipeline}':"
    lines = [header] + format_diff_lines(diffs)
    return "\n".join(lines)


def format_diff_json(pipeline: str, diffs: list[dict[str, Any]]) -> dict[str, Any]:
    return {"pipeline": pipeline, "diffs": diffs}
