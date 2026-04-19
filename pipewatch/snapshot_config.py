"""Parse snapshot-related configuration from a config dict."""
from __future__ import annotations

from typing import Any

DEFAULT_SNAPSHOT_DIR = None  # falls back to snapshot.DEFAULT_SNAPSHOT_DIR


def parse_snapshot_config(config: dict[str, Any]) -> dict[str, Any]:
    """Extract snapshot settings from the top-level config mapping.

    Expected config shape::

        snapshots:
          enabled: true
          directory: /var/pipewatch/snapshots
          auto_diff: true
    """
    raw: dict[str, Any] = config.get("snapshots", {})
    return {
        "enabled": bool(raw.get("enabled", True)),
        "directory": raw.get("directory", DEFAULT_SNAPSHOT_DIR),
        "auto_diff": bool(raw.get("auto_diff", False)),
    }


def snapshot_dir_for(config: dict[str, Any]) -> str | None:
    """Convenience: return the configured snapshot directory or None."""
    return parse_snapshot_config(config).get("directory")
