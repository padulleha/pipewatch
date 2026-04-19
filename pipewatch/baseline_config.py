"""Parse baseline configuration from a config dict."""

from __future__ import annotations

from typing import Any, Dict, List


def parse_baseline_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Extract baseline settings from a top-level config dict.

    Expected config shape:
        baseline:
          directory: ~/.pipewatch/baselines   # optional
          auto_save: false                    # optional, default false
          pipelines:                          # optional allowlist
            - my_pipeline
    """
    raw = config.get("baseline", {})
    if not isinstance(raw, dict):
        raw = {}

    import os

    directory = raw.get(
        "directory",
        os.path.join(os.path.expanduser("~"), ".pipewatch", "baselines"),
    )
    auto_save = bool(raw.get("auto_save", False))
    pipelines: List[str] = list(raw.get("pipelines", []))

    return {
        "directory": directory,
        "auto_save": auto_save,
        "pipelines": pipelines,
    }
