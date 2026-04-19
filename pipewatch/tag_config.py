"""Parse tag filter configuration from dicts/YAML structures."""
from __future__ import annotations
from typing import Any, Dict
from pipewatch.tags import TagFilter


def parse_tag_filter(cfg: Dict[str, Any]) -> TagFilter:
    """Parse a tag filter from a config dict.

    Expected shape::

        tags:
          require:
            env: production
          exclude:
            status: disabled
    """
    tag_cfg = cfg.get("tags", {})
    required = dict(tag_cfg.get("require", {}) or {})
    excluded = dict(tag_cfg.get("exclude", {}) or {})
    return TagFilter(required=required, excluded=excluded)


def tag_filter_for_pipeline(pipeline_cfg: Dict[str, Any]) -> TagFilter:
    """Convenience wrapper to extract tag filter from a pipeline config entry."""
    return parse_tag_filter(pipeline_cfg)
