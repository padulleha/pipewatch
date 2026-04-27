"""Parse redaction rules from a pipewatch config dict."""
from __future__ import annotations

from typing import Any

from pipewatch.redaction import RedactionRegistry, RedactionRule


def parse_redaction_rules(config: dict[str, Any]) -> RedactionRegistry:
    """Build a :class:`RedactionRegistry` from the ``redaction`` section of
    the top-level config dict.

    Expected shape::

        redaction:
          rules:
            - field: "meta.api_key"
            - field: "meta.token"
              pattern: "tok_[A-Za-z0-9]+"
    """
    registry = RedactionRegistry()
    section = config.get("redaction", {})
    rules_raw = section.get("rules", [])
    if not isinstance(rules_raw, list):
        return registry
    for entry in rules_raw:
        if not isinstance(entry, dict):
            continue
        field_path = entry.get("field")
        if not field_path or not isinstance(field_path, str):
            continue
        pattern = entry.get("pattern")  # may be None
        registry.add(RedactionRule(field=field_path, pattern=pattern or None))
    return registry
