"""Redaction support for alert events — masks sensitive fields before dispatch."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from pipewatch.alerts import AlertEvent

_MASK = "***"


@dataclass
class RedactionRule:
    """Defines a pattern-based redaction rule for alert event metadata."""

    field: str  # dot-separated key path, e.g. "meta.api_key"
    pattern: str | None = None  # optional regex; if None, entire value is masked
    _compiled: re.Pattern | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.pattern:
            self._compiled = re.compile(self.pattern)

    def apply(self, event: AlertEvent) -> AlertEvent:
        """Return a copy of *event* with the targeted field redacted."""
        meta = dict(event.extra_meta or {})
        keys = self.field.split(".")
        _redact_nested(meta, keys, self._compiled)
        return AlertEvent(
            pipeline=event.pipeline,
            metric=event.metric,
            status=event.status,
            value=event.value,
            message=event.message,
            extra_meta=meta,
        )


def _redact_nested(
    data: dict[str, Any], keys: list[str], compiled: re.Pattern | None
) -> None:
    if not keys or not isinstance(data, dict):
        return
    head, *tail = keys
    if head not in data:
        return
    if not tail:
        if compiled is None:
            data[head] = _MASK
        else:
            if isinstance(data[head], str):
                data[head] = compiled.sub(_MASK, data[head])
    else:
        _redact_nested(data[head], tail, compiled)


@dataclass
class RedactionRegistry:
    _rules: list[RedactionRule] = field(default_factory=list)

    def add(self, rule: RedactionRule) -> None:
        self._rules.append(rule)

    def apply_all(self, event: AlertEvent) -> AlertEvent:
        """Apply every registered rule in order and return the redacted event."""
        for rule in self._rules:
            event = rule.apply(event)
        return event

    def __len__(self) -> int:
        return len(self._rules)
