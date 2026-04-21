"""Runbook links: attach remediation URLs and notes to alert events."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from pipewatch.metrics import EvaluationResult


@dataclass
class RunbookEntry:
    pipeline: str
    metric: str
    url: Optional[str] = None
    notes: Optional[str] = None
    tags: list[str] = field(default_factory=list)

    def matches(self, result: EvaluationResult) -> bool:
        """Return True if this entry applies to the given evaluation result."""
        if self.pipeline and self.pipeline != result.pipeline:
            return False
        if self.metric and self.metric != result.metric_name:
            return False
        return True

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.metric,
            "url": self.url,
            "notes": self.notes,
            "tags": self.tags,
        }


@dataclass
class RunbookRegistry:
    _entries: list[RunbookEntry] = field(default_factory=list)

    def add(self, entry: RunbookEntry) -> None:
        self._entries.append(entry)

    def lookup(self, result: EvaluationResult) -> Optional[RunbookEntry]:
        """Return the first matching runbook entry for the result, or None."""
        for entry in self._entries:
            if entry.matches(result):
                return entry
        return None

    def annotate(self, result: EvaluationResult) -> dict:
        """Return a dict with runbook info merged from the best matching entry."""
        entry = self.lookup(result)
        if entry is None:
            return {"runbook_url": None, "runbook_notes": None}
        return {"runbook_url": entry.url, "runbook_notes": entry.notes}
