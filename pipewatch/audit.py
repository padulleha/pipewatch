"""Audit log for pipewatch — records alert dispatches, escalations, and suppression events."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

DEFAULT_AUDIT_PATH = Path(".pipewatch") / "audit.jsonl"


@dataclass
class AuditEntry:
    """A single audit log record."""

    event_type: str          # e.g. 'alert_dispatched', 'escalation', 'suppressed', 'dedup_skipped'
    pipeline: str
    metric: str
    status: str              # ok / warning / critical
    message: str
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    extra: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "event_type": self.event_type,
            "pipeline": self.pipeline,
            "metric": self.metric,
            "status": self.status,
            "message": self.message,
            "timestamp": self.timestamp,
            "extra": self.extra,
        }

    @staticmethod
    def from_dict(d: dict) -> "AuditEntry":
        return AuditEntry(
            event_type=d["event_type"],
            pipeline=d["pipeline"],
            metric=d["metric"],
            status=d["status"],
            message=d["message"],
            timestamp=d.get("timestamp", ""),
            extra=d.get("extra", {}),
        )


def _audit_path(audit_file: Optional[Path] = None) -> Path:
    return audit_file or DEFAULT_AUDIT_PATH


def append_entry(entry: AuditEntry, audit_file: Optional[Path] = None) -> None:
    """Append a single audit entry to the JSONL audit log."""
    path = _audit_path(audit_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry.to_dict()) + "\n")


def read_entries(
    audit_file: Optional[Path] = None,
    pipeline: Optional[str] = None,
    event_type: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[AuditEntry]:
    """Read audit entries, optionally filtered by pipeline or event type."""
    path = _audit_path(audit_file)
    if not path.exists():
        return []

    entries: List[AuditEntry] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue
            entry = AuditEntry.from_dict(d)
            if pipeline and entry.pipeline != pipeline:
                continue
            if event_type and entry.event_type != event_type:
                continue
            entries.append(entry)

    if limit:
        entries = entries[-limit:]
    return entries


def clear_audit(audit_file: Optional[Path] = None) -> None:
    """Delete the audit log file."""
    path = _audit_path(audit_file)
    if path.exists():
        path.unlink()
