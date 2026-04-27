"""Dead-letter queue for failed alert dispatches."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import List, Optional

DEFAULT_DL_PATH = Path(".pipewatch") / "deadletter.json"


@dataclass
class DeadLetterEntry:
    pipeline: str
    metric: str
    status: str
    channel: str
    error: str
    payload: dict
    timestamp: float = field(default_factory=time.time)
    retry_count: int = 0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "DeadLetterEntry":
        return cls(**data)

    def __str__(self) -> str:
        return (
            f"[{self.channel}] {self.pipeline}/{self.metric} "
            f"({self.status}) — {self.error} (retries={self.retry_count})"
        )


def _load_raw(path: Path) -> List[dict]:
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return []


def _save_raw(entries: List[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(entries, indent=2))


def push_entry(entry: DeadLetterEntry, path: Path = DEFAULT_DL_PATH) -> None:
    """Append a failed dispatch entry to the dead-letter queue."""
    raw = _load_raw(path)
    raw.append(entry.to_dict())
    _save_raw(raw, path)


def list_entries(
    path: Path = DEFAULT_DL_PATH,
    pipeline: Optional[str] = None,
    channel: Optional[str] = None,
) -> List[DeadLetterEntry]:
    """Return dead-letter entries, optionally filtered."""
    raw = _load_raw(path)
    entries = [DeadLetterEntry.from_dict(r) for r in raw]
    if pipeline:
        entries = [e for e in entries if e.pipeline == pipeline]
    if channel:
        entries = [e for e in entries if e.channel == channel]
    return entries


def purge(
    path: Path = DEFAULT_DL_PATH,
    pipeline: Optional[str] = None,
) -> int:
    """Remove entries from the queue. Returns count removed."""
    raw = _load_raw(path)
    before = len(raw)
    if pipeline:
        raw = [r for r in raw if r.get("pipeline") != pipeline]
    else:
        raw = []
    _save_raw(raw, path)
    return before - len(raw)


def increment_retry(entry: DeadLetterEntry, path: Path = DEFAULT_DL_PATH) -> None:
    """Increment the retry_count for a matching entry (by pipeline+metric+channel+timestamp)."""
    raw = _load_raw(path)
    for r in raw:
        if (
            r["pipeline"] == entry.pipeline
            and r["metric"] == entry.metric
            and r["channel"] == entry.channel
            and r["timestamp"] == entry.timestamp
        ):
            r["retry_count"] += 1
            break
    _save_raw(raw, path)
