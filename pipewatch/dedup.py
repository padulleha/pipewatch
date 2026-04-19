"""Alert deduplication: suppress repeated alerts within a cooldown window."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class DedupEntry:
    pipeline: str
    metric: str
    status: str
    first_seen: float
    last_seen: float
    count: int = 1


@dataclass
class DedupRegistry:
    cooldown_seconds: float = 300.0
    _store: Dict[str, DedupEntry] = field(default_factory=dict, repr=False)

    def _key(self, pipeline: str, metric: str, status: str) -> str:
        return f"{pipeline}::{metric}::{status}"

    def is_duplicate(self, pipeline: str, metric: str, status: str) -> bool:
        key = self._key(pipeline, metric, status)
        entry = self._store.get(key)
        if entry is None:
            return False
        return (time.time() - entry.last_seen) < self.cooldown_seconds

    def record(self, pipeline: str, metric: str, status: str) -> DedupEntry:
        key = self._key(pipeline, metric, status)
        now = time.time()
        if key in self._store:
            entry = self._store[key]
            entry.last_seen = now
            entry.count += 1
        else:
            entry = DedupEntry(
                pipeline=pipeline,
                metric=metric,
                status=status,
                first_seen=now,
                last_seen=now,
            )
            self._store[key] = entry
        return entry

    def clear(self, pipeline: Optional[str] = None) -> None:
        if pipeline is None:
            self._store.clear()
        else:
            keys = [k for k in self._store if k.startswith(f"{pipeline}::")]
            for k in keys:
                del self._store[k]

    def all_entries(self) -> list[DedupEntry]:
        return list(self._store.values())
