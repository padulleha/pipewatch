"""Periodic digest summarisation for alert events."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Dict

from pipewatch.alerts import AlertEvent


@dataclass
class DigestEntry:
    pipeline: str
    metric_name: str
    status: str
    count: int
    first_seen: datetime
    last_seen: datetime

    def to_dict(self) -> Dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "status": self.status,
            "count": self.count,
            "first_seen": self.first_seen.isoformat(),
            "last_seen": self.last_seen.isoformat(),
        }


@dataclass
class DigestReport:
    generated_at: datetime
    window_seconds: int
    entries: List[DigestEntry] = field(default_factory=list)

    @property
    def total_alerts(self) -> int:
        return sum(e.count for e in self.entries)

    def to_text(self) -> str:
        lines = [
            f"=== PipeWatch Digest ({self.generated_at.strftime('%Y-%m-%d %H:%M:%S')} UTC) ===",
            f"Window: {self.window_seconds}s | Total alerts: {self.total_alerts}",
            "",
        ]
        if not self.entries:
            lines.append("  No alerts in this window.")
        else:
            for e in self.entries:
                lines.append(
                    f"  [{e.status.upper():8s}] {e.pipeline}/{e.metric_name}"
                    f"  x{e.count}  (first: {e.first_seen.strftime('%H:%M:%S')},"
                    f" last: {e.last_seen.strftime('%H:%M:%S')})"
                )
        return "\n".join(lines)


def build_digest(events: List[AlertEvent], window_seconds: int = 3600) -> DigestReport:
    """Aggregate *events* into a DigestReport."""
    now = datetime.now(tz=timezone.utc)
    cutoff = now.timestamp() - window_seconds

    grouped: Dict[tuple, DigestEntry] = {}
    for ev in events:
        ts = ev.timestamp if isinstance(ev.timestamp, datetime) else datetime.fromisoformat(str(ev.timestamp))
        if ts.timestamp() < cutoff:
            continue
        key = (ev.pipeline, ev.metric_name, ev.status)
        if key not in grouped:
            grouped[key] = DigestEntry(
                pipeline=ev.pipeline,
                metric_name=ev.metric_name,
                status=ev.status,
                count=0,
                first_seen=ts,
                last_seen=ts,
            )
        entry = grouped[key]
        entry.count += 1
        if ts < entry.first_seen:
            entry.first_seen = ts
        if ts > entry.last_seen:
            entry.last_seen = ts

    return DigestReport(
        generated_at=now,
        window_seconds=window_seconds,
        entries=sorted(grouped.values(), key=lambda e: e.last_seen, reverse=True),
    )
