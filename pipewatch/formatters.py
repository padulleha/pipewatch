"""Output formatters for pipeline reports."""
import json
from typing import Callable
from pipewatch.report import PipelineReport


def format_text(report: PipelineReport) -> str:
    return report.to_text()


def format_json(report: PipelineReport) -> str:
    data = {
        "pipeline": report.pipeline,
        "generated_at": report.generated_at.isoformat(),
        "entries": report.entries,
    }
    return json.dumps(data, indent=2)


def format_markdown(report: PipelineReport) -> str:
    lines = [
        f"# Pipeline Report: `{report.pipeline}`",
        f"_Generated: {report.generated_at.isoformat()}_",
        "",
        "| Metric | Value | Status | Trend | Anomaly |",
        "|--------|-------|--------|-------|---------|",
    ]
    for e in report.entries:
        anomaly = "yes" if e.get("anomaly") else "no"
        lines.append(
            f"| {e['metric']} | {e['value']} | {e['status'].upper()} | {e.get('trend','stable')} | {anomaly} |"
        )
    return "\n".join(lines)


FORMAT_MAP: dict[str, Callable[[PipelineReport], str]] = {
    "text": format_text,
    "json": format_json,
    "markdown": format_markdown,
}


def get_formatter(fmt: str) -> Callable[[PipelineReport], str]:
    formatter = FORMAT_MAP.get(fmt)
    if formatter is None:
        raise ValueError(f"Unknown format '{fmt}'. Choose from: {list(FORMAT_MAP.keys())}")
    return formatter
