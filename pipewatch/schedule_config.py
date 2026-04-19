"""Parse scheduler configuration from pipewatch config dicts."""

from typing import Any, Dict, List


DEFAULT_INTERVAL = 60  # seconds


def parse_schedule_entries(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return a list of schedule entry dicts from the top-level config.

    Expected config shape::

        schedules:
          - pipeline: my_pipeline
            interval: 120
            enabled: true
    """
    raw = config.get("schedules", [])
    if not isinstance(raw, list):
        return []

    entries = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        pipeline = item.get("pipeline", "").strip()
        if not pipeline:
            continue
        entries.append(
            {
                "pipeline": pipeline,
                "interval": int(item.get("interval", DEFAULT_INTERVAL)),
                "enabled": bool(item.get("enabled", True)),
            }
        )
    return entries


def build_scheduler_from_config(config: Dict[str, Any], callback_factory):
    """Instantiate and populate a PipelineScheduler from config.

    ``callback_factory(pipeline_name)`` must return a zero-argument callable.
    """
    from pipewatch.scheduler import PipelineScheduler  # local import avoids cycles

    scheduler = PipelineScheduler()
    for entry in parse_schedule_entries(config):
        name = entry["pipeline"]
        scheduler.register(
            name=name,
            interval_seconds=entry["interval"],
            callback=callback_factory(name),
            enabled=entry["enabled"],
        )
    return scheduler
