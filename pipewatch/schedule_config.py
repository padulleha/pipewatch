"""Parse scheduler configuration from pipewatch config dicts."""

from typing import Any, Dict, List


DEFAULT_INTERVAL = 60  # seconds
MIN_INTERVAL = 1  # seconds


def parse_schedule_entries(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return a list of schedule entry dicts from the top-level config.

    Expected config shape::

        schedules:
          - pipeline: my_pipeline
            interval: 120
            enabled: true

    Raises:
        ValueError: If an entry's interval is less than MIN_INTERVAL.
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
        interval = int(item.get("interval", DEFAULT_INTERVAL))
        if interval < MIN_INTERVAL:
            raise ValueError(
                f"Schedule entry for '{pipeline}' has invalid interval {interval!r}: "
                f"must be at least {MIN_INTERVAL} second(s)."
            )
        entries.append(
            {
                "pipeline": pipeline,
                "interval": interval,
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
