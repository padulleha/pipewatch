"""Simple interval-based scheduler for periodic pipeline checks."""

import time
import threading
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional
import logging

logger = logging.getLogger(__name__)


@dataclass
class ScheduledJob:
    name: str
    interval_seconds: int
    callback: Callable
    enabled: bool = True
    last_run: Optional[float] = None
    run_count: int = 0


class PipelineScheduler:
    def __init__(self):
        self._jobs: Dict[str, ScheduledJob] = {}
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def register(self, name: str, interval_seconds: int, callback: Callable, enabled: bool = True):
        self._jobs[name] = ScheduledJob(
            name=name,
            interval_seconds=interval_seconds,
            callback=callback,
            enabled=enabled,
        )
        logger.debug("Registered job '%s' every %ds", name, interval_seconds)

    def unregister(self, name: str):
        self._jobs.pop(name, None)

    def _tick(self):
        while not self._stop_event.is_set():
            now = time.time()
            for job in list(self._jobs.values()):
                if not job.enabled:
                    continue
                if job.last_run is None or (now - job.last_run) >= job.interval_seconds:
                    try:
                        job.callback()
                    except Exception as exc:  # noqa: BLE001
                        logger.error("Job '%s' raised: %s", job.name, exc)
                    job.last_run = time.time()
                    job.run_count += 1
            self._stop_event.wait(1)

    def start(self):
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._tick, daemon=True, name="pipewatch-scheduler")
        self._thread.start()
        logger.info("Scheduler started with %d job(s)", len(self._jobs))

    def stop(self, timeout: float = 5.0):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        logger.info("Scheduler stopped")

    def job_info(self, name: str) -> Optional[ScheduledJob]:
        return self._jobs.get(name)
