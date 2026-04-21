"""Middleware that wraps alert dispatch with correlation detection."""
from __future__ import annotations

from typing import Callable, List, Optional

from pipewatch.alerts import AlertChannel, AlertEvent
from pipewatch.correlation import CorrelationAlert, CorrelationWindow, check_correlation
from pipewatch.correlation_config import CorrelationConfig
from pipewatch.metrics import EvaluationResult


class CorrelationMiddleware:
    """Detects correlated failures across pipelines and fires a synthetic alert."""

    def __init__(
        self,
        window: CorrelationWindow,
        config: CorrelationConfig,
        correlation_channels: Optional[List[AlertChannel]] = None,
    ) -> None:
        self._window = window
        self._config = config
        self._channels: List[AlertChannel] = correlation_channels or []
        self._last_correlation_key: Optional[str] = None
        self._dispatched_count = 0
        self._correlation_fires = 0

    def process(
        self,
        result: EvaluationResult,
        dispatch: Callable[[EvaluationResult], None],
    ) -> None:
        """Forward result through dispatch, then check for correlation."""
        dispatch(result)
        self._dispatched_count += 1

        if not self._config.enabled:
            return

        alert = check_correlation(
            self._window,
            result,
            min_pipelines=self._config.min_pipelines,
        )
        if alert is not None:
            key = _correlation_key(alert)
            if key != self._last_correlation_key:
                self._last_correlation_key = key
                self._correlation_fires += 1
                self._fire_correlation_alert(alert)

    def _fire_correlation_alert(self, alert: CorrelationAlert) -> None:
        for channel in self._channels:
            try:
                channel.send(
                    AlertEvent(
                        pipeline="__correlation__",
                        metric_name="correlated_failures",
                        status="critical",
                        message=alert.summary(),
                        value=float(alert.event_count),
                        threshold=float(self._config.min_pipelines),
                    )
                )
            except Exception:  # noqa: BLE001
                pass

    def stats(self) -> dict:
        return {
            "dispatched": self._dispatched_count,
            "correlation_fires": self._correlation_fires,
        }
