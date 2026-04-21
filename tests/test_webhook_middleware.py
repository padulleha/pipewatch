"""Tests for pipewatch.webhook_middleware."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerts import AlertEvent
from pipewatch.webhook import WebhookChannel
from pipewatch.webhook_middleware import WebhookMiddleware


def _event(status="critical"):
    return AlertEvent(
        pipeline="pipe",
        metric="error_rate",
        status=status,
        value=0.9,
        threshold=0.5,
        rule_name="high_errors",
    )


def _mock_channel(success=True) -> WebhookChannel:
    ch = MagicMock(spec=WebhookChannel)
    ch.url = "http://mock.example.com"
    ch.send.return_value = success
    return ch


class TestWebhookMiddleware:
    def test_calls_all_channels(self):
        ch1 = _mock_channel(True)
        ch2 = _mock_channel(True)
        mw = WebhookMiddleware(channels=[ch1, ch2])
        mw.process(_event())
        ch1.send.assert_called_once()
        ch2.send.assert_called_once()

    def test_calls_downstream(self):
        ch = _mock_channel(True)
        downstream = MagicMock()
        mw = WebhookMiddleware(channels=[ch], downstream=downstream)
        evt = _event()
        mw.process(evt)
        downstream.assert_called_once_with(evt)

    def test_downstream_called_even_on_failure(self):
        ch = _mock_channel(False)
        downstream = MagicMock()
        mw = WebhookMiddleware(channels=[ch], downstream=downstream)
        mw.process(_event())
        downstream.assert_called_once()

    def test_stats_track_success_and_failure(self):
        channels = [_mock_channel(True), _mock_channel(False)]
        mw = WebhookMiddleware(channels=channels)
        mw.process(_event())
        s = mw.stats()
        assert s["success"] == 1
        assert s["failure"] == 1
        assert s["total_dispatched"] == 2

    def test_reset_stats(self):
        ch = _mock_channel(True)
        mw = WebhookMiddleware(channels=[ch])
        mw.process(_event())
        mw.reset_stats()
        s = mw.stats()
        assert s["total_dispatched"] == 0

    def test_returns_dispatch_results(self):
        ch = _mock_channel(True)
        mw = WebhookMiddleware(channels=[ch])
        results = mw.process(_event())
        assert len(results) == 1
        assert results[0].success is True

    def test_no_channels_no_error(self):
        mw = WebhookMiddleware(channels=[])
        results = mw.process(_event())
        assert results == []
        assert mw.stats()["total_dispatched"] == 0
