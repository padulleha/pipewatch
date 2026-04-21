"""Tests for pipewatch.webhook and pipewatch.webhook_config."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerts import AlertEvent
from pipewatch.metrics import EvaluationResult
from pipewatch.webhook import WebhookChannel, dispatch_to_webhooks
from pipewatch.webhook_config import parse_webhook_channels


def _event(pipeline="pipe1", metric="row_count", status="critical", value=0.0):
    return AlertEvent(
        pipeline=pipeline,
        metric=metric,
        status=status,
        value=value,
        threshold=100.0,
        rule_name="low_rows",
    )


# ---------------------------------------------------------------------------
# WebhookChannel.send
# ---------------------------------------------------------------------------

class TestWebhookChannelSend:
    def test_returns_true_on_200(self):
        ch = WebhookChannel(url="http://example.com/hook")
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        with patch("urllib.request.urlopen", return_value=mock_resp):
            assert ch.send(_event()) is True

    def test_returns_false_on_url_error(self):
        import urllib.error
        ch = WebhookChannel(url="http://bad-host/hook")
        with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("err")):
            assert ch.send(_event()) is False

    def test_payload_contains_pipeline_and_metric(self):
        ch = WebhookChannel(url="http://example.com/hook")
        captured = {}

        def fake_urlopen(req, timeout):
            captured["data"] = json.loads(req.data)
            m = MagicMock()
            m.__enter__ = lambda s: s
            m.__exit__ = MagicMock(return_value=False)
            return m

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            ch.send(_event(pipeline="etl", metric="latency"))

        assert captured["data"]["pipeline"] == "etl"
        assert captured["data"]["metric"] == "latency"

    def test_custom_headers_forwarded(self):
        ch = WebhookChannel(url="http://example.com", headers={"X-Token": "abc"})
        captured = {}

        def fake_urlopen(req, timeout):
            captured["headers"] = dict(req.headers)
            m = MagicMock()
            m.__enter__ = lambda s: s
            m.__exit__ = MagicMock(return_value=False)
            return m

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            ch.send(_event())

        assert captured["headers"].get("X-token") == "abc"


# ---------------------------------------------------------------------------
# dispatch_to_webhooks
# ---------------------------------------------------------------------------

def test_dispatch_returns_results_for_all_channels():
    channels = [
        WebhookChannel(url="http://a.example.com"),
        WebhookChannel(url="http://b.example.com"),
    ]
    mock_resp = MagicMock()
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    with patch("urllib.request.urlopen", return_value=mock_resp):
        results = dispatch_to_webhooks(_event(), channels)
    assert len(results) == 2
    assert all(r.success for r in results)


# ---------------------------------------------------------------------------
# parse_webhook_channels
# ---------------------------------------------------------------------------

class TestParseWebhookChannels:
    def test_empty_config(self):
        assert parse_webhook_channels({}) == []

    def test_single_channel(self):
        cfg = {"webhooks": [{"url": "http://hook.io/x"}]}
        channels = parse_webhook_channels(cfg)
        assert len(channels) == 1
        assert channels[0].url == "http://hook.io/x"
        assert channels[0].timeout == 10

    def test_custom_timeout_and_name(self):
        cfg = {"webhooks": [{"url": "http://x.io", "timeout": 3, "name": "fast"}]}
        ch = parse_webhook_channels(cfg)[0]
        assert ch.timeout == 3
        assert ch.name == "fast"

    def test_skips_entry_without_url(self):
        cfg = {"webhooks": [{"headers": {}}]}
        assert parse_webhook_channels(cfg) == []

    def test_multiple_channels(self):
        cfg = {
            "webhooks": [
                {"url": "http://a.io"},
                {"url": "http://b.io", "name": "b-hook"},
            ]
        }
        channels = parse_webhook_channels(cfg)
        assert len(channels) == 2
        assert channels[1].name == "b-hook"
