"""Tests for alert dispatch and channel configuration."""

import pytest
from unittest.mock import MagicMock, patch
from pipewatch.alerts import AlertEvent, LogAlertChannel, EmailAlertChannel, dispatch_alerts
from pipewatch.alert_config import parse_alert_channels


def make_result(status, metric_name="row_count", value=0.0, threshold=100.0):
    r = MagicMock()
    r.status = status
    r.metric_name = metric_name
    r.value = value
    r.threshold = threshold
    r.message = f"{metric_name} is {status}"
    return r


class TestAlertEvent:
    def test_summary_format(self):
        event = AlertEvent("etl", "row_count", "critical", 5.0, 100.0)
        assert "CRITICAL" in event.summary()
        assert "etl/row_count" in event.summary()
        assert "5.0" in event.summary()

    def test_summary_includes_threshold(self):
        event = AlertEvent("etl", "row_count", "warning", 5.0, 100.0)
        assert "100.0" in event.summary()


class TestLogAlertChannel:
    def test_send_returns_true(self):
        channel = LogAlertChannel(level="warning")
        event = AlertEvent("etl", "lag", "warning", 30.0, 20.0)
        assert channel.send(event) is True


class TestEmailAlertChannel:
    def test_send_success(self):
        channel = EmailAlertChannel(
            smtp_host="localhost", smtp_port=25,
            sender="a@b.com", recipients=["c@d.com"]
        )
        event = AlertEvent("etl", "lag", "critical", 99.0, 10.0)
        with patch("smtplib.SMTP") as mock_smtp:
            instance = mock_smtp.return_value.__enter__.return_value
            result = channel.send(event)
        assert result is True
        instance.sendmail.assert_called_once()

    def test_send_failure_returns_false(self):
        channel = EmailAlertChannel(
            smtp_host="bad-host", smtp_port=9999,
            sender="a@b.com", recipients=["c@d.com"]
        )
        event = AlertEvent("etl", "lag", "critical", 99.0, 10.0)
        with patch("smtplib.SMTP", side_effect=ConnectionRefusedError):
            result = channel.send(event)
        assert result is False

    def test_send_to_multiple_recipients(self):
        channel = EmailAlertChannel(
            smtp_host="localhost", smtp_port=25,
            sender="a@b.com", recipients=["c@d.com", "e@f.com"]
        )
        event = AlertEvent("etl", "lag", "critical", 99.0, 10.0)
        with patch("smtplib.SMTP") as mock_smtp:
            instance = mock_smtp.return_value.__enter__.return_value
            result = channel.send(event)
        assert result is True
        _, call_args, _ = instance.sendmail.mock_calls[0]
        assert call_args[1] == ["c@d.com", "e@f.com"]


class TestDispatchAlerts:
    def test_only_non_ok_dispatched(self):
        channel = MagicMock()
        results = [make_result("ok"), make_result("warning"), make_result("critical")]
        dispatched = dispatch_alerts(results, "my_pipeline", [channel])
        assert len(dispatched) == 2
        assert channel.send.call_count == 2

    def test_no_alerts_when_all_ok(self):
        channel = MagicMock()
        results = [make_result("ok"), make_result("ok")]
        dispatched = dispatch_alerts(results, "my_pipeline", [channel])
        assert dispatched == []
        channel.send.assert_not_called()


class TestParseAlertChannels:
    def test_parse_log_channel(self):
        config = {"alerts": [{"type": "log", "level": "error"}]}
        channels = parse_alert_channels(config)
        assert len(channels) == 1
        assert isinstance(channels[0], LogAlertChannel)
        assert channels[0].level == "error"

    def test_parse_email_channel(self):
        config = {"alerts": [{
            "type": "email", "smtp_host": "smtp.x.com",
