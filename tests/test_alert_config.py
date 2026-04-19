"""Additional integration-style tests for alert config parsing."""

from pipewatch.alert_config import parse_alert_channels
from pipewatch.alerts import LogAlertChannel, EmailAlertChannel


def test_default_log_level():
    config = {"alerts": [{"type": "log"}]}
    channels = parse_alert_channels(config)
    assert channels[0].level == "warning"


def test_multiple_channels():
    config = {
        "alerts": [
            {"type": "log", "level": "error"},
            {
                "type": "email",
                "smtp_host": "mail.example.com",
                "smtp_port": 465,
                "sender": "noreply@example.com",
                "recipients": ["team@example.com"],
                "username": "noreply@example.com",
                "password": "hunter2",
            },
        ]
    }
    channels = parse_alert_channels(config)
    assert len(channels) == 2
    assert isinstance(channels[0], LogAlertChannel)
    assert isinstance(channels[1], EmailAlertChannel)
    assert channels[1].username == "noreply@example.com"
    assert channels[1].recipients == ["team@example.com"]


def test_email_default_port():
    config = {"alerts": [{
        "type": "email",
        "smtp_host": "localhost",
        "sender": "a@b.com",
        "recipients": []
    }]}
    channels = parse_alert_channels(config)
    assert channels[0].smtp_port == 587
