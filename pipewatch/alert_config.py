"""Parse alert channel configuration from config dicts."""

from typing import List
from pipewatch.alerts import EmailAlertChannel, LogAlertChannel


def parse_alert_channels(config: dict) -> list:
    """Build alert channel objects from a config dict.

    Expected config structure:
        alerts:
          - type: log
            level: warning
          - type: email
            smtp_host: smtp.example.com
            smtp_port: 587
            sender: alerts@example.com
            recipients:
              - ops@example.com
            username: alerts@example.com
            password: secret
    """
    channels = []
    for entry in config.get("alerts", []):
        kind = entry.get("type", "log")
        if kind == "email":
            channels.append(
                EmailAlertChannel(
                    smtp_host=entry["smtp_host"],
                    smtp_port=int(entry.get("smtp_port", 587)),
                    sender=entry["sender"],
                    recipients=entry.get("recipients", []),
                    username=entry.get("username"),
                    password=entry.get("password"),
                )
            )
        elif kind == "log":
            channels.append(LogAlertChannel(level=entry.get("level", "warning")))
        else:
            raise ValueError(f"Unknown alert channel type: {kind!r}")
    return channels
