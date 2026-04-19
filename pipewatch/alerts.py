"""Alert channels and notification dispatch for pipewatch."""

from dataclasses import dataclass, field
from typing import Optional
import smtplib
import logging
from email.mime.text import MIMEText
from pipewatch.metrics import EvaluationResult

logger = logging.getLogger(__name__)


@dataclass
class AlertEvent:
    pipeline: str
    metric_name: str
    status: str  # 'warning' or 'critical'
    value: float
    threshold: float
    message: str = ""

    def summary(self) -> str:
        return (
            f"[{self.status.upper()}] {self.pipeline}/{self.metric_name} "
            f"= {self.value} (threshold: {self.threshold})"
        )


@dataclass
class EmailAlertChannel:
    smtp_host: str
    smtp_port: int
    sender: str
    recipients: list
    username: Optional[str] = None
    password: Optional[str] = None

    def send(self, event: AlertEvent) -> bool:
        subject = f"Pipewatch Alert: {event.summary()}"
        body = event.message or event.summary()
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = self.sender
        msg["To"] = ", ".join(self.recipients)
        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.username and self.password:
                    server.login(self.username, self.password)
                server.sendmail(self.sender, self.recipients, msg.as_string())
            logger.info("Alert email sent: %s", subject)
            return True
        except Exception as exc:
            logger.error("Failed to send alert email: %s", exc)
            return False


@dataclass
class LogAlertChannel:
    level: str = "warning"

    def send(self, event: AlertEvent) -> bool:
        log_fn = getattr(logger, self.level, logger.warning)
        log_fn("ALERT: %s", event.summary())
        return True


def dispatch_alerts(results: list, pipeline: str, channels: list) -> list:
    """Dispatch alerts for any non-ok evaluation results."""
    dispatched = []
    for result in results:
        if result.status in ("warning", "critical"):
            event = AlertEvent(
                pipeline=pipeline,
                metric_name=result.metric_name,
                status=result.status,
                value=result.value,
                threshold=result.threshold,
                message=result.message,
            )
            for channel in channels:
                channel.send(event)
            dispatched.append(event)
    return dispatched
