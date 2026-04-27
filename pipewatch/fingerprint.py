"""Alert fingerprinting — generate stable identifiers for alert events.

A fingerprint uniquely identifies an alert by its pipeline, metric, status,
and optionally its threshold rule, so that downstream systems can correlate,
dedup, or track recurring alerts across time.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Optional

from pipewatch.metrics import EvaluationResult


@dataclass
class FingerprintConfig:
    """Controls which fields participate in fingerprint generation."""
    include_pipeline: bool = True
    include_metric: bool = True
    include_status: bool = True
    include_threshold: bool = False  # opt-in: makes fingerprint rule-specific
    include_tags: bool = False        # opt-in: makes fingerprint tag-specific
    salt: str = ""                    # optional namespace / tenant salt


@dataclass
class AlertFingerprint:
    hex: str
    components: dict = field(default_factory=dict)

    def __str__(self) -> str:
        return self.hex

    def short(self, length: int = 8) -> str:
        """Return a short human-readable prefix."""
        return self.hex[:length]


def compute_fingerprint(
    result: EvaluationResult,
    config: Optional[FingerprintConfig] = None,
) -> AlertFingerprint:
    """Compute a deterministic SHA-256 fingerprint for an EvaluationResult.

    Args:
        result: The evaluation result to fingerprint.
        config: Fingerprint configuration controlling included fields.

    Returns:
        An AlertFingerprint with the hex digest and contributing components.
    """
    if config is None:
        config = FingerprintConfig()

    components: dict = {}

    if config.salt:
        components["salt"] = config.salt
    if config.include_pipeline:
        components["pipeline"] = result.metric.pipeline
    if config.include_metric:
        components["metric"] = result.metric.name
    if config.include_status:
        components["status"] = result.status
    if config.include_threshold and result.rule is not None:
        components["threshold_warning"] = result.rule.warning
        components["threshold_critical"] = result.rule.critical
    if config.include_tags and result.metric.tags:
        # Sort for stability
        components["tags"] = dict(sorted(result.metric.tags.items()))

    payload = json.dumps(components, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(payload.encode()).hexdigest()
    return AlertFingerprint(hex=digest, components=components)
