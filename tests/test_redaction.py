"""Tests for pipewatch.redaction and pipewatch.redaction_config."""
from __future__ import annotations

import pytest

from pipewatch.alerts import AlertEvent
from pipewatch.redaction import RedactionRegistry, RedactionRule, _MASK
from pipewatch.redaction_config import parse_redaction_rules


def _event(meta: dict | None = None) -> AlertEvent:
    return AlertEvent(
        pipeline="pipe1",
        metric="row_count",
        status="warning",
        value=42.0,
        message="threshold exceeded",
        extra_meta=meta or {},
    )


# ---------------------------------------------------------------------------
# RedactionRule — full mask
# ---------------------------------------------------------------------------

class TestRedactionRuleFullMask:
    def test_masks_top_level_field(self):
        rule = RedactionRule(field="meta.api_key")
        event = _event({"api_key": "secret123"})
        result = rule.apply(event)
        assert result.extra_meta["api_key"] == _MASK

    def test_leaves_other_fields_intact(self):
        rule = RedactionRule(field="meta.api_key")
        event = _event({"api_key": "s", "safe": "keep"})
        result = rule.apply(event)
        assert result.extra_meta["safe"] == "keep"

    def test_missing_field_is_noop(self):
        rule = RedactionRule(field="meta.nonexistent")
        event = _event({"other": "value"})
        result = rule.apply(event)
        assert result.extra_meta == {"other": "value"}

    def test_original_event_is_not_mutated(self):
        rule = RedactionRule(field="meta.token")
        meta = {"token": "abc"}
        event = _event(meta)
        rule.apply(event)
        assert meta["token"] == "abc"


# ---------------------------------------------------------------------------
# RedactionRule — pattern mask
# ---------------------------------------------------------------------------

class TestRedactionRulePattern:
    def test_replaces_pattern_in_string(self):
        rule = RedactionRule(field="meta.auth", pattern=r"Bearer \S+")
        event = _event({"auth": "Bearer tok_abc123 rest"})
        result = rule.apply(event)
        assert result.extra_meta["auth"] == f"{_MASK} rest"

    def test_no_match_leaves_value_unchanged(self):
        rule = RedactionRule(field="meta.auth", pattern=r"tok_\w+")
        event = _event({"auth": "no-token-here"})
        result = rule.apply(event)
        assert result.extra_meta["auth"] == "no-token-here"


# ---------------------------------------------------------------------------
# RedactionRegistry
# ---------------------------------------------------------------------------

def test_registry_applies_all_rules():
    reg = RedactionRegistry()
    reg.add(RedactionRule(field="meta.key1"))
    reg.add(RedactionRule(field="meta.key2"))
    event = _event({"key1": "a", "key2": "b", "key3": "c"})
    result = reg.apply_all(event)
    assert result.extra_meta["key1"] == _MASK
    assert result.extra_meta["key2"] == _MASK
    assert result.extra_meta["key3"] == "c"


def test_registry_len():
    reg = RedactionRegistry()
    assert len(reg) == 0
    reg.add(RedactionRule(field="meta.x"))
    assert len(reg) == 1


# ---------------------------------------------------------------------------
# parse_redaction_rules
# ---------------------------------------------------------------------------

def test_parse_empty_config():
    reg = parse_redaction_rules({})
    assert len(reg) == 0


def test_parse_rules_no_pattern():
    cfg = {"redaction": {"rules": [{"field": "meta.secret"}]}}
    reg = parse_redaction_rules(cfg)
    assert len(reg) == 1


def test_parse_rules_with_pattern():
    cfg = {"redaction": {"rules": [{"field": "meta.auth", "pattern": r"tok_\w+"}]}}
    reg = parse_redaction_rules(cfg)
    event = _event({"auth": "tok_abc xyz"})
    result = reg.apply_all(event)
    assert result.extra_meta["auth"] == f"{_MASK} xyz"


def test_parse_skips_entry_missing_field():
    cfg = {"redaction": {"rules": [{"pattern": r"\d+"}, {"field": "meta.ok"}]}}
    reg = parse_redaction_rules(cfg)
    assert len(reg) == 1


def test_parse_non_list_rules_returns_empty():
    cfg = {"redaction": {"rules": "not-a-list"}}
    reg = parse_redaction_rules(cfg)
    assert len(reg) == 0
