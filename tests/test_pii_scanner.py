"""Tests for the PII scanner node."""

import pytest

from agent.nodes.pii_scanner import (
    _compute_risk_level,
    _scan_column_names,
    _scan_regex,
    scan_pii,
)


class TestRegexScanner:
    """Tests for regex-based PII pattern detection."""

    def test_detects_ssn(self) -> None:
        findings = _scan_regex("value = '123-45-6789'")
        types = [f["type"] for f in findings]
        assert "SSN" in types

    def test_detects_email(self) -> None:
        findings = _scan_regex("user = 'test@example.com'")
        types = [f["type"] for f in findings]
        assert "EMAIL" in types

    def test_detects_phone(self) -> None:
        findings = _scan_regex("phone = '555-123-4567'")
        types = [f["type"] for f in findings]
        assert "PHONE" in types

    def test_detects_credit_card(self) -> None:
        findings = _scan_regex("cc = '4111111111111111'")
        types = [f["type"] for f in findings]
        assert "CREDIT_CARD" in types

    def test_detects_ip_address(self) -> None:
        findings = _scan_regex("ip = '192.168.1.100'")
        types = [f["type"] for f in findings]
        assert "IP_ADDRESS" in types

    def test_no_false_positives_on_clean_code(self) -> None:
        clean = (
            "from pyspark.sql import functions as F\n"
            "df = df.filter(F.col('status') == 'active')\n"
        )
        findings = _scan_regex(clean)
        pii_types = {f["type"] for f in findings}
        assert "SSN" not in pii_types
        assert "CREDIT_CARD" not in pii_types
        assert "EMAIL" not in pii_types


class TestColumnNameScanner:
    """Tests for column name heuristic detection."""

    def test_detects_email_column(self) -> None:
        findings = _scan_column_names(["email"])
        assert len(findings) > 0
        assert findings[0]["column"] == "email"

    def test_detects_ssn_column(self) -> None:
        findings = _scan_column_names(["ssn"])
        assert len(findings) > 0

    def test_detects_dob_column(self) -> None:
        findings = _scan_column_names(["dob"])
        assert len(findings) > 0

    def test_detects_mrn_column(self) -> None:
        findings = _scan_column_names(["mrn"])
        assert len(findings) > 0
        assert findings[0]["score"] == 4

    def test_no_false_positives_on_clean_columns(self) -> None:
        findings = _scan_column_names(
            ["product_id", "quantity", "revenue", "region"]
        )
        assert len(findings) == 0

    def test_multiple_pii_columns(self) -> None:
        findings = _scan_column_names(
            ["name", "email", "phone", "product_id"]
        )
        pii_cols = [f["column"] for f in findings]
        assert "name" in pii_cols
        assert "email" in pii_cols
        assert "phone" in pii_cols
        assert "product_id" not in pii_cols


class TestRiskScoring:
    """Tests for risk level computation."""

    def test_empty_findings_is_low(self) -> None:
        assert _compute_risk_level([]) == "low"

    def test_score_2_is_low_or_medium(self) -> None:
        findings = [{"type": "PHONE", "score": 2}]
        level = _compute_risk_level(findings)
        assert level in {"low", "medium"}

    def test_score_3_is_medium_or_high(self) -> None:
        findings = [{"type": "EMAIL", "score": 3}]
        level = _compute_risk_level(findings)
        assert level in {"medium", "high"}

    def test_ssn_is_critical(self) -> None:
        findings = [{"type": "SSN", "score": 4}]
        level = _compute_risk_level(findings)
        assert level == "critical"

    def test_credit_card_is_critical(self) -> None:
        findings = [{"type": "CREDIT_CARD", "score": 4}]
        level = _compute_risk_level(findings)
        assert level == "critical"


class TestScanPIINode:
    """Integration tests for the full scan_pii node."""

    def test_pii_detected_in_code(self, pii_code: str) -> None:
        state = {
            "session_id": "test-123",
            "input_code": pii_code,
            "parsed_structure": {
                "columns": [
                    "name", "email", "ssn", "dob", "phone"
                ]
            },
        }
        result = scan_pii(state)
        assert result["pii_report"]["has_pii"] is True
        assert result["pii_report"]["entity_count"] > 0

    def test_no_pii_in_clean_code(self, no_pii_code: str) -> None:
        state = {
            "session_id": "test-456",
            "input_code": no_pii_code,
            "parsed_structure": {
                "columns": ["product_id", "quantity", "revenue"]
            },
        }
        result = scan_pii(state)
        assert result["pii_report"]["has_pii"] is False

    def test_state_preserved_after_scan(
        self, simple_pyspark_code: str
    ) -> None:
        state = {
            "session_id": "test-789",
            "input_code": simple_pyspark_code,
            "parsed_structure": {"columns": []},
            "token_budget": 8000,
            "target": "snowflake",
        }
        result = scan_pii(state)
        assert result["token_budget"] == 8000
        assert result["target"] == "snowflake"
        assert "pii_report" in result
