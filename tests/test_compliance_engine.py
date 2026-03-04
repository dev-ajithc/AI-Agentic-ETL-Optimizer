"""Tests for the compliance engine node."""

import pytest

from agent.nodes.compliance_engine import (
    _check_rule,
    check_compliance,
)
from compliance.rules import COMPLIANCE_RULES, RULE_INDEX


class TestComplianceRuleIndex:
    """Tests for rule metadata and index correctness."""

    def test_all_expected_rules_present(self) -> None:
        expected = {
            "GDPR-001", "GDPR-002", "GDPR-003",
            "HIPAA-001", "HIPAA-002",
            "SOX-001", "SOX-002",
            "ETL-001", "ETL-002", "ETL-003",
        }
        assert expected == set(RULE_INDEX.keys())

    def test_rule_has_required_fields(self) -> None:
        for rule in COMPLIANCE_RULES:
            assert rule.rule_id
            assert rule.name
            assert rule.profile
            assert rule.description
            assert isinstance(rule.auto_fix, bool)
            assert rule.severity


class TestGDPR001PIIMasking:
    """GDPR-001: PII columns must be masked."""

    def test_fails_when_pii_unmasked(self) -> None:
        rule = RULE_INDEX["GDPR-001"]
        result = _check_rule(
            rule,
            code="df = df.select('email', 'name')",
            pii_report={"pii_column_names": ["email", "name"]},
            parsed_structure={},
        )
        assert result["status"] == "fail"

    def test_passes_when_mask_pii_present(self) -> None:
        rule = RULE_INDEX["GDPR-001"]
        result = _check_rule(
            rule,
            code=(
                "df = df.withColumn('email', mask_pii(col('email'), 'EMAIL'))"
            ),
            pii_report={"pii_column_names": ["email"]},
            parsed_structure={},
        )
        assert result["status"] == "pass"

    def test_passes_when_no_pii_columns(self) -> None:
        rule = RULE_INDEX["GDPR-001"]
        result = _check_rule(
            rule,
            code="df = df.select('product_id', 'revenue')",
            pii_report={"pii_column_names": []},
            parsed_structure={},
        )
        assert result["status"] == "pass"


class TestSOX001AuditColumns:
    """SOX-001: Audit trail columns must be present."""

    def test_fails_when_audit_cols_missing(self) -> None:
        rule = RULE_INDEX["SOX-001"]
        result = _check_rule(
            rule,
            code="df = df.select('product_id')",
            pii_report={},
            parsed_structure={},
        )
        assert result["status"] == "fail"
        assert "_etl_run_id" in result["detail"]

    def test_passes_when_all_audit_cols_present(self) -> None:
        rule = RULE_INDEX["SOX-001"]
        code = (
            "df = df.withColumn('_etl_run_id', F.lit(RUN_ID))\n"
            "df = df.withColumn('_etl_timestamp', F.current_timestamp())\n"
            "df = df.withColumn('_etl_source', F.lit('raw'))\n"
        )
        result = _check_rule(
            rule,
            code=code,
            pii_report={},
            parsed_structure={},
        )
        assert result["status"] == "pass"


class TestSOX002Idempotency:
    """SOX-002: Writes must be idempotent."""

    def test_warns_when_no_idempotency(self) -> None:
        rule = RULE_INDEX["SOX-002"]
        result = _check_rule(
            rule,
            code="df.write.format('parquet').mode('append').save('/out')",
            pii_report={},
            parsed_structure={},
        )
        assert result["status"] == "warn"

    def test_passes_when_overwrite_present(self) -> None:
        rule = RULE_INDEX["SOX-002"]
        result = _check_rule(
            rule,
            code="df.write.format('parquet').mode('overwrite').save('/out')",
            pii_report={},
            parsed_structure={},
        )
        assert result["status"] == "pass"

    def test_passes_when_merge_present(self) -> None:
        rule = RULE_INDEX["SOX-002"]
        result = _check_rule(
            rule,
            code="delta_table.alias('t').merge(df, 't.id = s.id')",
            pii_report={},
            parsed_structure={},
        )
        assert result["status"] == "pass"


class TestETL001SchemaValidation:
    """ETL-001: Schema validation at ingestion boundary."""

    def test_warns_when_no_validation(self) -> None:
        rule = RULE_INDEX["ETL-001"]
        result = _check_rule(
            rule,
            code="df = spark.read.parquet('/data')",
            pii_report={},
            parsed_structure={},
        )
        assert result["status"] == "warn"

    def test_passes_when_struct_type_used(self) -> None:
        rule = RULE_INDEX["ETL-001"]
        code = (
            "schema = StructType([StructField('id', LongType())])\n"
            "df = spark.read.schema(schema).parquet('/data')\n"
        )
        result = _check_rule(
            rule,
            code=code,
            pii_report={},
            parsed_structure={},
        )
        assert result["status"] == "pass"


class TestETL003SelectStar:
    """ETL-003: SELECT * should be avoided."""

    def test_warns_on_select_star(self) -> None:
        rule = RULE_INDEX["ETL-003"]
        result = _check_rule(
            rule,
            code="df = df.select('*')",
            pii_report={},
            parsed_structure={},
        )
        assert result["status"] == "warn"


class TestCheckComplianceNode:
    """Integration tests for the full check_compliance node."""

    def test_overall_fail_on_compliance_fail_code(
        self, compliance_fail_code: str
    ) -> None:
        state = {
            "session_id": "test-c1",
            "rewritten_code": compliance_fail_code,
            "pii_report": {
                "pii_column_names": ["name", "email", "ssn"]
            },
            "parsed_structure": {},
            "compliance_profile": "gdpr,hipaa,sox",
            "target": "snowflake",
            "warnings": [],
        }
        result = check_compliance(state)
        assert (
            result["compliance_report"]["overall_status"]
            in {"fail", "warn"}
        )

    def test_overall_pass_on_compliant_code(
        self, compliance_pass_code: str
    ) -> None:
        state = {
            "session_id": "test-c2",
            "rewritten_code": compliance_pass_code,
            "pii_report": {"pii_column_names": []},
            "parsed_structure": {},
            "compliance_profile": "sox",
            "target": "delta_lake",
            "warnings": [],
        }
        result = check_compliance(state)
        report = result["compliance_report"]
        assert "overall_status" in report
        assert "checks" in report
        assert len(report["checks"]) > 0

    def test_compliance_report_schema_version(
        self, compliance_pass_code: str
    ) -> None:
        state = {
            "session_id": "test-c3",
            "rewritten_code": compliance_pass_code,
            "pii_report": {"pii_column_names": []},
            "parsed_structure": {},
            "compliance_profile": "gdpr",
            "target": "snowflake",
            "warnings": [],
        }
        result = check_compliance(state)
        assert (
            result["compliance_report"]["schema_version"] == "1.0"
        )

    def test_active_profiles_filter(self) -> None:
        state = {
            "session_id": "test-c4",
            "rewritten_code": "df = df.select('id')",
            "pii_report": {"pii_column_names": []},
            "parsed_structure": {},
            "compliance_profile": "sox",
            "target": "snowflake",
            "warnings": [],
        }
        result = check_compliance(state)
        rule_ids = [
            c["id"]
            for c in result["compliance_report"]["checks"]
        ]
        assert any(r.startswith("SOX") for r in rule_ids)
        assert not any(r.startswith("GDPR") for r in rule_ids)
        assert not any(r.startswith("HIPAA") for r in rule_ids)
