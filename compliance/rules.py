"""Compliance rule definitions for GDPR, HIPAA, SOX, and ETL checks."""

import dataclasses


@dataclasses.dataclass(frozen=True)
class ComplianceRule:
    """A single compliance rule definition."""

    rule_id: str
    name: str
    profile: str
    description: str
    auto_fix: bool
    severity: str


COMPLIANCE_RULES: list[ComplianceRule] = [
    ComplianceRule(
        rule_id="GDPR-001",
        name="PII Masking",
        profile="gdpr",
        description=(
            "PII columns must be masked before output"
        ),
        auto_fix=True,
        severity="critical",
    ),
    ComplianceRule(
        rule_id="GDPR-002",
        name="Data Retention Policy",
        profile="gdpr",
        description=(
            "A data retention policy comment must be present"
        ),
        auto_fix=False,
        severity="warning",
    ),
    ComplianceRule(
        rule_id="GDPR-003",
        name="Cross-Border Transfer",
        profile="gdpr",
        description=(
            "Cross-border data transfers must be flagged"
        ),
        auto_fix=False,
        severity="warning",
    ),
    ComplianceRule(
        rule_id="HIPAA-001",
        name="PHI Masking",
        profile="hipaa",
        description=(
            "Protected Health Information fields must be masked"
        ),
        auto_fix=True,
        severity="critical",
    ),
    ComplianceRule(
        rule_id="HIPAA-002",
        name="Encryption at Rest",
        profile="hipaa",
        description=(
            "Encryption-at-rest must be referenced or confirmed"
        ),
        auto_fix=False,
        severity="warning",
    ),
    ComplianceRule(
        rule_id="SOX-001",
        name="Audit Trail Columns",
        profile="sox",
        description=(
            "Audit columns _etl_run_id, _etl_timestamp, "
            "_etl_source must be present"
        ),
        auto_fix=True,
        severity="critical",
    ),
    ComplianceRule(
        rule_id="SOX-002",
        name="Idempotency Guard",
        profile="sox",
        description=(
            "Write operations must be idempotent "
            "(MERGE/OVERWRITE)"
        ),
        auto_fix=True,
        severity="warning",
    ),
    ComplianceRule(
        rule_id="ETL-001",
        name="Schema Validation",
        profile="all",
        description=(
            "Schema validation must be present at ingestion"
        ),
        auto_fix=True,
        severity="warning",
    ),
    ComplianceRule(
        rule_id="ETL-002",
        name="Deterministic Timestamps",
        profile="all",
        description=(
            "Non-deterministic timestamps must use run_id seed"
        ),
        auto_fix=True,
        severity="warning",
    ),
    ComplianceRule(
        rule_id="ETL-003",
        name="Explicit Column Selection",
        profile="all",
        description=(
            "SELECT * should be replaced with explicit columns"
        ),
        auto_fix=False,
        severity="info",
    ),
]


RULE_INDEX: dict[str, ComplianceRule] = {
    r.rule_id: r for r in COMPLIANCE_RULES
}
