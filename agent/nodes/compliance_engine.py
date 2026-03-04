"""Compliance engine node — evaluates rewritten code against rule sets."""

import json
import re
from datetime import datetime, timezone
from typing import Any

import structlog

from agent.state import AgentState
from compliance.rules import COMPLIANCE_RULES, ComplianceRule

logger = structlog.get_logger(__name__)

_AUDIT_COLUMNS = {"_etl_run_id", "_etl_timestamp", "_etl_source"}


def _check_rule(
    rule: ComplianceRule,
    code: str,
    pii_report: dict,
    parsed_structure: dict,
) -> dict[str, Any]:
    """Evaluate a single compliance rule against the rewritten code."""
    status = "pass"
    detail = ""
    remediation = ""

    if rule.rule_id == "GDPR-001":
        pii_cols = pii_report.get("pii_column_names", [])
        unmasked = [
            c for c in pii_cols if f"mask_pii" not in code
        ]
        if unmasked:
            status = "fail"
            detail = (
                f"PII columns without masking: "
                + ", ".join(unmasked)
            )
            remediation = (
                "Add mask_pii() calls for detected PII columns"
            )
        else:
            detail = "No unmasked PII columns detected"

    elif rule.rule_id == "GDPR-002":
        if "retention" not in code.lower():
            status = "warn"
            detail = "No data retention policy comment found"
            remediation = (
                "Add a comment describing data retention policy"
            )
        else:
            detail = "Retention policy comment present"

    elif rule.rule_id == "GDPR-003":
        cross_border_keywords = [
            "cross.border", "transfer", "eu", "gdpr",
            "international", "overseas"
        ]
        if any(k in code.lower() for k in cross_border_keywords):
            status = "warn"
            detail = (
                "Possible cross-border data transfer — "
                "ensure GDPR Art.46 compliance"
            )
            remediation = (
                "Document legal basis for cross-border transfer"
            )
        else:
            detail = "No cross-border transfer indicators found"

    elif rule.rule_id == "HIPAA-001":
        phi_fields = {
            "name", "dob", "ssn", "mrn", "address",
            "phone", "email", "date_of_birth"
        }
        pii_cols = {
            c.lower()
            for c in pii_report.get("pii_column_names", [])
        }
        unmasked_phi = phi_fields & pii_cols
        if unmasked_phi and "mask_pii" not in code:
            status = "fail"
            detail = (
                "PHI fields without masking: "
                + ", ".join(unmasked_phi)
            )
            remediation = "Apply HIPAA Safe Harbor or Expert Determination"
        else:
            detail = "PHI masking present or no PHI detected"

    elif rule.rule_id == "HIPAA-002":
        if "encrypt" not in code.lower():
            status = "warn"
            detail = "No encryption-at-rest comment or call found"
            remediation = (
                "Add comment confirming encryption at rest is configured"
            )
        else:
            detail = "Encryption reference present"

    elif rule.rule_id == "SOX-001":
        missing_cols = [
            c for c in _AUDIT_COLUMNS if c not in code
        ]
        if missing_cols:
            status = "fail"
            detail = (
                "Missing audit columns: "
                + ", ".join(missing_cols)
            )
            remediation = (
                "Add _etl_run_id, _etl_timestamp, _etl_source columns"
            )
        else:
            detail = "All SOX audit columns present"

    elif rule.rule_id == "SOX-002":
        has_idempotency = (
            "MERGE" in code.upper()
            or "mergeInto" in code
            or "idempotent" in code.lower()
            or "OVERWRITE" in code.upper()
            or "overwrite" in code.lower()
        )
        if not has_idempotency:
            status = "warn"
            detail = (
                "No idempotency guard detected "
                "(no MERGE/OVERWRITE pattern)"
            )
            remediation = (
                "Use MERGE or INSERT OVERWRITE for idempotent writes"
            )
        else:
            detail = "Idempotency pattern detected"

    elif rule.rule_id == "ETL-001":
        has_validation = (
            "StructType" in code
            or "schema" in code.lower()
            or "assert" in code
            or "expect" in code
            or "validate" in code.lower()
        )
        if not has_validation:
            status = "warn"
            detail = "No schema validation at ingestion boundary"
            remediation = (
                "Add StructType schema or Great Expectations suite"
            )
        else:
            detail = "Schema validation present"

    elif rule.rule_id == "ETL-002":
        has_non_det = bool(
            re.search(
                r"current_timestamp\(\)|now\(\)|datetime\.now\(",
                code,
            )
        )
        has_run_id = "_etl_run_id" in code or "run_id" in code
        if has_non_det and not has_run_id:
            status = "warn"
            detail = (
                "Non-deterministic timestamp used without run_id seed"
            )
            remediation = "Pin run_id to make runs reproducible"
        else:
            detail = "No non-deterministic timestamp issue"

    elif rule.rule_id == "ETL-003":
        if re.search(r"select\s*\(\s*[\"']\*[\"']", code) or (
            re.search(r"\.select\(\s*\*", code)
        ):
            status = "warn"
            detail = "SELECT * detected — prefer explicit column list"
            remediation = "Replace SELECT * with explicit column names"
        else:
            detail = "No unbounded SELECT * detected"

    return {
        "id": rule.rule_id,
        "name": rule.name,
        "profile": rule.profile,
        "status": status,
        "detail": detail,
        "remediation": remediation,
        "auto_fix": rule.auto_fix,
    }


def check_compliance(state: AgentState) -> AgentState:
    """Run all enabled compliance rules and build compliance report."""
    session_id = state["session_id"]
    code = state.get("rewritten_code", "")
    pii_report = state.get("pii_report", {})
    parsed = state.get("parsed_structure", {})
    profiles_str = state.get("compliance_profile", "gdpr,hipaa,sox")
    active_profiles = {
        p.strip().lower() for p in profiles_str.split(",")
    }

    logger.info(
        "compliance_check_start",
        session_id=session_id,
        profiles=list(active_profiles),
    )

    checks = []
    for rule in COMPLIANCE_RULES:
        if rule.profile.lower() not in active_profiles and rule.profile.lower() != "all":
            continue
        result = _check_rule(rule, code, pii_report, parsed)
        checks.append(result)

    statuses = [c["status"] for c in checks]
    if "fail" in statuses:
        overall = "fail"
    elif "warn" in statuses:
        overall = "warn"
    else:
        overall = "pass"

    audit_cols_present = [
        c for c in _AUDIT_COLUMNS if c in code
    ]

    compliance_report = {
        "schema_version": "1.0",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "target": state.get("target", "unknown"),
        "compliance_profiles": list(active_profiles),
        "overall_status": overall,
        "checks": checks,
        "pii_summary": {
            "entities_detected": pii_report.get(
                "entity_count", 0
            ),
            "risk_level": pii_report.get("risk_level", "low"),
            "masked_in_output": "mask_pii" in code,
        },
        "audit_columns_added": audit_cols_present,
        "idempotency_guard": bool(
            re.search(
                r"MERGE|mergeInto|overwrite|OVERWRITE", code
            )
        ),
        "schema_validation": bool(
            re.search(
                r"StructType|schema=|validate|expect", code
            )
        ),
        "lineage_metadata": "_etl_source" in code,
    }

    logger.info(
        "compliance_check_complete",
        session_id=session_id,
        overall=overall,
        checks_run=len(checks),
    )

    warnings = list(state.get("warnings", []))
    if overall == "fail":
        warnings.append(
            "Compliance check FAILED — review report before deployment"
        )
    elif overall == "warn":
        warnings.append(
            "Compliance warnings detected — review before deployment"
        )

    return {
        **state,
        "compliance_report": compliance_report,
        "warnings": warnings,
    }
