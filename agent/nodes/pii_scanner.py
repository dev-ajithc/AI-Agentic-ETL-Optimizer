"""PII detection node: regex + column heuristics + spaCy NER."""

import re
from typing import Any

import structlog

from agent.state import AgentState

logger = structlog.get_logger(__name__)

_PII_COLUMN_NAMES = {
    "name", "first_name", "last_name", "fullname", "full_name",
    "email", "email_address", "mail",
    "phone", "phone_number", "mobile", "cell",
    "dob", "date_of_birth", "birthdate", "birth_date",
    "ssn", "social_security", "social_security_number",
    "address", "street", "street_address",
    "zip", "zipcode", "zip_code", "postal_code",
    "mrn", "medical_record", "patient_id",
    "passport", "passport_number",
    "credit_card", "card_number", "cvv",
    "ip", "ip_address", "ipv4", "ipv6",
    "gender", "sex", "race", "ethnicity",
    "salary", "income", "wage",
    "account_number", "bank_account", "routing_number",
    "license", "drivers_license",
    "national_id", "national_id_number",
}

_REGEX_PATTERNS: list[tuple[str, str, int]] = [
    (
        "SSN",
        r"\b\d{3}-\d{2}-\d{4}\b",
        4,
    ),
    (
        "EMAIL",
        r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b",
        3,
    ),
    (
        "PHONE",
        r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b",
        2,
    ),
    (
        "CREDIT_CARD",
        r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}"
        r"|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12})\b",
        4,
    ),
    (
        "IP_ADDRESS",
        r"\b(?:\d{1,3}\.){3}\d{1,3}\b",
        2,
    ),
    (
        "PASSPORT",
        r"\b[A-Z]{1,2}[0-9]{6,9}\b",
        3,
    ),
]

_RISK_LABELS = {1: "low", 2: "low", 3: "medium", 4: "high"}

_CRITICAL_TYPES = {"SSN", "CREDIT_CARD", "PASSPORT"}


def _scan_regex(source: str) -> list[dict[str, Any]]:
    """Find PII patterns via regex in the source string."""
    findings: list[dict[str, Any]] = []
    for entity_type, pattern, base_score in _REGEX_PATTERNS:
        matches = re.findall(pattern, source)
        if matches:
            findings.append(
                {
                    "type": entity_type,
                    "count": len(matches),
                    "score": base_score,
                    "source": "regex",
                    "sample": matches[0][:8] + "…",
                }
            )
    return findings


def _scan_column_names(columns: list[str]) -> list[dict[str, Any]]:
    """Check column names against known PII field name heuristics."""
    findings: list[dict[str, Any]] = []
    for col in columns:
        normalised = col.lower().strip()
        if normalised in _PII_COLUMN_NAMES:
            score = 4 if normalised in {"ssn", "mrn", "passport"} else 3
            findings.append(
                {
                    "type": "COLUMN_NAME",
                    "column": col,
                    "score": score,
                    "source": "heuristic",
                }
            )
    return findings


def _scan_spacy(source: str) -> list[dict[str, Any]]:
    """Use spaCy NER to detect PII entities in string literals."""
    try:
        import spacy

        try:
            nlp = spacy.load("en_core_web_sm")
        except OSError:
            logger.warning(
                "spacy_model_not_found",
                model="en_core_web_sm",
                hint="Run: python -m spacy download en_core_web_sm",
            )
            return []

        string_literals = re.findall(r'["\']([^"\']{10,})["\']', source)
        findings: list[dict[str, Any]] = []
        for literal in string_literals[:50]:
            doc = nlp(literal)
            for ent in doc.ents:
                if ent.label_ in {"PERSON", "ORG", "GPE", "LOC"}:
                    findings.append(
                        {
                            "type": f"NER_{ent.label_}",
                            "text": ent.text[:20],
                            "score": 2,
                            "source": "spacy",
                        }
                    )
        return findings
    except ImportError:
        logger.warning("spacy_not_installed")
        return []


def _compute_risk_level(
    findings: list[dict[str, Any]]
) -> str:
    """Derive overall risk level from finding scores."""
    if not findings:
        return "low"
    max_score = max(f.get("score", 1) for f in findings)
    critical = any(
        f.get("type") in _CRITICAL_TYPES for f in findings
    )
    if critical or max_score >= 4:
        return "critical"
    if max_score == 3:
        return "high"
    if max_score == 2:
        return "medium"
    return "low"


def scan_pii(state: AgentState) -> AgentState:
    """Run PII detection and populate pii_report in state."""
    session_id = state["session_id"]
    source = state["input_code"]
    columns: list[str] = state.get(
        "parsed_structure", {}
    ).get("columns", [])

    logger.info("pii_scan_start", session_id=session_id)

    regex_findings = _scan_regex(source)
    column_findings = _scan_column_names(columns)
    spacy_findings = _scan_spacy(source)

    all_findings = regex_findings + column_findings + spacy_findings
    risk_level = _compute_risk_level(all_findings)

    pii_report = {
        "entities": all_findings,
        "entity_count": len(all_findings),
        "risk_level": risk_level,
        "pii_column_names": [
            f["column"]
            for f in column_findings
            if "column" in f
        ],
        "has_pii": len(all_findings) > 0,
    }

    logger.info(
        "pii_scan_complete",
        session_id=session_id,
        entity_count=len(all_findings),
        risk_level=risk_level,
    )

    return {**state, "pii_report": pii_report}
