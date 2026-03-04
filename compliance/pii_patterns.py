"""PII regex patterns and column name heuristics reference."""

import re
from typing import NamedTuple


class PIIPattern(NamedTuple):
    """A compiled PII detection regex pattern."""

    entity_type: str
    pattern: re.Pattern
    risk_score: int
    description: str


REGEX_PATTERNS: list[PIIPattern] = [
    PIIPattern(
        entity_type="SSN",
        pattern=re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
        risk_score=4,
        description="US Social Security Number",
    ),
    PIIPattern(
        entity_type="EMAIL",
        pattern=re.compile(
            r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b"
        ),
        risk_score=3,
        description="Email address",
    ),
    PIIPattern(
        entity_type="PHONE",
        pattern=re.compile(
            r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"
        ),
        risk_score=2,
        description="US phone number",
    ),
    PIIPattern(
        entity_type="CREDIT_CARD",
        pattern=re.compile(
            r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}"
            r"|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12})\b"
        ),
        risk_score=4,
        description="Credit card number",
    ),
    PIIPattern(
        entity_type="IP_ADDRESS",
        pattern=re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
        risk_score=2,
        description="IPv4 address",
    ),
    PIIPattern(
        entity_type="PASSPORT",
        pattern=re.compile(r"\b[A-Z]{1,2}[0-9]{6,9}\b"),
        risk_score=3,
        description="Passport number",
    ),
    PIIPattern(
        entity_type="IBAN",
        pattern=re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{4}\d{7,}\b"),
        risk_score=4,
        description="International Bank Account Number",
    ),
    PIIPattern(
        entity_type="NPI",
        pattern=re.compile(r"\bNPI[\s:\-]?\d{10}\b"),
        risk_score=3,
        description="US National Provider Identifier",
    ),
]

PII_COLUMN_NAMES: frozenset[str] = frozenset(
    {
        "name",
        "first_name",
        "last_name",
        "fullname",
        "full_name",
        "given_name",
        "surname",
        "email",
        "email_address",
        "mail",
        "phone",
        "phone_number",
        "mobile",
        "cell",
        "telephone",
        "dob",
        "date_of_birth",
        "birthdate",
        "birth_date",
        "age",
        "ssn",
        "social_security",
        "social_security_number",
        "sin",
        "address",
        "street",
        "street_address",
        "city",
        "state",
        "zip",
        "zipcode",
        "zip_code",
        "postal_code",
        "country",
        "mrn",
        "medical_record",
        "patient_id",
        "patient_name",
        "diagnosis",
        "condition",
        "passport",
        "passport_number",
        "credit_card",
        "card_number",
        "cvv",
        "ccn",
        "ip",
        "ip_address",
        "ipv4",
        "ipv6",
        "gender",
        "sex",
        "race",
        "ethnicity",
        "religion",
        "salary",
        "income",
        "wage",
        "compensation",
        "account_number",
        "bank_account",
        "routing_number",
        "iban",
        "license",
        "drivers_license",
        "dl_number",
        "national_id",
        "national_id_number",
        "tax_id",
        "ein",
        "tin",
        "npi",
        "username",
        "user_id",
        "biometric",
        "fingerprint",
        "face_id",
    }
)

CRITICAL_ENTITY_TYPES: frozenset[str] = frozenset(
    {"SSN", "CREDIT_CARD", "PASSPORT", "IBAN"}
)
