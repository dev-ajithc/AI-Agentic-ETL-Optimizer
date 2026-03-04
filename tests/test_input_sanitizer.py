"""Tests for the 6-stage input sanitization pipeline."""

import pytest

from tests.conftest import (
    MALICIOUS_EVAL,
    MALICIOUS_EXEC,
    MALICIOUS_PATH_TRAVERSAL,
    MALICIOUS_SUBPROCESS,
    MALICIOUS_UNKNOWN_IMPORT,
    SIMPLE_PYSPARK,
)
from validation.input_sanitizer import sanitize_input


class TestStage1SizeCheck:
    """Stage 1: token size limit enforcement."""

    def test_accepts_code_within_limit(self) -> None:
        result = sanitize_input(SIMPLE_PYSPARK, max_tokens=10_000)
        assert result.passed is True

    def test_rejects_oversized_code(self) -> None:
        huge_code = "# comment\n" * 100_000
        result = sanitize_input(huge_code, max_tokens=100)
        assert result.passed is False
        assert result.error_code == "E002"
        assert result.rule_violated == "SIZE_CHECK"


class TestStage2EncodingCheck:
    """Stage 2: encoding validation and null-byte stripping."""

    def test_accepts_valid_utf8(self) -> None:
        result = sanitize_input(SIMPLE_PYSPARK)
        assert result.passed is True

    def test_strips_null_bytes(self) -> None:
        code_with_null = SIMPLE_PYSPARK + "\x00"
        result = sanitize_input(code_with_null)
        assert result.passed is True
        assert result.sanitized_code is not None
        assert "\x00" not in result.sanitized_code


class TestStage3SyntaxCheck:
    """Stage 3: Python syntax validation."""

    def test_accepts_valid_syntax(self) -> None:
        result = sanitize_input(SIMPLE_PYSPARK)
        assert result.passed is True

    def test_rejects_syntax_error(self) -> None:
        bad_code = "def broken(\n    x\n"
        result = sanitize_input(bad_code)
        assert result.passed is False
        assert result.error_code == "E001"
        assert result.rule_violated == "SYNTAX_CHECK"


class TestStage4DangerousPatterns:
    """Stage 4: dangerous function call blocking."""

    def test_blocks_os_system(self) -> None:
        result = sanitize_input(MALICIOUS_EXEC)
        assert result.passed is False
        assert result.rule_violated == "DANGEROUS_PATTERN"

    def test_blocks_subprocess(self) -> None:
        result = sanitize_input(MALICIOUS_SUBPROCESS)
        assert result.passed is False
        assert result.rule_violated in {
            "DANGEROUS_PATTERN",
            "IMPORT_WHITELIST",
        }

    def test_blocks_eval(self) -> None:
        result = sanitize_input(MALICIOUS_EVAL)
        assert result.passed is False
        assert result.rule_violated == "DANGEROUS_PATTERN"

    def test_blocks_write_mode_open(self) -> None:
        code_with_write = (
            "from pyspark.sql import SparkSession\n"
            "spark = SparkSession.builder.getOrCreate()\n"
            "f = open('/tmp/file', 'w')\n"
        )
        result = sanitize_input(code_with_write)
        assert result.passed is False
        assert result.rule_violated == "DANGEROUS_WRITE"


class TestStage5ImportWhitelist:
    """Stage 5: import allowlist enforcement."""

    def test_allows_pyspark(self) -> None:
        result = sanitize_input(SIMPLE_PYSPARK)
        assert result.passed is True

    def test_blocks_unknown_import(self) -> None:
        result = sanitize_input(MALICIOUS_UNKNOWN_IMPORT)
        assert result.passed is False
        assert result.rule_violated == "IMPORT_WHITELIST"

    def test_allows_prefect(self) -> None:
        from tests.conftest import SAMPLE_PREFECT

        result = sanitize_input(SAMPLE_PREFECT)
        assert result.passed is True


class TestStage6PathTraversal:
    """Stage 6: path traversal detection."""

    def test_blocks_path_traversal(self) -> None:
        result = sanitize_input(MALICIOUS_PATH_TRAVERSAL)
        assert result.passed is False
        assert result.rule_violated == "PATH_TRAVERSAL"

    def test_blocks_etc_path(self) -> None:
        code = (
            "from pyspark.sql import SparkSession\n"
            "spark = SparkSession.builder.getOrCreate()\n"
            "df = spark.read.text('/etc/passwd')\n"
        )
        result = sanitize_input(code)
        assert result.passed is False
        assert result.rule_violated == "PATH_TRAVERSAL"


class TestSanitizedOutput:
    """Tests for sanitized_code output correctness."""

    def test_returns_sanitized_code_on_pass(self) -> None:
        result = sanitize_input(SIMPLE_PYSPARK)
        assert result.passed is True
        assert result.sanitized_code is not None
        assert len(result.sanitized_code) > 0

    def test_no_sanitized_code_on_failure(self) -> None:
        result = sanitize_input(MALICIOUS_EXEC)
        assert result.passed is False
        assert result.sanitized_code is None

    def test_session_id_parameter(self) -> None:
        result = sanitize_input(
            SIMPLE_PYSPARK, session_id="test-session-123"
        )
        assert result.passed is True
