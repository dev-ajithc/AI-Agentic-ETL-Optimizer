"""Input sanitization pipeline — 6-stage gate before LLM/execution."""

import ast
import dataclasses
import re
import unicodedata

import structlog

logger = structlog.get_logger(__name__)

_DANGEROUS_CALLS = {
    "os.system",
    "os.popen",
    "os.execv",
    "os.execvp",
    "subprocess.run",
    "subprocess.call",
    "subprocess.Popen",
    "subprocess.check_output",
    "subprocess.check_call",
    "exec",
    "eval",
    "__import__",
    "compile",
    "globals",
    "locals",
    "vars",
    "getattr",
    "setattr",
    "delattr",
    "builtins",
}

_DANGEROUS_WRITE_MODES = re.compile(
    r'open\s*\([^)]*["\'][wa\+]["\']'
)

_NETWORK_CALLS = re.compile(
    r"(?:requests\.|urllib\.|http\.client\.|socket\.|ftplib\.|"
    r"smtplib\.|telnetlib\.|xmlrpc\.)"
)

_PATH_TRAVERSAL = re.compile(r"\.\.[/\\]|/etc/|/proc/|/sys/")

_ALLOWED_IMPORTS = {
    "pyspark",
    "pandas",
    "numpy",
    "prefect",
    "datetime",
    "json",
    "math",
    "re",
    "os",
    "sys",
    "functools",
    "itertools",
    "typing",
    "collections",
    "decimal",
    "uuid",
    "hashlib",
    "logging",
    "pathlib",
    "dataclasses",
    "abc",
    "copy",
    "io",
    "string",
    "time",
    "calendar",
    "enum",
    "operator",
    "struct",
    "base64",
    "csv",
    "gzip",
    "zipfile",
    "great_expectations",
    "delta",
    "pyarrow",
    "boto3",
    "azure",
    "google",
}

_APPROX_CHARS_PER_TOKEN = 4


@dataclasses.dataclass
class SanitizationResult:
    """Result of the 6-stage sanitization pipeline."""

    passed: bool
    error_code: str | None
    error_message: str | None
    rule_violated: str | None
    sanitized_code: str | None


def _stage1_size_check(
    code: str, max_tokens: int
) -> SanitizationResult | None:
    """Reject if estimated token count exceeds limit."""
    estimated_tokens = len(code) // _APPROX_CHARS_PER_TOKEN
    if estimated_tokens > max_tokens:
        return SanitizationResult(
            passed=False,
            error_code="E002",
            error_message=(
                f"Input too large: ~{estimated_tokens} tokens "
                f"(limit: {max_tokens}). Trim code and retry."
            ),
            rule_violated="SIZE_CHECK",
            sanitized_code=None,
        )
    return None


def _stage2_encoding_check(
    code: str,
) -> tuple[SanitizationResult | None, str]:
    """Validate UTF-8 and strip null bytes."""
    try:
        cleaned = code.encode("utf-8").decode("utf-8")
    except UnicodeDecodeError as exc:
        return (
            SanitizationResult(
                passed=False,
                error_code="E001",
                error_message=f"Invalid encoding: {exc}",
                rule_violated="ENCODING_CHECK",
                sanitized_code=None,
            ),
            code,
        )

    cleaned = cleaned.replace("\x00", "")

    nul_stripped = code.replace("\x00", "")
    if nul_stripped != code:
        logger.warning("null_bytes_stripped")

    return None, cleaned


def _stage3_syntax_check(
    code: str,
) -> SanitizationResult | None:
    """Reject if code is not valid Python syntax."""
    try:
        ast.parse(code)
        return None
    except SyntaxError as exc:
        return SanitizationResult(
            passed=False,
            error_code="E001",
            error_message=(
                f"Invalid Python syntax at line {exc.lineno}: "
                f"{exc.msg}"
            ),
            rule_violated="SYNTAX_CHECK",
            sanitized_code=None,
        )


def _stage4_dangerous_pattern_scan(
    code: str,
) -> SanitizationResult | None:
    """Block dangerous function calls and write-mode file opens."""
    for call in _DANGEROUS_CALLS:
        if call in code:
            return SanitizationResult(
                passed=False,
                error_code="E001",
                error_message=(
                    f"Dangerous pattern detected: `{call}`. "
                    "Remove it before submitting."
                ),
                rule_violated="DANGEROUS_PATTERN",
                sanitized_code=None,
            )

    if _DANGEROUS_WRITE_MODES.search(code):
        return SanitizationResult(
            passed=False,
            error_code="E001",
            error_message=(
                "File write operations are not permitted "
                "in submitted code."
            ),
            rule_violated="DANGEROUS_WRITE",
            sanitized_code=None,
        )

    if _NETWORK_CALLS.search(code):
        return SanitizationResult(
            passed=False,
            error_code="E001",
            error_message=(
                "Direct network calls are not permitted "
                "in submitted code."
            ),
            rule_violated="NETWORK_CALL",
            sanitized_code=None,
        )

    return None


def _stage5_import_whitelist(
    code: str,
) -> SanitizationResult | None:
    """Allow only known data-engineering imports."""
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return None

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                top = alias.name.split(".")[0]
                if top not in _ALLOWED_IMPORTS:
                    return SanitizationResult(
                        passed=False,
                        error_code="E001",
                        error_message=(
                            f"Import `{alias.name}` is not in the "
                            "allowed data-engineering import list."
                        ),
                        rule_violated="IMPORT_WHITELIST",
                        sanitized_code=None,
                    )
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                top = node.module.split(".")[0]
                if top not in _ALLOWED_IMPORTS:
                    return SanitizationResult(
                        passed=False,
                        error_code="E001",
                        error_message=(
                            f"Import `{node.module}` is not in the "
                            "allowed data-engineering import list."
                        ),
                        rule_violated="IMPORT_WHITELIST",
                        sanitized_code=None,
                    )
    return None


def _stage6_path_traversal(
    code: str,
) -> SanitizationResult | None:
    """Block path traversal and access to sensitive system paths."""
    if _PATH_TRAVERSAL.search(code):
        return SanitizationResult(
            passed=False,
            error_code="E001",
            error_message=(
                "Path traversal patterns detected (e.g., `../`, "
                "`/etc/`). Remove absolute or traversal paths."
            ),
            rule_violated="PATH_TRAVERSAL",
            sanitized_code=None,
        )
    return None


def sanitize_input(
    code: str,
    max_tokens: int = 10_000,
    session_id: str = "unknown",
) -> SanitizationResult:
    """
    Run the 6-stage sanitization pipeline.

    Returns a SanitizationResult. On success, `sanitized_code`
    contains the cleaned code. On failure, `passed` is False and
    `error_code`/`error_message` describe the rejection reason.

    No rejected input ever reaches the LLM.
    """
    logger.info(
        "sanitizer_start",
        session_id=session_id,
        code_length=len(code),
    )

    result = _stage1_size_check(code, max_tokens)
    if result:
        logger.warning(
            "sanitizer_rejected",
            session_id=session_id,
            rule=result.rule_violated,
        )
        return result

    err, code = _stage2_encoding_check(code)
    if err:
        logger.warning(
            "sanitizer_rejected",
            session_id=session_id,
            rule=err.rule_violated,
        )
        return err

    result = _stage3_syntax_check(code)
    if result:
        logger.warning(
            "sanitizer_rejected",
            session_id=session_id,
            rule=result.rule_violated,
        )
        return result

    result = _stage4_dangerous_pattern_scan(code)
    if result:
        logger.warning(
            "sanitizer_rejected",
            session_id=session_id,
            rule=result.rule_violated,
        )
        return result

    result = _stage5_import_whitelist(code)
    if result:
        logger.warning(
            "sanitizer_rejected",
            session_id=session_id,
            rule=result.rule_violated,
        )
        return result

    result = _stage6_path_traversal(code)
    if result:
        logger.warning(
            "sanitizer_rejected",
            session_id=session_id,
            rule=result.rule_violated,
        )
        return result

    logger.info(
        "sanitizer_passed",
        session_id=session_id,
        final_length=len(code),
    )
    return SanitizationResult(
        passed=True,
        error_code=None,
        error_message=None,
        rule_violated=None,
        sanitized_code=code,
    )
