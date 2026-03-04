"""Local Spark validation node — sandboxed AST + dry-run check."""

import ast
import dataclasses
import importlib.util
import re

import structlog

from agent.state import AgentState
from config import get_settings
from validation.spark_sandbox import run_spark_sandbox

logger = structlog.get_logger(__name__)


@dataclasses.dataclass
class ValidationResult:
    """Result of local Spark validation."""

    passed: bool
    syntax_valid: bool
    imports_valid: bool
    runtime_error: str | None
    output_schema: dict | None
    execution_time_s: float
    synthetic_rows_processed: int


_ALLOWED_IMPORTS = {
    "pyspark",
    "pandas",
    "numpy",
    "datetime",
    "json",
    "os",
    "re",
    "math",
    "functools",
    "itertools",
    "typing",
    "collections",
    "decimal",
    "uuid",
    "hashlib",
    "logging",
    "prefect",
    "great_expectations",
    "delta",
}


def _check_syntax(code: str) -> tuple[bool, str | None]:
    """Return (valid, error_message)."""
    try:
        ast.parse(code)
        return True, None
    except SyntaxError as exc:
        return False, str(exc)


def _check_imports(code: str) -> tuple[bool, str | None]:
    """Verify all imports are from the allowed set."""
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return False, "Cannot check imports — syntax invalid"

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                top = alias.name.split(".")[0]
                if top not in _ALLOWED_IMPORTS:
                    return (
                        False,
                        f"Import not in allowlist: {alias.name}",
                    )
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                top = node.module.split(".")[0]
                if top not in _ALLOWED_IMPORTS:
                    return (
                        False,
                        f"Import not in allowlist: {node.module}",
                    )
    return True, None


def _infer_synthetic_schema(
    parsed_structure: dict,
) -> list[tuple[str, str]]:
    """
    Infer a simple schema from parsed column names.
    Returns list of (col_name, spark_type_string).
    """
    columns = parsed_structure.get("columns", [])
    schema = []
    for col in columns[:20]:
        lower = col.lower()
        if any(k in lower for k in ["date", "time", "ts", "at"]):
            dtype = "TimestampType()"
        elif any(k in lower for k in ["id", "num", "count", "qty"]):
            dtype = "LongType()"
        elif any(k in lower for k in ["amount", "price", "rate"]):
            dtype = "DoubleType()"
        elif any(k in lower for k in ["flag", "is_", "has_", "active"]):
            dtype = "BooleanType()"
        else:
            dtype = "StringType()"
        schema.append((col, dtype))

    if not schema:
        schema = [
            ("id", "LongType()"),
            ("value", "StringType()"),
            ("amount", "DoubleType()"),
        ]
    return schema


def validate_spark(state: AgentState) -> AgentState:
    """Run syntax, import, and sandbox validation on rewritten code."""
    settings = get_settings()
    session_id = state["session_id"]
    rewritten = state.get("rewritten_code", "")

    if not rewritten:
        logger.warning(
            "validator_no_code", session_id=session_id
        )
        result = ValidationResult(
            passed=False,
            syntax_valid=False,
            imports_valid=False,
            runtime_error="No rewritten code to validate",
            output_schema=None,
            execution_time_s=0.0,
            synthetic_rows_processed=0,
        )
        return {
            **state,
            "validation_result": dataclasses.asdict(result),
        }

    logger.info("validator_start", session_id=session_id)

    syntax_valid, syntax_err = _check_syntax(rewritten)
    if not syntax_valid:
        logger.warning(
            "validator_syntax_fail",
            session_id=session_id,
            error=syntax_err,
        )
        result = ValidationResult(
            passed=False,
            syntax_valid=False,
            imports_valid=False,
            runtime_error=f"Syntax error: {syntax_err}",
            output_schema=None,
            execution_time_s=0.0,
            synthetic_rows_processed=0,
        )
        return {
            **state,
            "validation_result": dataclasses.asdict(result),
            "retry_count": state.get("retry_count", 0) + 1,
        }

    imports_valid, import_err = _check_imports(rewritten)
    if not imports_valid:
        logger.warning(
            "validator_import_fail",
            session_id=session_id,
            error=import_err,
        )
        result = ValidationResult(
            passed=False,
            syntax_valid=True,
            imports_valid=False,
            runtime_error=f"Import error: {import_err}",
            output_schema=None,
            execution_time_s=0.0,
            synthetic_rows_processed=0,
        )
        return {
            **state,
            "validation_result": dataclasses.asdict(result),
            "retry_count": state.get("retry_count", 0) + 1,
        }

    schema = _infer_synthetic_schema(
        state.get("parsed_structure", {})
    )
    sandbox_result = run_spark_sandbox(
        code=rewritten,
        schema=schema,
        timeout_s=settings.spark_validation_timeout_s,
        session_id=session_id,
    )

    passed = sandbox_result.get("passed", False)
    if not passed:
        new_retry = state.get("retry_count", 0) + 1
        logger.warning(
            "validator_sandbox_fail",
            session_id=session_id,
            retry_count=new_retry,
            error=sandbox_result.get("error"),
        )
        return {
            **state,
            "validation_result": {
                "passed": False,
                "syntax_valid": True,
                "imports_valid": True,
                **sandbox_result,
            },
            "retry_count": new_retry,
        }

    logger.info(
        "validator_pass",
        session_id=session_id,
        execution_time_s=sandbox_result.get("execution_time_s"),
    )
    return {
        **state,
        "validation_result": {
            "passed": True,
            "syntax_valid": True,
            "imports_valid": True,
            **sandbox_result,
        },
        "retry_count": 0,
    }
