"""LangGraph tool wrapper for sandboxed Spark execution."""

from validation.spark_sandbox import run_spark_sandbox


def spark_runner_tool(
    code: str,
    schema: list[tuple[str, str]],
    timeout_s: int = 60,
    session_id: str = "unknown",
) -> dict:
    """
    LangGraph-callable tool wrapping the Spark sandbox.

    Args:
        code: Python code to validate.
        schema: List of (column_name, spark_type_string) tuples.
        timeout_s: Subprocess timeout in seconds.
        session_id: Session identifier for logging.

    Returns:
        Validation result dict with keys:
        passed, runtime_error, output_schema,
        execution_time_s, synthetic_rows_processed.
    """
    return run_spark_sandbox(
        code=code,
        schema=schema,
        timeout_s=timeout_s,
        session_id=session_id,
    )
