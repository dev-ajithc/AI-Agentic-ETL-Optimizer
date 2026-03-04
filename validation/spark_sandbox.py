"""Sandboxed Spark subprocess execution for dry-run validation."""

import json
import os
import resource
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
from pathlib import Path

import structlog

logger = structlog.get_logger(__name__)

_SANDBOX_SCRIPT_TEMPLATE = textwrap.dedent(
    """
import json
import sys
import time

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        BooleanType, DateType, DoubleType, LongType,
        StringType, StructField, StructType, TimestampType,
    )
except ImportError as e:
    print(json.dumps({{
        "passed": False,
        "error": f"PySpark not available: {{e}}",
        "execution_time_s": 0.0,
        "synthetic_rows_processed": 0,
    }}))
    sys.exit(1)

schema_def = {schema_json}
spark = SparkSession.builder \\
    .appName("etl_optimizer_dry_run") \\
    .master("local[1]") \\
    .config("spark.driver.memory", "512m") \\
    .config("spark.executor.memory", "256m") \\
    .config("spark.ui.enabled", "false") \\
    .config("spark.sql.shuffle.partitions", "1") \\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

TYPE_MAP = {{
    "StringType()": StringType(),
    "LongType()": LongType(),
    "DoubleType()": DoubleType(),
    "BooleanType()": BooleanType(),
    "DateType()": DateType(),
    "TimestampType()": TimestampType(),
}}

fields = [
    StructField(col, TYPE_MAP.get(dtype, StringType()), True)
    for col, dtype in schema_def
]
schema = StructType(fields)

rows = []
for i in range(10):
    row = []
    for col, dtype in schema_def:
        if "Long" in dtype:
            row.append(i + 1)
        elif "Double" in dtype:
            row.append(float(i) * 1.5)
        elif "Boolean" in dtype:
            row.append(i % 2 == 0)
        else:
            row.append(f"synthetic_{{col}}_{{i}}")
    rows.append(tuple(row))

_synthetic_df = spark.createDataFrame(rows, schema=schema)

t0 = time.monotonic()
try:
    # Inject synthetic DataFrame as `df` for scripts that expect it
    df = _synthetic_df
    # Execute the user code
    exec(open("/sandbox/rewritten.py").read(), {{"spark": spark, "df": df}})
    elapsed = time.monotonic() - t0
    print(json.dumps({{
        "passed": True,
        "runtime_error": None,
        "output_schema": None,
        "execution_time_s": round(elapsed, 3),
        "synthetic_rows_processed": 10,
    }}))
except Exception as exc:
    elapsed = time.monotonic() - t0
    print(json.dumps({{
        "passed": False,
        "runtime_error": str(exc),
        "output_schema": None,
        "execution_time_s": round(elapsed, 3),
        "synthetic_rows_processed": 0,
    }}))
finally:
    spark.stop()
"""
)


def _set_resource_limits() -> None:
    """Apply ulimit constraints to the subprocess (UNIX only)."""
    try:
        resource.setrlimit(
            resource.RLIMIT_AS,
            (2 * 1024 * 1024 * 1024, resource.RLIM_INFINITY),
        )
        resource.setrlimit(
            resource.RLIMIT_CPU,
            (60, resource.RLIM_INFINITY),
        )
    except (AttributeError, ValueError):
        pass


def run_spark_sandbox(
    code: str,
    schema: list[tuple[str, str]],
    timeout_s: int = 60,
    session_id: str = "unknown",
) -> dict:
    """
    Execute `code` in an isolated subprocess with synthetic data.

    Returns a dict with keys: passed, runtime_error,
    output_schema, execution_time_s, synthetic_rows_processed.
    """
    sandbox_dir = Path(tempfile.mkdtemp(
        prefix=f"etl_optimizer_{session_id}_"
    ))
    logger.info(
        "sandbox_start",
        session_id=session_id,
        sandbox_dir=str(sandbox_dir),
    )

    try:
        code_path = sandbox_dir / "rewritten.py"
        code_path.write_text(code, encoding="utf-8")
        code_path.chmod(0o644)

        schema_json = json.dumps(schema)
        runner_script = _SANDBOX_SCRIPT_TEMPLATE.format(
            schema_json=schema_json
        )
        runner_path = sandbox_dir / "runner.py"
        runner_path.write_text(runner_script, encoding="utf-8")
        runner_path.chmod(0o644)

        env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": os.environ.get("HOME", "/tmp"),
            "PYSPARK_PYTHON": sys.executable,
            "JAVA_HOME": os.environ.get("JAVA_HOME", ""),
            "SPARK_LOCAL_DIRS": str(sandbox_dir),
            "PYTHONPATH": ":".join(sys.path),
        }
        env = {k: v for k, v in env.items() if v}

        t0 = time.monotonic()
        try:
            proc = subprocess.run(
                [sys.executable, str(runner_path)],
                cwd=str(sandbox_dir),
                capture_output=True,
                text=True,
                timeout=timeout_s,
                env=env,
                preexec_fn=_set_resource_limits,
            )
        except subprocess.TimeoutExpired:
            elapsed = time.monotonic() - t0
            logger.warning(
                "sandbox_timeout",
                session_id=session_id,
                timeout_s=timeout_s,
            )
            return {
                "passed": False,
                "runtime_error": (
                    f"E006: Validation timeout after {timeout_s}s"
                ),
                "output_schema": None,
                "execution_time_s": round(elapsed, 3),
                "synthetic_rows_processed": 0,
            }

        elapsed = time.monotonic() - t0

        stdout = proc.stdout.strip()
        stderr = proc.stderr.strip()

        if proc.returncode != 0 and not stdout:
            logger.warning(
                "sandbox_process_error",
                session_id=session_id,
                returncode=proc.returncode,
                stderr=stderr[:500],
            )
            return {
                "passed": False,
                "runtime_error": (
                    f"Process exited {proc.returncode}: "
                    + stderr[:300]
                ),
                "output_schema": None,
                "execution_time_s": round(elapsed, 3),
                "synthetic_rows_processed": 0,
            }

        last_json_line = None
        for line in reversed(stdout.splitlines()):
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                last_json_line = line
                break

        if last_json_line:
            try:
                result = json.loads(last_json_line)
                logger.info(
                    "sandbox_complete",
                    session_id=session_id,
                    passed=result.get("passed"),
                    execution_time_s=result.get("execution_time_s"),
                )
                return result
            except json.JSONDecodeError:
                pass

        return {
            "passed": False,
            "runtime_error": (
                "Could not parse sandbox output. "
                + stdout[:200]
            ),
            "output_schema": None,
            "execution_time_s": round(elapsed, 3),
            "synthetic_rows_processed": 0,
        }

    finally:
        try:
            shutil.rmtree(sandbox_dir, ignore_errors=True)
            logger.debug(
                "sandbox_cleanup",
                session_id=session_id,
                sandbox_dir=str(sandbox_dir),
            )
        except Exception:
            pass
