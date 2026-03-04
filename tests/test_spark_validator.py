"""Tests for the Spark validation node and sandbox."""

from unittest.mock import patch

import pytest

from agent.nodes.validator import (
    _check_imports,
    _check_syntax,
    _infer_synthetic_schema,
    validate_spark,
)


class TestSyntaxChecker:
    """Tests for _check_syntax utility."""

    def test_valid_syntax_returns_true(self) -> None:
        code = "x = 1\nprint(x)\n"
        valid, err = _check_syntax(code)
        assert valid is True
        assert err is None

    def test_invalid_syntax_returns_false(self) -> None:
        code = "def broken(\n   x"
        valid, err = _check_syntax(code)
        assert valid is False
        assert err is not None

    def test_empty_string_is_valid(self) -> None:
        valid, err = _check_syntax("")
        assert valid is True

    def test_complex_valid_code(self) -> None:
        code = (
            "from pyspark.sql import SparkSession\n"
            "spark = SparkSession.builder.getOrCreate()\n"
            "df = spark.read.parquet('/data')\n"
            "df.write.parquet('/out')\n"
        )
        valid, err = _check_syntax(code)
        assert valid is True


class TestImportChecker:
    """Tests for _check_imports utility."""

    def test_allows_pyspark_import(self) -> None:
        code = "from pyspark.sql import SparkSession\n"
        valid, err = _check_imports(code)
        assert valid is True
        assert err is None

    def test_allows_pandas(self) -> None:
        code = "import pandas as pd\n"
        valid, err = _check_imports(code)
        assert valid is True

    def test_allows_numpy(self) -> None:
        code = "import numpy as np\n"
        valid, err = _check_imports(code)
        assert valid is True

    def test_blocks_unknown_import(self) -> None:
        code = "import flask\n"
        valid, err = _check_imports(code)
        assert valid is False
        assert "flask" in err

    def test_allows_prefect(self) -> None:
        code = "from prefect import flow, task\n"
        valid, err = _check_imports(code)
        assert valid is True

    def test_allows_multiple_valid_imports(self) -> None:
        code = (
            "from pyspark.sql import SparkSession\n"
            "from pyspark.sql import functions as F\n"
            "from pyspark.sql.types import StructType\n"
            "import pandas as pd\n"
            "import datetime\n"
        )
        valid, err = _check_imports(code)
        assert valid is True


class TestSchemaInference:
    """Tests for _infer_synthetic_schema utility."""

    def test_returns_default_schema_for_empty_columns(
        self,
    ) -> None:
        schema = _infer_synthetic_schema({})
        assert len(schema) > 0
        col_names = [s[0] for s in schema]
        assert "id" in col_names

    def test_infers_timestamp_for_date_columns(self) -> None:
        parsed = {"columns": ["created_at", "updated_ts"]}
        schema = _infer_synthetic_schema(parsed)
        type_map = dict(schema)
        assert type_map.get("created_at") == "TimestampType()"

    def test_infers_long_for_id_columns(self) -> None:
        parsed = {"columns": ["user_id", "order_id"]}
        schema = _infer_synthetic_schema(parsed)
        type_map = dict(schema)
        assert type_map.get("user_id") == "LongType()"

    def test_infers_double_for_amount_columns(self) -> None:
        parsed = {"columns": ["price", "amount", "total"]}
        schema = _infer_synthetic_schema(parsed)
        type_map = dict(schema)
        assert type_map.get("price") == "DoubleType()"

    def test_infers_string_for_generic_columns(self) -> None:
        parsed = {"columns": ["status", "region", "category"]}
        schema = _infer_synthetic_schema(parsed)
        type_map = dict(schema)
        assert type_map.get("status") == "StringType()"

    def test_caps_at_20_columns(self) -> None:
        parsed = {
            "columns": [f"col_{i}" for i in range(50)]
        }
        schema = _infer_synthetic_schema(parsed)
        assert len(schema) <= 20


class TestValidateSparkNode:
    """Integration tests for the validate_spark node."""

    def test_fails_on_empty_rewritten_code(
        self, agent_state_base: dict
    ) -> None:
        state = dict(agent_state_base)
        state["rewritten_code"] = ""
        state["parsed_structure"] = {"columns": ["id"]}
        result = validate_spark(state)
        assert result["validation_result"]["passed"] is False

    def test_fails_on_syntax_error_in_rewritten(
        self, agent_state_base: dict
    ) -> None:
        state = dict(agent_state_base)
        state["rewritten_code"] = "def broken(\n   x"
        state["parsed_structure"] = {"columns": ["id"]}
        result = validate_spark(state)
        vr = result["validation_result"]
        assert vr["passed"] is False
        assert vr["syntax_valid"] is False

    def test_increments_retry_count_on_syntax_fail(
        self, agent_state_base: dict
    ) -> None:
        state = dict(agent_state_base)
        state["rewritten_code"] = "def broken(\n   x"
        state["parsed_structure"] = {"columns": []}
        state["retry_count"] = 1
        result = validate_spark(state)
        assert result["retry_count"] == 2

    def test_fails_on_unknown_import(
        self, agent_state_base: dict
    ) -> None:
        state = dict(agent_state_base)
        state["rewritten_code"] = (
            "import flask\nfrom flask import Flask\n"
        )
        state["parsed_structure"] = {"columns": []}
        result = validate_spark(state)
        vr = result["validation_result"]
        assert vr["passed"] is False
        assert vr["imports_valid"] is False

    def test_sandbox_called_for_valid_syntax_and_imports(
        self, agent_state_base: dict
    ) -> None:
        state = dict(agent_state_base)
        state["rewritten_code"] = (
            "from pyspark.sql import SparkSession\n"
            "spark = SparkSession.builder.getOrCreate()\n"
        )
        state["parsed_structure"] = {"columns": ["id", "name"]}

        mock_result = {
            "passed": True,
            "runtime_error": None,
            "output_schema": None,
            "execution_time_s": 1.5,
            "synthetic_rows_processed": 10,
        }

        with patch(
            "agent.nodes.validator.run_spark_sandbox",
            return_value=mock_result,
        ):
            result = validate_spark(state)

        vr = result["validation_result"]
        assert vr["passed"] is True
        assert vr["syntax_valid"] is True
        assert vr["imports_valid"] is True

    def test_sandbox_failure_increments_retry(
        self, agent_state_base: dict
    ) -> None:
        state = dict(agent_state_base)
        state["rewritten_code"] = (
            "from pyspark.sql import SparkSession\n"
            "spark = SparkSession.builder.getOrCreate()\n"
        )
        state["parsed_structure"] = {"columns": []}
        state["retry_count"] = 0

        mock_result = {
            "passed": False,
            "runtime_error": "AnalysisException: column not found",
            "output_schema": None,
            "execution_time_s": 2.0,
            "synthetic_rows_processed": 0,
        }

        with patch(
            "agent.nodes.validator.run_spark_sandbox",
            return_value=mock_result,
        ):
            result = validate_spark(state)

        assert result["retry_count"] == 1
        assert result["validation_result"]["passed"] is False
