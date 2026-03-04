"""Tests for LangGraph agent graph nodes (LLM calls mocked)."""

import os
from unittest.mock import MagicMock, patch

import pytest

from agent.nodes.code_parser import parse_job
from agent.nodes.diff_generator import generate_diff
from agent.nodes.pii_scanner import scan_pii


class TestCodeParserNode:
    """Tests for parse_job node."""

    def test_parses_pyspark_imports(
        self, agent_state_base: dict
    ) -> None:
        result = parse_job(agent_state_base)
        parsed = result["parsed_structure"]
        assert "pyspark" in parsed["imports"]

    def test_detects_pyspark_type(
        self, agent_state_base: dict
    ) -> None:
        result = parse_job(agent_state_base)
        assert result["input_type"] == "pyspark"

    def test_detects_prefect_type(
        self, agent_state_base: dict, sample_prefect_code: str
    ) -> None:
        state = dict(agent_state_base)
        state["input_code"] = sample_prefect_code
        result = parse_job(state)
        assert result["input_type"] == "prefect"

    def test_extracts_transforms(
        self, agent_state_base: dict, sample_pyspark_code: str
    ) -> None:
        state = dict(agent_state_base)
        state["input_code"] = sample_pyspark_code
        result = parse_job(state)
        transforms = result["parsed_structure"]["transforms"]
        assert len(transforms) > 0

    def test_handles_syntax_error(
        self, agent_state_base: dict
    ) -> None:
        state = dict(agent_state_base)
        state["input_code"] = "def broken(\n   x"
        result = parse_job(state)
        assert result["error"] is not None
        assert "E001" in result["error"]

    def test_detects_line_count(
        self, agent_state_base: dict
    ) -> None:
        result = parse_job(agent_state_base)
        assert result["parsed_structure"]["line_count"] > 0

    def test_detects_pii_columns(
        self, agent_state_base: dict, pii_code: str
    ) -> None:
        state = dict(agent_state_base)
        state["input_code"] = pii_code
        result = parse_job(state)
        columns = result["parsed_structure"]["columns"]
        assert len(columns) > 0

    def test_state_pass_through(
        self, agent_state_base: dict
    ) -> None:
        result = parse_job(agent_state_base)
        assert result["session_id"] == agent_state_base["session_id"]
        assert result["target"] == agent_state_base["target"]
        assert result["token_budget"] == agent_state_base["token_budget"]

    def test_detects_prefect_flow_names(
        self, agent_state_base: dict, sample_prefect_code: str
    ) -> None:
        state = dict(agent_state_base)
        state["input_code"] = sample_prefect_code
        result = parse_job(state)
        flows = result["parsed_structure"].get("prefect_flows", [])
        assert "etl_pipeline" in flows

    def test_detects_prefect_task_names(
        self, agent_state_base: dict, sample_prefect_code: str
    ) -> None:
        state = dict(agent_state_base)
        state["input_code"] = sample_prefect_code
        result = parse_job(state)
        tasks = result["parsed_structure"].get("prefect_tasks", [])
        assert len(tasks) > 0


class TestDiffGeneratorNode:
    """Tests for generate_diff node."""

    def test_generates_diff_for_different_code(
        self, agent_state_base: dict
    ) -> None:
        state = dict(agent_state_base)
        state["input_code"] = "x = 1\ny = 2\n"
        state["rewritten_code"] = "x = 1\ny = 3\nz = 4\n"
        result = generate_diff(state)
        assert result["diff"] != ""
        assert "+" in result["diff"] or "-" in result["diff"]

    def test_empty_diff_for_identical_code(
        self, agent_state_base: dict
    ) -> None:
        code = "x = 1\ny = 2\n"
        state = dict(agent_state_base)
        state["input_code"] = code
        state["rewritten_code"] = code
        result = generate_diff(state)
        assert result["diff"] == "" or "@@" not in result["diff"]

    def test_skips_diff_when_no_rewrite(
        self, agent_state_base: dict
    ) -> None:
        state = dict(agent_state_base)
        state["rewritten_code"] = ""
        result = generate_diff(state)
        assert result["diff"] == ""

    def test_diff_contains_filename_headers(
        self, agent_state_base: dict
    ) -> None:
        state = dict(agent_state_base)
        state["input_code"] = "a = 1\n"
        state["rewritten_code"] = "a = 2\n"
        result = generate_diff(state)
        assert "original.py" in result["diff"]
        assert "rewritten.py" in result["diff"]

    def test_state_preserved_after_diff(
        self, agent_state_base: dict
    ) -> None:
        state = dict(agent_state_base)
        state["input_code"] = "a = 1\n"
        state["rewritten_code"] = "a = 2\n"
        result = generate_diff(state)
        assert result["token_budget"] == agent_state_base["token_budget"]
        assert result["session_id"] == agent_state_base["session_id"]


class TestRewriterNodeMocked:
    """Tests for rewrite_code node with mocked LLM calls."""

    def test_rewriter_returns_code_on_success(
        self, agent_state_base: dict
    ) -> None:
        state = dict(agent_state_base)
        state["parsed_structure"] = {
            "transforms": ["filter", "groupBy"],
            "columns": ["id", "value"],
            "has_schema_def": False,
        }
        state["pii_report"] = {
            "pii_column_names": [],
            "entity_count": 0,
            "risk_level": "low",
        }
        state["rewrite_plan"] = "Rewrite for Snowflake"

        mock_message = MagicMock()
        mock_message.content = [MagicMock(text="rewritten_code = 'ok'")]
        mock_message.usage = MagicMock(
            input_tokens=100, output_tokens=50
        )

        with patch(
            "agent.nodes.rewriter.anthropic.Anthropic"
        ) as mock_anthropic:
            instance = mock_anthropic.return_value
            instance.messages.create.return_value = mock_message

            from agent.nodes.rewriter import rewrite_code
            result = rewrite_code(state)

        assert result["error"] is None
        assert result["rewritten_code"] != ""
        assert result["tokens_used"] == 150

    def test_rewriter_sets_error_on_api_failure(
        self, agent_state_base: dict
    ) -> None:
        import anthropic as _anthropic

        state = dict(agent_state_base)
        state["parsed_structure"] = {"transforms": [], "columns": []}
        state["pii_report"] = {"pii_column_names": []}
        state["rewrite_plan"] = ""

        with patch(
            "agent.nodes.rewriter.anthropic.Anthropic"
        ) as mock_anthropic:
            instance = mock_anthropic.return_value
            instance.messages.create.side_effect = (
                _anthropic.APITimeoutError(request=MagicMock())
            )

            from agent.nodes.rewriter import rewrite_code
            result = rewrite_code(state)

        assert result["error"] is not None
