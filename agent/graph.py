"""LangGraph agent graph definition for ETL optimization."""

from langgraph.graph import END, START, StateGraph

from agent.nodes.artifact_packager import package_artifacts
from agent.nodes.code_parser import parse_job
from agent.nodes.compliance_engine import check_compliance
from agent.nodes.diff_generator import generate_diff
from agent.nodes.pii_scanner import scan_pii
from agent.nodes.rewriter import rewrite_code
from agent.nodes.validator import validate_spark
from agent.state import AgentState
from config import get_settings


def _should_retry(state: AgentState) -> str:
    """
    Route after validation:
    - 'rewrite' if validation failed and retries remain
    - 'compliance' if validation passed
    - 'end_error' if retries exhausted
    """
    settings = get_settings()
    if state.get("error"):
        return "end_error"
    vr = state.get("validation_result", {})
    if vr.get("passed", False):
        return "compliance"
    retry = state.get("retry_count", 0)
    if retry < settings.max_rewrite_retries + 1:
        return "rewrite"
    return "end_error"


def _check_error(state: AgentState) -> str:
    """Route to 'end_error' if an error exists, otherwise continue."""
    if state.get("error"):
        return "end_error"
    return "continue"


def build_graph() -> StateGraph:
    """
    Build and compile the LangGraph optimization pipeline.

    Graph flow:
    parse_job → pii_scanner → rewrite_code → validate_spark
      → (retry rewrite_code | compliance)
    compliance → diff_generator → artifact_packager → END
    """
    graph = StateGraph(AgentState)

    graph.add_node("parse_job", parse_job)
    graph.add_node("scan_pii", scan_pii)
    graph.add_node("rewrite_code", rewrite_code)
    graph.add_node("validate_spark", validate_spark)
    graph.add_node("check_compliance", check_compliance)
    graph.add_node("generate_diff", generate_diff)
    graph.add_node("package_artifacts", package_artifacts)

    graph.add_edge(START, "parse_job")

    graph.add_conditional_edges(
        "parse_job",
        _check_error,
        {"continue": "scan_pii", "end_error": END},
    )

    graph.add_edge("scan_pii", "rewrite_code")

    graph.add_conditional_edges(
        "rewrite_code",
        _check_error,
        {"continue": "validate_spark", "end_error": END},
    )

    graph.add_conditional_edges(
        "validate_spark",
        _should_retry,
        {
            "rewrite": "rewrite_code",
            "compliance": "check_compliance",
            "end_error": END,
        },
    )

    graph.add_edge("check_compliance", "generate_diff")
    graph.add_edge("generate_diff", "package_artifacts")
    graph.add_edge("package_artifacts", END)

    return graph.compile()
