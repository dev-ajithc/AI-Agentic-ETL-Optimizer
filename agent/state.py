"""LangGraph agent state definition."""

from typing import Annotated, Literal

from langgraph.graph.message import add_messages
from typing_extensions import TypedDict


class AgentState(TypedDict):
    """Full mutable state threaded through every graph node."""

    session_id: str
    input_code: str
    input_type: Literal["pyspark", "prefect"]
    target: Literal["snowflake", "delta_lake"]
    compliance_profile: str
    token_budget: int
    tokens_used: int
    parsed_structure: dict
    pii_report: dict
    rewrite_plan: str
    rewritten_code: str
    validation_result: dict
    compliance_report: dict
    diff: str
    messages: Annotated[list, add_messages]
    retry_count: int
    error: str | None
    warnings: list[str]
