"""Diff generator node — produces unified diff of original vs rewritten."""

import difflib

import structlog

from agent.state import AgentState

logger = structlog.get_logger(__name__)


def generate_diff(state: AgentState) -> AgentState:
    """Generate a unified diff between input_code and rewritten_code."""
    session_id = state["session_id"]
    original = state.get("input_code", "")
    rewritten = state.get("rewritten_code", "")

    if not rewritten:
        logger.warning(
            "diff_skipped_no_rewrite", session_id=session_id
        )
        return {**state, "diff": ""}

    original_lines = original.splitlines(keepends=True)
    rewritten_lines = rewritten.splitlines(keepends=True)

    diff_lines = list(
        difflib.unified_diff(
            original_lines,
            rewritten_lines,
            fromfile="original.py",
            tofile="rewritten.py",
            lineterm="",
        )
    )

    diff_str = "\n".join(diff_lines)
    added = sum(1 for l in diff_lines if l.startswith("+") and not l.startswith("+++"))
    removed = sum(1 for l in diff_lines if l.startswith("-") and not l.startswith("---"))

    logger.info(
        "diff_generated",
        session_id=session_id,
        lines_added=added,
        lines_removed=removed,
    )

    return {**state, "diff": diff_str}
