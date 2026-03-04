"""AI Agentic ETL Optimizer — Streamlit entry point."""

import asyncio
import hashlib
import os
from pathlib import Path

import streamlit as st

from agent.graph import build_graph
from agent.state import AgentState
from config import get_settings
from db.repository import (
    ArtifactRepository,
    AuditRepository,
    PIIRepository,
    SessionRepository,
    init_db,
)
from logging_config import configure_logging
from ui.components import (
    render_error,
    render_header,
    render_input_panel,
    render_output_tabs,
    render_refinement_panel,
    render_session_history,
    render_token_usage,
    render_warnings,
)
from ui.session_state import (
    get_session_id,
    init_session_state,
    new_session,
    push_history,
)
from validation.input_sanitizer import sanitize_input

st.set_page_config(
    page_title="AI ETL Optimizer",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def _startup() -> tuple:
    """
    One-time startup: configure logging, init DB, compile graph.
    Cached so it only runs once per Streamlit process.
    """
    settings = get_settings()
    configure_logging(settings.log_level)
    init_db(settings.sqlite_path)

    Path("logs").mkdir(exist_ok=True)
    Path("exports").mkdir(exist_ok=True)

    graph = build_graph()
    return settings, graph


def _sha256(text: str) -> str:
    """Return SHA-256 hex digest of text."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _run_optimization(
    code: str,
    input_type: str,
    target: str,
    compliance_profiles: list[str],
    token_budget: int,
    session_id: str,
    settings,
    graph,
) -> dict:
    """
    Execute the full LangGraph optimization pipeline synchronously.
    Returns the final AgentState as a dict.
    """
    compliance_str = ",".join(compliance_profiles)

    initial_state: AgentState = {
        "session_id": session_id,
        "input_code": code,
        "input_type": input_type,
        "target": target,
        "compliance_profile": compliance_str,
        "token_budget": token_budget,
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

    try:
        result = asyncio.run(graph.ainvoke(initial_state))
    except Exception as exc:
        result = dict(initial_state)
        result["error"] = f"E004: Agent graph error: {exc}"

    return result


def _persist_results(
    session_id: str,
    result: dict,
    input_hash: str,
    input_type: str,
    target: str,
    settings,
) -> None:
    """Persist session, artifacts, PII report, and audit logs to DB."""
    sqlite_path = settings.sqlite_path
    session_repo = SessionRepository(sqlite_path)
    artifact_repo = ArtifactRepository(sqlite_path)
    audit_repo = AuditRepository(sqlite_path)
    pii_repo = PIIRepository(sqlite_path)

    status = "failed" if result.get("error") else "success"
    session_repo.update_status(
        session_id,
        status=status,
        tokens_used=result.get("tokens_used"),
        cost_usd=None,
    )

    if result.get("rewritten_code"):
        artifact_repo.save(
            session_id, "code", result["rewritten_code"]
        )
    if result.get("diff"):
        artifact_repo.save(session_id, "diff", result["diff"])
    if result.get("compliance_report"):
        import json
        artifact_repo.save(
            session_id,
            "compliance",
            json.dumps(result["compliance_report"]),
        )
    if result.get("validation_result"):
        import json
        artifact_repo.save(
            session_id,
            "validation",
            json.dumps(result["validation_result"]),
        )

    pii = result.get("pii_report", {})
    if pii.get("entities"):
        pii_repo.save(
            session_id,
            entities=pii.get("entities", []),
            risk_level=pii.get("risk_level", "low"),
        )

    audit_repo.log(
        session_id,
        event_type="optimization_complete",
        event_data={
            "status": status,
            "tokens_used": result.get("tokens_used"),
            "target": target,
            "input_type": input_type,
            "pii_risk": pii.get("risk_level", "low"),
        },
        severity="INFO" if status == "success" else "ERROR",
    )


def main() -> None:
    """Main Streamlit application entry point."""
    settings, graph = _startup()
    init_session_state()

    render_header()

    with st.sidebar:
        st.header("⚙️ Session")
        session_id = get_session_id()
        st.caption(f"Session: `{session_id[:8]}...`")

        if st.button("🆕 New Session", use_container_width=True):
            session_id = new_session()
            st.rerun()

        st.divider()
        st.subheader("📋 History")
        history = st.session_state.get("history", [])
        selected = render_session_history(history)
        if selected:
            st.session_state["session_id"] = selected
            st.info(f"Loaded session: `{selected[:8]}...`")

        st.divider()
        with st.expander("ℹ️ About", expanded=False):
            st.markdown(
                "**AI Agentic ETL Optimizer v1.0**\n\n"
                "Powered by Claude + LangGraph.\n\n"
                "- LLM: Claude 3.5 Sonnet\n"
                "- Compliance: GDPR, HIPAA, SOX\n"
                "- Targets: Snowflake, Delta Lake"
            )

    left_col, right_col = st.columns([1, 1], gap="large")

    with left_col:
        (
            code,
            input_type,
            target,
            compliance_profiles,
            token_budget,
        ) = render_input_panel()

        run_btn = st.button(
            "🚀 Optimize",
            type="primary",
            use_container_width=True,
            disabled=st.session_state.get("running", False),
        )

    with right_col:
        last_result = st.session_state.get("last_result")
        error = st.session_state.get("error")
        warnings = st.session_state.get("warnings", [])

        if error:
            render_error(error)

        render_warnings(warnings)

        if last_result:
            tokens_used = last_result.get("tokens_used", 0)
            render_token_usage(tokens_used, token_budget)
            render_output_tabs(last_result)

            refinement_msg = render_refinement_panel(session_id)
            if refinement_msg:
                _handle_refinement(
                    refinement_msg,
                    last_result,
                    session_id,
                    settings,
                    graph,
                    token_budget,
                )

    if run_btn:
        if not code or not code.strip():
            st.warning("Please paste or upload code before optimizing.")
            return

        with st.spinner("Validating input..."):
            san = sanitize_input(
                code,
                max_tokens=settings.max_input_tokens,
                session_id=session_id,
            )

        if not san.passed:
            st.session_state["error"] = san.error_message
            st.session_state["running"] = False
            st.rerun()
            return

        input_hash = _sha256(san.sanitized_code)
        session_repo = SessionRepository(settings.sqlite_path)
        audit_repo = AuditRepository(settings.sqlite_path)

        db_session_id = session_repo.create(
            input_hash=input_hash,
            input_type=input_type,
            target=target,
            prompt_version=settings.prompt_version,
        )
        session_repo.update_status(db_session_id, "running")
        audit_repo.log(
            db_session_id,
            event_type="input_received",
            event_data={
                "input_type": input_type,
                "target": target,
                "compliance": compliance_profiles,
                "code_length": len(san.sanitized_code),
            },
        )

        st.session_state["session_id"] = db_session_id
        st.session_state["running"] = True
        st.session_state["error"] = None
        st.session_state["warnings"] = []

        node_status = st.empty()
        progress_bar = st.progress(0)

        steps = [
            "parse_job",
            "scan_pii",
            "rewrite_code",
            "validate_spark",
            "check_compliance",
            "generate_diff",
            "package_artifacts",
        ]

        async def run_with_streaming():
            results = {}
            total = len(steps)
            for i, event in enumerate(
                graph.stream(
                    {
                        "session_id": db_session_id,
                        "input_code": san.sanitized_code,
                        "input_type": input_type,
                        "target": target,
                        "compliance_profile": ",".join(
                            compliance_profiles
                        ),
                        "token_budget": token_budget,
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
                )
            ):
                node_name = list(event.keys())[0]
                node_data = event[node_name]
                step_idx = steps.index(node_name) if node_name in steps else i
                node_status.info(f"⏳ Step: **{node_name}**")
                progress_bar.progress(
                    min((step_idx + 1) / total, 1.0)
                )
                results.update(node_data)
            return results

        with st.spinner("Optimizing your ETL pipeline..."):
            try:
                final_state = asyncio.run(run_with_streaming())
            except Exception as exc:
                final_state = {
                    "error": f"E004: {exc}",
                    "session_id": db_session_id,
                }

        node_status.empty()
        progress_bar.empty()

        _persist_results(
            db_session_id,
            final_state,
            input_hash,
            input_type,
            target,
            settings,
        )

        push_history(db_session_id)
        st.session_state["last_result"] = final_state
        st.session_state["running"] = False

        if final_state.get("error"):
            st.session_state["error"] = final_state["error"]
        else:
            st.session_state["error"] = None

        st.session_state["warnings"] = final_state.get("warnings", [])
        st.rerun()


def _handle_refinement(
    message: str,
    last_result: dict,
    session_id: str,
    settings,
    graph,
    token_budget: int,
) -> None:
    """Handle a multi-turn refinement request."""
    from db.repository import RefinementRepository

    refine_repo = RefinementRepository(settings.sqlite_path)
    refine_repo.add_message(session_id, "user", message)

    history = refine_repo.get_window(session_id, window=6)

    existing_code = last_result.get("rewritten_code", "")
    refinement_code = (
        existing_code
        + f"\n# REFINEMENT REQUEST: {message}\n"
    )

    with st.spinner("Applying refinement..."):
        refinement_state = _run_optimization(
            code=refinement_code,
            input_type=last_result.get("input_type", "pyspark"),
            target=last_result.get("target", "snowflake"),
            compliance_profiles=last_result.get(
                "compliance_profile", "gdpr"
            ).split(","),
            token_budget=token_budget,
            session_id=session_id,
            settings=settings,
            graph=graph,
        )

    if refinement_state.get("rewritten_code"):
        refine_repo.add_message(
            session_id,
            "assistant",
            refinement_state["rewritten_code"][:500],
        )
        st.session_state["last_result"] = refinement_state
        st.session_state["warnings"] = refinement_state.get(
            "warnings", []
        )
        st.rerun()
    else:
        st.error(
            f"Refinement failed: {refinement_state.get('error')}"
        )


if __name__ == "__main__":
    main()
