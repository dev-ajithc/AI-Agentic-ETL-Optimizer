"""Reusable Streamlit UI components for the ETL Optimizer."""

import json

import streamlit as st


def render_header() -> None:
    """Render the application header and data sovereignty notice."""
    st.title("🔧 AI Agentic ETL Optimizer")
    st.caption(
        "Convert legacy PySpark / Prefect jobs into optimized, "
        "governed, cloud-native pipelines using Claude AI."
    )
    with st.expander("⚠️ Data Sovereignty Notice", expanded=False):
        st.info(
            "**Your code is processed locally.** Input code is sent "
            "to the Anthropic API only for the rewrite step. "
            "No data is stored on Anthropic servers beyond the "
            "API call lifetime. Do NOT submit code containing "
            "real credentials, secrets, or live PII data."
        )


def render_input_panel() -> tuple[str, str, str, list[str], int]:
    """
    Render the left-panel code input form.

    Returns:
        Tuple of (code, input_type, target,
                  compliance_profiles, token_budget).
    """
    st.subheader("📥 Input")

    input_type = st.selectbox(
        "Input Type",
        options=["pyspark", "prefect"],
        format_func=lambda x: (
            "PySpark Script" if x == "pyspark" else "Prefect Flow"
        ),
        key="input_type",
    )

    target = st.selectbox(
        "Target Platform",
        options=["snowflake", "delta_lake"],
        format_func=lambda x: (
            "❄️ Snowflake (Snowpark)"
            if x == "snowflake"
            else "🔷 Delta Lake (Databricks)"
        ),
        key="target",
    )

    compliance_options = ["gdpr", "hipaa", "sox"]
    compliance = st.multiselect(
        "Compliance Profiles",
        options=compliance_options,
        default=st.session_state.get(
            "compliance_profile", compliance_options
        ),
        format_func=str.upper,
        key="compliance_profile",
    )

    token_budget = st.slider(
        "Token Budget",
        min_value=2000,
        max_value=16000,
        value=st.session_state.get("token_budget", 8000),
        step=500,
        key="token_budget",
        help="Hard cap on Claude API tokens per run",
    )

    uploaded = st.file_uploader(
        "Upload .py file (or paste below)",
        type=["py"],
        key="uploaded_file",
    )

    if uploaded is not None:
        code = uploaded.read().decode("utf-8")
        st.session_state["input_code"] = code
    else:
        code = st.text_area(
            "Paste your PySpark / Prefect code",
            height=350,
            value=st.session_state.get("input_code", ""),
            key="input_code_area",
            placeholder="# Paste your ETL code here...",
        )
        st.session_state["input_code"] = code

    return code, input_type, target, compliance, token_budget


def render_status_bar(running: bool, step: str = "") -> None:
    """Render a status indicator during agent execution."""
    if running:
        st.info(f"⏳ Running: {step or 'processing...'}")


def render_error(error_msg: str) -> None:
    """Render a structured error message."""
    code = error_msg.split(":")[0] if ":" in error_msg else ""
    st.error(f"**{code}** — {error_msg}")


def render_warnings(warnings: list[str]) -> None:
    """Render a list of warnings."""
    if warnings:
        for w in warnings:
            st.warning(w)


def render_output_tabs(result: dict) -> None:
    """Render the output panel with tabbed views of all artifacts."""
    tab_code, tab_diff, tab_compliance, tab_validation, tab_pii = (
        st.tabs([
            "📄 Rewritten Code",
            "📊 Diff",
            "🛡️ Compliance",
            "✅ Validation",
            "🔍 PII Report",
        ])
    )

    with tab_code:
        rewritten = result.get("rewritten_code", "")
        if rewritten:
            st.code(rewritten, language="python")
            st.download_button(
                "⬇️ Download rewritten.py",
                data=rewritten,
                file_name="rewritten.py",
                mime="text/plain",
            )
        else:
            st.warning("No rewritten code available.")

    with tab_diff:
        diff = result.get("diff", "")
        if diff:
            st.code(diff, language="diff")
        else:
            st.info("No diff available.")

    with tab_compliance:
        _render_compliance_panel(result.get("compliance_report", {}))

    with tab_validation:
        _render_validation_panel(result.get("validation_result", {}))

    with tab_pii:
        _render_pii_panel(result.get("pii_report", {}))

    zip_bytes = result.get("_zip_bytes")
    if zip_bytes:
        st.divider()
        st.download_button(
            "📦 Download All Artifacts (ZIP)",
            data=zip_bytes,
            file_name=(
                f"etl_optimized_{result.get('session_id', 'run')[:8]}.zip"
            ),
            mime="application/zip",
            type="primary",
        )


def _render_compliance_panel(report: dict) -> None:
    """Render the compliance report tab contents."""
    if not report:
        st.info("No compliance report available.")
        return

    overall = report.get("overall_status", "unknown").upper()
    colour = {
        "PASS": "green",
        "WARN": "orange",
        "FAIL": "red",
    }.get(overall, "gray")
    icon = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}.get(overall, "❓")

    st.markdown(
        f"### Overall Status: :{colour}[{icon} {overall}]"
    )

    checks = report.get("checks", [])
    if checks:
        for chk in checks:
            status = chk.get("status", "")
            s_icon = {"pass": "✅", "warn": "⚠️", "fail": "❌"}.get(
                status, "❓"
            )
            with st.expander(
                f"{s_icon} [{chk.get('id')}] {chk.get('name')} "
                f"— {chk.get('profile', '').upper()}",
                expanded=(status == "fail"),
            ):
                st.write(f"**Detail:** {chk.get('detail', '')}")
                if chk.get("remediation"):
                    st.write(
                        f"**Remediation:** {chk.get('remediation')}"
                    )

    pii_summary = report.get("pii_summary", {})
    if pii_summary:
        st.divider()
        st.markdown("#### PII Summary")
        col1, col2, col3 = st.columns(3)
        col1.metric(
            "Entities Detected",
            pii_summary.get("entities_detected", 0),
        )
        col2.metric(
            "Risk Level",
            pii_summary.get("risk_level", "n/a").upper(),
        )
        col3.metric(
            "Masked in Output",
            "Yes" if pii_summary.get("masked_in_output") else "No",
        )

    st.divider()
    with st.expander("Raw JSON", expanded=False):
        st.json(report)


def _render_validation_panel(vr: dict) -> None:
    """Render the validation result tab contents."""
    if not vr:
        st.info("No validation result available.")
        return

    passed = vr.get("passed", False)
    st.markdown(
        f"### Validation: {'✅ PASSED' if passed else '❌ FAILED'}"
    )

    col1, col2, col3, col4 = st.columns(4)
    col1.metric(
        "Syntax Valid",
        "✅" if vr.get("syntax_valid") else "❌",
    )
    col2.metric(
        "Imports Valid",
        "✅" if vr.get("imports_valid") else "❌",
    )
    col3.metric(
        "Execution Time",
        f"{vr.get('execution_time_s', 0):.2f}s",
    )
    col4.metric(
        "Rows Processed",
        vr.get("synthetic_rows_processed", 0),
    )

    if vr.get("runtime_error"):
        st.error(f"**Runtime Error:** `{vr['runtime_error']}`")


def _render_pii_panel(pii: dict) -> None:
    """Render the PII report tab contents."""
    if not pii:
        st.info("No PII report available.")
        return

    risk = pii.get("risk_level", "low")
    risk_colour = {
        "low": "green",
        "medium": "orange",
        "high": "red",
        "critical": "red",
    }.get(risk, "gray")

    st.markdown(
        f"### PII Risk Level: :{risk_colour}[{risk.upper()}]"
    )

    entities = pii.get("entities", [])
    if entities:
        col_names = pii.get("pii_column_names", [])
        if col_names:
            st.write(
                "**PII Columns Detected:** "
                + ", ".join(f"`{c}`" for c in col_names)
            )
        st.metric("Total Entities Found", len(entities))
        with st.expander("Entity Details", expanded=False):
            st.json(entities)
    else:
        st.success("No PII entities detected.")


def render_refinement_panel(session_id: str) -> str | None:
    """
    Render the multi-turn refinement input panel.

    Returns the user's follow-up message if submitted, else None.
    """
    st.subheader("🔄 Refine")
    refinement_input = st.text_area(
        "Request a follow-up change",
        height=80,
        placeholder=(
            "e.g. 'Add more partitioning' or "
            "'Increase parallelism for the join step'"
        ),
        key="refinement_input",
    )
    if st.button("Submit Refinement", key="btn_refine"):
        if refinement_input.strip():
            return refinement_input.strip()
    return None


def render_session_history(history: list[str]) -> str | None:
    """
    Render the session history sidebar panel.

    Returns a selected session_id if user clicks one, else None.
    """
    if not history:
        st.caption("No previous sessions.")
        return None

    selected = st.selectbox(
        "Load previous session",
        options=history,
        format_func=lambda x: x[:8] + "...",
        key="history_select",
    )
    if st.button("Load Session", key="btn_load_history"):
        return selected
    return None


def render_token_usage(
    tokens_used: int, token_budget: int
) -> None:
    """Render a token usage progress bar."""
    pct = min(tokens_used / max(token_budget, 1), 1.0)
    st.progress(pct, text=f"Tokens: {tokens_used} / {token_budget}")
    if pct >= 0.9:
        st.warning("Approaching token budget limit.")
