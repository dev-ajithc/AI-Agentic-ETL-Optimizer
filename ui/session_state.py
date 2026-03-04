"""Streamlit session_state management — centralised key definitions."""

import uuid

import streamlit as st

SESSION_DEFAULTS: dict[str, object] = {
    "session_id": None,
    "input_code": "",
    "input_type": "pyspark",
    "target": "snowflake",
    "compliance_profile": ["gdpr", "hipaa", "sox"],
    "last_result": None,
    "history": [],
    "token_budget": 8000,
    "refinement_messages": [],
    "running": False,
    "error": None,
    "warnings": [],
}


def init_session_state() -> None:
    """Initialise all expected session_state keys with defaults."""
    for key, default in SESSION_DEFAULTS.items():
        if key not in st.session_state:
            if key == "session_id":
                st.session_state[key] = str(uuid.uuid4())
            elif isinstance(default, list):
                st.session_state[key] = list(default)
            else:
                st.session_state[key] = default


def new_session() -> str:
    """Reset session state for a new optimization run."""
    new_id = str(uuid.uuid4())
    st.session_state["session_id"] = new_id
    st.session_state["last_result"] = None
    st.session_state["error"] = None
    st.session_state["warnings"] = []
    st.session_state["refinement_messages"] = []
    st.session_state["running"] = False
    return new_id


def get_session_id() -> str:
    """Return current session ID, creating one if absent."""
    if not st.session_state.get("session_id"):
        st.session_state["session_id"] = str(uuid.uuid4())
    return st.session_state["session_id"]


def push_history(session_id: str) -> None:
    """Append session_id to in-memory history list (max 20)."""
    history: list[str] = st.session_state.get("history", [])
    if session_id not in history:
        history.insert(0, session_id)
        st.session_state["history"] = history[:20]
