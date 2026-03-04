"""LLM rewrite node — calls Claude to rewrite ETL code."""

import re
import time
from pathlib import Path

import anthropic
import structlog

from agent.state import AgentState
from config import get_settings

logger = structlog.get_logger(__name__)

_PROMPT_DIR = Path(__file__).parent.parent.parent / "prompts"


def _load_system_prompt(version: int) -> str:
    """Load versioned system prompt from prompts/system_v{N}.txt."""
    prompt_file = _PROMPT_DIR / f"system_v{version}.txt"
    if not prompt_file.exists():
        raise FileNotFoundError(
            f"System prompt not found: {prompt_file}"
        )
    return prompt_file.read_text(encoding="utf-8")


def _build_user_message(state: AgentState) -> str:
    """Assemble the user-turn message with full context."""
    parsed = state.get("parsed_structure", {})
    pii = state.get("pii_report", {})
    plan = state.get("rewrite_plan", "")

    pii_columns = pii.get("pii_column_names", [])
    transforms = parsed.get("transforms", [])
    has_schema = parsed.get("has_schema_def", False)

    parts = [
        f"## Rewrite Task",
        f"",
        f"**Input type:** {state['input_type']}",
        f"**Target platform:** {state['target']}",
        f"**Compliance profiles:** {state['compliance_profile']}",
        f"",
        f"## Parsed Structure",
        f"- Detected transforms: {', '.join(transforms) or 'none'}",
        f"- Has schema definition: {has_schema}",
        f"- Columns found: "
        + (", ".join(parsed.get("columns", [])) or "none"),
        f"",
    ]

    if pii_columns:
        parts += [
            f"## PII Columns Detected",
            f"The following columns contain PII and MUST be masked:",
            ", ".join(f"`{c}`" for c in pii_columns),
            f"",
        ]

    if plan:
        parts += [
            f"## Rewrite Plan",
            plan,
            f"",
        ]

    parts += [
        f"## Original Code",
        f"```python",
        state["input_code"],
        f"```",
        f"",
        f"Rewrite the code above for the target platform. "
        f"Output ONLY the rewritten Python code block.",
    ]

    return "\n".join(parts)


def _extract_code_block(response_text: str) -> str:
    """Extract Python code from a fenced code block if present."""
    match = re.search(
        r"```(?:python)?\s*\n(.*?)```",
        response_text,
        re.DOTALL,
    )
    if match:
        return match.group(1).strip()
    return response_text.strip()


def _call_claude(
    client: anthropic.Anthropic,
    model: str,
    system_prompt: str,
    user_message: str,
    max_tokens: int,
) -> tuple[str, int, int]:
    """
    Call Claude and return (response_text, input_tokens, output_tokens).
    Raises anthropic.APITimeoutError or anthropic.APIError on failure.
    """
    message = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        temperature=0.0,
        system=system_prompt,
        messages=[{"role": "user", "content": user_message}],
    )
    text = message.content[0].text if message.content else ""
    in_tok = message.usage.input_tokens
    out_tok = message.usage.output_tokens
    return text, in_tok, out_tok


def rewrite_code(state: AgentState) -> AgentState:
    """Call Claude to rewrite the ETL job for the target platform."""
    settings = get_settings()
    session_id = state["session_id"]
    retry_count = state.get("retry_count", 0)

    model = settings.llm_primary_model
    if retry_count >= settings.max_rewrite_retries:
        model = settings.llm_escalation_model
        logger.warning(
            "rewriter_escalating",
            session_id=session_id,
            model=model,
            retry_count=retry_count,
        )

    logger.info(
        "rewrite_start",
        session_id=session_id,
        model=model,
        retry=retry_count,
    )

    try:
        system_prompt = _load_system_prompt(settings.prompt_version)
    except FileNotFoundError as exc:
        return {**state, "error": f"E004: {exc}"}

    remaining_budget = (
        state["token_budget"] - state.get("tokens_used", 0)
    )
    system_prompt = system_prompt.format(
        target=state["target"],
        compliance_profile=state["compliance_profile"],
        token_budget_remaining=remaining_budget,
    )

    user_message = _build_user_message(state)
    client = anthropic.Anthropic(
        api_key=settings.anthropic_api_key,
        timeout=30.0,
    )

    backoff = 2.0
    last_error: Exception | None = None

    for attempt in range(3):
        try:
            t0 = time.monotonic()
            raw_text, in_tok, out_tok = _call_claude(
                client,
                model,
                system_prompt,
                user_message,
                max_tokens=2000,
            )
            latency_ms = int((time.monotonic() - t0) * 1000)
            logger.info(
                "llm_call_success",
                session_id=session_id,
                model=model,
                tokens_in=in_tok,
                tokens_out=out_tok,
                latency_ms=latency_ms,
            )
            rewritten = _extract_code_block(raw_text)
            tokens_used = (
                state.get("tokens_used", 0) + in_tok + out_tok
            )
            return {
                **state,
                "rewritten_code": rewritten,
                "tokens_used": tokens_used,
                "error": None,
            }

        except anthropic.APITimeoutError as exc:
            last_error = exc
            logger.warning(
                "llm_timeout",
                session_id=session_id,
                attempt=attempt + 1,
            )
            if attempt < 2:
                time.sleep(backoff)
                backoff *= 2

        except anthropic.APIStatusError as exc:
            last_error = exc
            logger.error(
                "llm_api_error",
                session_id=session_id,
                status_code=exc.status_code,
                message=str(exc),
            )
            break

    if retry_count < settings.max_rewrite_retries:
        return {
            **state,
            "error": f"E003: LLM timeout after 3 attempts: {last_error}",
        }

    logger.error(
        "rewriter_fallback_haiku",
        session_id=session_id,
    )
    try:
        raw_text, in_tok, out_tok = _call_claude(
            client,
            settings.llm_fallback_model,
            system_prompt,
            user_message,
            max_tokens=1500,
        )
        rewritten = _extract_code_block(raw_text)
        tokens_used = (
            state.get("tokens_used", 0) + in_tok + out_tok
        )
        warnings = list(state.get("warnings", []))
        warnings.append(
            "Rewrite performed by fallback model "
            f"({settings.llm_fallback_model}) — review carefully."
        )
        return {
            **state,
            "rewritten_code": rewritten,
            "tokens_used": tokens_used,
            "warnings": warnings,
            "error": None,
        }
    except Exception as exc:
        return {
            **state,
            "error": f"E004: All LLM attempts failed: {exc}",
        }
