# AI Agentic ETL Optimizer

> Convert legacy PySpark / Prefect jobs into optimized, governed, cloud-native
> pipelines using **Claude AI + LangGraph** — with zero manual refactoring and
> full compliance audit trails.

---

## Features

- **AI-powered rewrite** — Claude 3.5 Sonnet rewrites PySpark / Prefect code
  for Snowflake (Snowpark) or Delta Lake targets
- **6-stage input sanitization** — dangerous patterns blocked before reaching LLM
- **PII detection** — regex + spaCy NER + column name heuristics with risk scoring
- **Compliance engine** — GDPR, HIPAA, SOX rule checks with auto-fix injection
- **Local Spark validation** — sandboxed dry-run on synthetic data; no real data needed
- **Multi-turn refinement** — follow-up requests in the same session
- **Full audit trail** — append-only SQLite log of every event
- **Reproducible artifacts** — SHA-256 hashed outputs, `temperature=0` rewrites
- **Streamlit UI** — live progress streaming, tabbed output, ZIP download

---

## Quick Start

### Prerequisites

- Python 3.12+
- Java 17+ (for local PySpark validation)
- An [Anthropic API key](https://console.anthropic.com/)

### Local Setup

```bash
# 1. Clone the repo
git clone https://github.com/your-org/ai-agentic-etl-optimizer
cd ai-agentic-etl-optimizer

# 2. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt
python -m spacy download en_core_web_sm

# 4. Configure environment
cp .env.example .env
# Edit .env and set ANTHROPIC_API_KEY=sk-ant-...

# 5. Run the app
streamlit run app.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

### Docker

```bash
# Build and run with Docker Compose
ANTHROPIC_API_KEY=sk-ant-... docker compose up --build
```

---

## Usage

1. **Paste or upload** your PySpark or Prefect Python script
2. **Select target platform** — Snowflake (Snowpark) or Delta Lake
3. **Select compliance profiles** — GDPR, HIPAA, SOX (multi-select)
4. **Set token budget** — controls API spend per run (default: 8 000 tokens)
5. **Click Optimize** — watch the agent pipeline execute step by step
6. **Review outputs** — rewritten code, diff, compliance report, PII report,
   validation result
7. **Download ZIP** — all artifacts bundled for hand-off or PR review
8. **Refine** — request follow-up changes without starting a new session

---

## Architecture

```
Input Code → [InputSanitizer] → [LangGraph Agent Graph]
                                    ├── parse_job
                                    ├── scan_pii
                                    ├── rewrite_code (Claude)
                                    ├── validate_spark (sandboxed)
                                    ├── check_compliance
                                    ├── generate_diff
                                    └── package_artifacts
                                → Streamlit Output + SQLite Audit Log
```

See [`design_specification.md`](design_specification.md) for the complete
architecture, data models, prompt spec, error taxonomy, and decision log.

---

## Project Structure

```
├── app.py                    Streamlit entry point
├── config.py                 Pydantic Settings (all env vars)
├── logging_config.py         structlog JSON configuration
├── agent/
│   ├── graph.py              LangGraph state graph
│   ├── state.py              AgentState TypedDict
│   └── nodes/                7 pipeline nodes
├── compliance/
│   ├── rules.py              10 compliance rules (GDPR/HIPAA/SOX/ETL)
│   ├── pii_patterns.py       Regex + column heuristics
│   └── profiles/             Profile JSON definitions
├── db/
│   ├── models.py             SQLAlchemy ORM models
│   ├── repository.py         DB access layer
│   └── migrations/           SQL schema
├── validation/
│   ├── input_sanitizer.py    6-stage input gate
│   └── spark_sandbox.py      Sandboxed subprocess executor
├── ui/
│   ├── components.py         Reusable Streamlit components
│   └── session_state.py      Session state management
├── prompts/
│   └── system_v1.txt         Versioned system prompt
└── tests/                    pytest test suite
```

---

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ANTHROPIC_API_KEY` | **Yes** | — | Anthropic API key |
| `MAX_TOKENS_PER_RUN` | No | `8000` | Hard token cap per run |
| `SPARK_VALIDATION_TIMEOUT_S` | No | `60` | Sandbox subprocess timeout |
| `SQLITE_PATH` | No | `data/optimizer.db` | SQLite database path |
| `LOG_LEVEL` | No | `INFO` | Logging level |
| `PII_SCAN_ENABLED` | No | `true` | Enable PII scanner |
| `COMPLIANCE_PROFILES` | No | `gdpr,hipaa,sox` | Active compliance profiles |
| `PROMPT_VERSION` | No | `1` | System prompt version |
| `ARTIFACT_RETENTION_DAYS` | No | `90` | Days to retain artifacts |
| `DAILY_TOKEN_LIMIT` | No | `500000` | Daily cumulative token cap |

See [`.env.example`](.env.example) for the full list.

---

## Running Tests

```bash
# Run full test suite
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=. --cov-report=term-missing

# Run a specific test module
pytest tests/test_input_sanitizer.py -v

# Run security scan
bandit -r . -x tests/

# Run dependency audit
pip-audit -r requirements.txt
```

---

## Security

- **Input sanitization**: 6-stage gate blocks dangerous patterns, unknown imports,
  path traversal, and oversized inputs **before** any LLM call
- **Sandboxed execution**: Spark validation runs in an isolated subprocess with
  RAM (`2 GB`) and CPU (`60 s`) limits; network access denied
- **Secret management**: All secrets via environment variables; app refuses to
  start without `ANTHROPIC_API_KEY`
- **PII masking stubs**: Detected PII columns get `mask_pii()` stubs injected —
  raises `NotImplementedError` to force production teams to implement properly
- **Append-only audit log**: Every action logged to SQLite; no UPDATE/DELETE
  permitted at the application layer
- **SHA-256 hashing**: All artifact content hashed; MD5 never used
- **Non-root Docker**: Container runs as `appuser` (UID 1001)

---

## Development

```bash
# Install pre-commit hooks
pre-commit install

# Lint
ruff check .

# Format
ruff format .

# Type check
mypy config.py db/ agent/ compliance/ validation/
```

---

## License

MIT — see [LICENSE](LICENSE).
