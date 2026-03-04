"""Application configuration via Pydantic Settings."""

from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed, validated application settings loaded from environment."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="forbid",
    )

    anthropic_api_key: str = Field(
        ...,
        description="Anthropic API key — required",
    )
    max_tokens_per_run: int = Field(
        default=8000,
        ge=1000,
        le=100000,
        description="Hard token cap per optimization run",
    )
    spark_validation_timeout_s: int = Field(
        default=60,
        ge=10,
        le=300,
        description="Max seconds for local Spark dry-run subprocess",
    )
    sqlite_path: str = Field(
        default="data/optimizer.db",
        description="Path to SQLite database file",
    )
    log_level: str = Field(
        default="INFO",
        description="Logging level: DEBUG|INFO|WARNING|ERROR|CRITICAL",
    )
    pii_scan_enabled: bool = Field(
        default=True,
        description="Enable spaCy + regex PII scanning",
    )
    compliance_profiles: list[str] = Field(
        default=["gdpr", "hipaa", "sox"],
        description="Active compliance profiles",
    )
    metrics_enabled: bool = Field(
        default=False,
        description="Expose Prometheus /metrics endpoint",
    )
    enable_healthcheck: bool = Field(
        default=False,
        description="Enable FastAPI health check sidecar",
    )
    prompt_version: int = Field(
        default=1,
        ge=1,
        description="System prompt version to load from prompts/",
    )
    artifact_retention_days: int = Field(
        default=90,
        ge=1,
        le=3650,
        description="Days to retain session artifacts in SQLite",
    )
    daily_token_limit: int = Field(
        default=500_000,
        ge=10_000,
        description="Cumulative daily token spend cap",
    )
    max_input_tokens: int = Field(
        default=10_000,
        ge=100,
        description="Max tokens accepted in a single user submission",
    )
    llm_primary_model: str = Field(
        default="claude-3-5-sonnet-20241022",
        description="Primary Claude model for rewrites",
    )
    llm_fallback_model: str = Field(
        default="claude-3-haiku-20240307",
        description="Fallback model on timeout/error",
    )
    llm_escalation_model: str = Field(
        default="claude-3-opus-20240229",
        description="Escalation model after 3 validation failures",
    )
    llm_temperature: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="LLM temperature — 0 for reproducibility",
    )
    max_rewrite_retries: int = Field(
        default=3,
        ge=1,
        le=5,
        description="Max rewrite attempts before escalation",
    )
    session_history_limit: int = Field(
        default=20,
        ge=1,
        description="Max sessions stored in SQLite history",
    )

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Ensure log level is one of the standard Python levels."""
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper = v.upper()
        if upper not in allowed:
            raise ValueError(
                f"log_level must be one of {allowed}, got {v!r}"
            )
        return upper

    @field_validator("compliance_profiles", mode="before")
    @classmethod
    def parse_profiles(cls, v: object) -> list[str]:
        """Accept comma-separated string or list."""
        if isinstance(v, str):
            return [p.strip().lower() for p in v.split(",") if p.strip()]
        return [str(p).lower() for p in v]  # type: ignore[union-attr]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached Settings instance."""
    return Settings()
