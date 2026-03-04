"""SQLAlchemy ORM models for the ETL optimizer database."""

import datetime

from sqlalchemy import (
    CheckConstraint,
    Float,
    ForeignKey,
    Index,
    Integer,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Shared declarative base for all models."""


class Session(Base):
    """Represents a single ETL optimization run."""

    __tablename__ = "sessions"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    created_at: Mapped[str] = mapped_column(Text, nullable=False)
    input_hash: Mapped[str] = mapped_column(Text, nullable=False)
    input_type: Mapped[str] = mapped_column(Text, nullable=False)
    target: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        Text, nullable=False, default="pending"
    )
    tokens_used: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cost_usd: Mapped[float | None] = mapped_column(Float, nullable=True)
    prompt_version: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1
    )

    __table_args__ = (
        CheckConstraint(
            "input_type IN ('pyspark', 'prefect')",
            name="ck_session_input_type",
        ),
        CheckConstraint(
            "target IN ('snowflake', 'delta_lake')",
            name="ck_session_target",
        ),
        CheckConstraint(
            "status IN ('pending','running','success','failed')",
            name="ck_session_status",
        ),
        Index("idx_sessions_created_at", "created_at"),
    )


class Artifact(Base):
    """Stores generated artifacts for a session."""

    __tablename__ = "artifacts"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    session_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("sessions.id", ondelete="CASCADE"),
        nullable=False,
    )
    artifact_type: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (
        CheckConstraint(
            "artifact_type IN ('code','diff','compliance','validation','zip')",
            name="ck_artifact_type",
        ),
        Index("idx_artifacts_session", "session_id"),
    )


class AuditLog(Base):
    """Append-only audit log — no UPDATE/DELETE permitted at app level."""

    __tablename__ = "audit_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    session_id: Mapped[str] = mapped_column(Text, nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    event_data: Mapped[str | None] = mapped_column(Text, nullable=True)
    timestamp: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str] = mapped_column(
        Text, nullable=False, default="INFO"
    )

    __table_args__ = (
        CheckConstraint(
            "severity IN ('DEBUG','INFO','WARNING','ERROR','CRITICAL')",
            name="ck_audit_severity",
        ),
        Index("idx_audit_session", "session_id"),
    )


class PIIReport(Base):
    """PII scan results for a session."""

    __tablename__ = "pii_reports"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    session_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("sessions.id", ondelete="CASCADE"),
        nullable=False,
    )
    entities: Mapped[str] = mapped_column(Text, nullable=False)
    risk_level: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (
        CheckConstraint(
            "risk_level IN ('low','medium','high','critical')",
            name="ck_pii_risk_level",
        ),
        Index("idx_pii_session", "session_id"),
    )


class RefinementMessage(Base):
    """Multi-turn refinement conversation messages."""

    __tablename__ = "refinement_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    session_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("sessions.id", ondelete="CASCADE"),
        nullable=False,
    )
    role: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (
        CheckConstraint(
            "role IN ('user', 'assistant')",
            name="ck_msg_role",
        ),
    )


def now_iso() -> str:
    """Return current UTC time as ISO-8601 string."""
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
