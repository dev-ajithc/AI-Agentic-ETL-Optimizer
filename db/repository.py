"""Database access layer — sessions, artifacts, audit log, PII reports."""

import hashlib
import json
import os
import uuid
from pathlib import Path

import structlog
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session as DBSession

from db.models import (
    Artifact,
    AuditLog,
    Base,
    PIIReport,
    RefinementMessage,
    Session,
    now_iso,
)

logger = structlog.get_logger(__name__)


def _engine(sqlite_path: str):
    """Create SQLAlchemy engine with WAL mode enabled."""
    db_path = Path(sqlite_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False},
        echo=False,
    )
    with engine.connect() as conn:
        conn.execute(text("PRAGMA journal_mode=WAL"))
        conn.execute(text("PRAGMA foreign_keys=ON"))
        conn.commit()
    return engine


_ENGINE_CACHE: dict[str, object] = {}


def get_engine(sqlite_path: str):
    """Return cached engine for the given path."""
    if sqlite_path not in _ENGINE_CACHE:
        _ENGINE_CACHE[sqlite_path] = _engine(sqlite_path)
    return _ENGINE_CACHE[sqlite_path]


def init_db(sqlite_path: str) -> None:
    """Create all tables if they do not exist."""
    engine = get_engine(sqlite_path)
    Base.metadata.create_all(engine)
    logger.info("db_initialized", path=sqlite_path)


def _sha256(content: str) -> str:
    """Return hex SHA-256 digest of content."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


class SessionRepository:
    """CRUD operations for optimization sessions."""

    def __init__(self, sqlite_path: str) -> None:
        self.engine = get_engine(sqlite_path)

    def create(
        self,
        input_hash: str,
        input_type: str,
        target: str,
        prompt_version: int,
    ) -> str:
        """Insert a new session row and return its UUID."""
        session_id = str(uuid.uuid4())
        with DBSession(self.engine) as db:
            db.add(
                Session(
                    id=session_id,
                    created_at=now_iso(),
                    input_hash=input_hash,
                    input_type=input_type,
                    target=target,
                    status="pending",
                    prompt_version=prompt_version,
                )
            )
            db.commit()
        logger.info(
            "session_created",
            session_id=session_id,
            target=target,
        )
        return session_id

    def update_status(
        self,
        session_id: str,
        status: str,
        tokens_used: int | None = None,
        cost_usd: float | None = None,
    ) -> None:
        """Update session status and optionally token/cost fields."""
        with DBSession(self.engine) as db:
            row = db.get(Session, session_id)
            if row is None:
                logger.warning(
                    "session_not_found", session_id=session_id
                )
                return
            row.status = status
            if tokens_used is not None:
                row.tokens_used = tokens_used
            if cost_usd is not None:
                row.cost_usd = cost_usd
            db.commit()

    def get(self, session_id: str) -> Session | None:
        """Fetch a session by ID."""
        with DBSession(self.engine) as db:
            return db.get(Session, session_id)

    def list_recent(self, limit: int = 20) -> list[Session]:
        """Return most recent sessions ordered by created_at DESC."""
        with DBSession(self.engine) as db:
            return (
                db.query(Session)
                .order_by(Session.created_at.desc())
                .limit(limit)
                .all()
            )


class ArtifactRepository:
    """Store and retrieve generated artifacts."""

    def __init__(self, sqlite_path: str) -> None:
        self.engine = get_engine(sqlite_path)

    def save(
        self,
        session_id: str,
        artifact_type: str,
        content: str,
    ) -> str:
        """Persist an artifact and return its UUID."""
        artifact_id = str(uuid.uuid4())
        with DBSession(self.engine) as db:
            db.add(
                Artifact(
                    id=artifact_id,
                    session_id=session_id,
                    artifact_type=artifact_type,
                    content=content,
                    content_hash=_sha256(content),
                    created_at=now_iso(),
                )
            )
            db.commit()
        return artifact_id

    def get_by_session(
        self, session_id: str, artifact_type: str | None = None
    ) -> list[Artifact]:
        """Retrieve artifacts for a session, optionally filtered by type."""
        with DBSession(self.engine) as db:
            q = db.query(Artifact).filter(
                Artifact.session_id == session_id
            )
            if artifact_type:
                q = q.filter(Artifact.artifact_type == artifact_type)
            return q.all()


class AuditRepository:
    """Append-only audit log writer."""

    def __init__(self, sqlite_path: str) -> None:
        self.engine = get_engine(sqlite_path)

    def log(
        self,
        session_id: str,
        event_type: str,
        event_data: dict | None = None,
        severity: str = "INFO",
    ) -> None:
        """Append an audit log entry."""
        with DBSession(self.engine) as db:
            db.add(
                AuditLog(
                    session_id=session_id,
                    event_type=event_type,
                    event_data=(
                        json.dumps(event_data) if event_data else None
                    ),
                    timestamp=now_iso(),
                    severity=severity,
                )
            )
            db.commit()

    def get_by_session(self, session_id: str) -> list[AuditLog]:
        """Return all audit entries for a session."""
        with DBSession(self.engine) as db:
            return (
                db.query(AuditLog)
                .filter(AuditLog.session_id == session_id)
                .order_by(AuditLog.id.asc())
                .all()
            )


class PIIRepository:
    """Store PII scan reports."""

    def __init__(self, sqlite_path: str) -> None:
        self.engine = get_engine(sqlite_path)

    def save(
        self,
        session_id: str,
        entities: list[dict],
        risk_level: str,
    ) -> str:
        """Persist PII report and return its UUID."""
        report_id = str(uuid.uuid4())
        with DBSession(self.engine) as db:
            db.add(
                PIIReport(
                    id=report_id,
                    session_id=session_id,
                    entities=json.dumps(entities),
                    risk_level=risk_level,
                    created_at=now_iso(),
                )
            )
            db.commit()
        return report_id

    def get_by_session(self, session_id: str) -> PIIReport | None:
        """Return the PII report for a session."""
        with DBSession(self.engine) as db:
            return (
                db.query(PIIReport)
                .filter(PIIReport.session_id == session_id)
                .first()
            )


class RefinementRepository:
    """Store multi-turn refinement conversation messages."""

    def __init__(self, sqlite_path: str) -> None:
        self.engine = get_engine(sqlite_path)

    def add_message(
        self, session_id: str, role: str, content: str
    ) -> None:
        """Append a conversation message."""
        with DBSession(self.engine) as db:
            db.add(
                RefinementMessage(
                    session_id=session_id,
                    role=role,
                    content=content,
                    created_at=now_iso(),
                )
            )
            db.commit()

    def get_window(
        self, session_id: str, window: int = 6
    ) -> list[dict[str, str]]:
        """Return the last `window` messages as dicts."""
        with DBSession(self.engine) as db:
            rows = (
                db.query(RefinementMessage)
                .filter(
                    RefinementMessage.session_id == session_id
                )
                .order_by(RefinementMessage.id.desc())
                .limit(window)
                .all()
            )
        return [
            {"role": r.role, "content": r.content}
            for r in reversed(rows)
        ]
