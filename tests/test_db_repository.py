"""Tests for the database repository layer."""

import json

import pytest

from db.repository import (
    ArtifactRepository,
    AuditRepository,
    PIIRepository,
    RefinementRepository,
    SessionRepository,
    init_db,
)


class TestSessionRepository:
    """Tests for SessionRepository CRUD operations."""

    def test_create_returns_uuid(self, tmp_sqlite: str) -> None:
        repo = SessionRepository(tmp_sqlite)
        sid = repo.create(
            input_hash="abc123",
            input_type="pyspark",
            target="snowflake",
            prompt_version=1,
        )
        assert len(sid) == 36
        assert sid.count("-") == 4

    def test_get_returns_created_session(
        self, tmp_sqlite: str
    ) -> None:
        repo = SessionRepository(tmp_sqlite)
        sid = repo.create(
            input_hash="hash1",
            input_type="prefect",
            target="delta_lake",
            prompt_version=1,
        )
        session = repo.get(sid)
        assert session is not None
        assert session.id == sid
        assert session.input_type == "prefect"
        assert session.target == "delta_lake"
        assert session.status == "pending"

    def test_update_status(self, tmp_sqlite: str) -> None:
        repo = SessionRepository(tmp_sqlite)
        sid = repo.create(
            input_hash="hash2",
            input_type="pyspark",
            target="snowflake",
            prompt_version=1,
        )
        repo.update_status(
            sid,
            status="success",
            tokens_used=1500,
            cost_usd=0.003,
        )
        session = repo.get(sid)
        assert session.status == "success"
        assert session.tokens_used == 1500
        assert abs(session.cost_usd - 0.003) < 1e-9

    def test_update_nonexistent_session_no_error(
        self, tmp_sqlite: str
    ) -> None:
        repo = SessionRepository(tmp_sqlite)
        repo.update_status("nonexistent-id", status="failed")

    def test_list_recent_returns_ordered_results(
        self, tmp_sqlite: str
    ) -> None:
        repo = SessionRepository(tmp_sqlite)
        ids = []
        for i in range(5):
            sid = repo.create(
                input_hash=f"hash_{i}",
                input_type="pyspark",
                target="snowflake",
                prompt_version=1,
            )
            ids.append(sid)
        recent = repo.list_recent(limit=3)
        assert len(recent) == 3

    def test_list_recent_limit_respected(
        self, tmp_sqlite: str
    ) -> None:
        repo = SessionRepository(tmp_sqlite)
        for i in range(10):
            repo.create(
                input_hash=f"h{i}",
                input_type="pyspark",
                target="snowflake",
                prompt_version=1,
            )
        recent = repo.list_recent(limit=5)
        assert len(recent) == 5


class TestArtifactRepository:
    """Tests for ArtifactRepository operations."""

    def test_save_and_retrieve_artifact(
        self, tmp_sqlite: str
    ) -> None:
        session_repo = SessionRepository(tmp_sqlite)
        sid = session_repo.create(
            input_hash="h1",
            input_type="pyspark",
            target="snowflake",
            prompt_version=1,
        )
        artifact_repo = ArtifactRepository(tmp_sqlite)
        artifact_id = artifact_repo.save(
            sid, "code", "print('hello')"
        )
        assert len(artifact_id) == 36

        artifacts = artifact_repo.get_by_session(sid)
        assert len(artifacts) == 1
        assert artifacts[0].content == "print('hello')"
        assert artifacts[0].artifact_type == "code"

    def test_content_hash_is_sha256(
        self, tmp_sqlite: str
    ) -> None:
        import hashlib

        session_repo = SessionRepository(tmp_sqlite)
        sid = session_repo.create(
            input_hash="h2",
            input_type="pyspark",
            target="snowflake",
            prompt_version=1,
        )
        artifact_repo = ArtifactRepository(tmp_sqlite)
        content = "df = spark.read.parquet('/data')"
        artifact_repo.save(sid, "code", content)

        artifacts = artifact_repo.get_by_session(sid, "code")
        expected_hash = hashlib.sha256(
            content.encode("utf-8")
        ).hexdigest()
        assert artifacts[0].content_hash == expected_hash

    def test_filter_by_artifact_type(
        self, tmp_sqlite: str
    ) -> None:
        session_repo = SessionRepository(tmp_sqlite)
        sid = session_repo.create(
            input_hash="h3",
            input_type="pyspark",
            target="snowflake",
            prompt_version=1,
        )
        artifact_repo = ArtifactRepository(tmp_sqlite)
        artifact_repo.save(sid, "code", "code content")
        artifact_repo.save(sid, "diff", "diff content")
        artifact_repo.save(sid, "compliance", "{}")

        code_only = artifact_repo.get_by_session(sid, "code")
        assert len(code_only) == 1
        assert code_only[0].artifact_type == "code"


class TestAuditRepository:
    """Tests for append-only audit log."""

    def test_log_and_retrieve(self, tmp_sqlite: str) -> None:
        repo = AuditRepository(tmp_sqlite)
        repo.log(
            session_id="sess-1",
            event_type="input_received",
            event_data={"size": 500},
            severity="INFO",
        )
        entries = repo.get_by_session("sess-1")
        assert len(entries) == 1
        assert entries[0].event_type == "input_received"
        assert entries[0].severity == "INFO"

    def test_event_data_is_json(self, tmp_sqlite: str) -> None:
        repo = AuditRepository(tmp_sqlite)
        data = {"key": "value", "count": 42}
        repo.log(
            session_id="sess-2",
            event_type="test_event",
            event_data=data,
        )
        entries = repo.get_by_session("sess-2")
        parsed = json.loads(entries[0].event_data)
        assert parsed["key"] == "value"
        assert parsed["count"] == 42

    def test_none_event_data_allowed(
        self, tmp_sqlite: str
    ) -> None:
        repo = AuditRepository(tmp_sqlite)
        repo.log(
            session_id="sess-3",
            event_type="no_data_event",
            event_data=None,
        )
        entries = repo.get_by_session("sess-3")
        assert entries[0].event_data is None

    def test_multiple_entries_ordered(
        self, tmp_sqlite: str
    ) -> None:
        repo = AuditRepository(tmp_sqlite)
        for event in ["start", "pii_found", "complete"]:
            repo.log("sess-4", event_type=event)
        entries = repo.get_by_session("sess-4")
        assert len(entries) == 3
        assert entries[0].event_type == "start"
        assert entries[2].event_type == "complete"


class TestPIIRepository:
    """Tests for PII report storage."""

    def test_save_and_retrieve(self, tmp_sqlite: str) -> None:
        session_repo = SessionRepository(tmp_sqlite)
        sid = session_repo.create(
            input_hash="h_pii",
            input_type="pyspark",
            target="snowflake",
            prompt_version=1,
        )
        pii_repo = PIIRepository(tmp_sqlite)
        entities = [
            {"type": "EMAIL", "score": 3, "source": "regex"}
        ]
        report_id = pii_repo.save(
            sid, entities=entities, risk_level="high"
        )
        assert len(report_id) == 36

        report = pii_repo.get_by_session(sid)
        assert report is not None
        assert report.risk_level == "high"
        loaded_entities = json.loads(report.entities)
        assert loaded_entities[0]["type"] == "EMAIL"

    def test_returns_none_for_unknown_session(
        self, tmp_sqlite: str
    ) -> None:
        pii_repo = PIIRepository(tmp_sqlite)
        result = pii_repo.get_by_session("nonexistent-session")
        assert result is None


class TestRefinementRepository:
    """Tests for multi-turn refinement message storage."""

    def test_add_and_retrieve_messages(
        self, tmp_sqlite: str
    ) -> None:
        session_repo = SessionRepository(tmp_sqlite)
        sid = session_repo.create(
            input_hash="h_ref",
            input_type="pyspark",
            target="snowflake",
            prompt_version=1,
        )
        repo = RefinementRepository(tmp_sqlite)
        repo.add_message(sid, "user", "Make it faster")
        repo.add_message(sid, "assistant", "Here is the optimized version")

        messages = repo.get_window(sid, window=6)
        assert len(messages) == 2
        assert messages[0]["role"] == "user"
        assert messages[0]["content"] == "Make it faster"
        assert messages[1]["role"] == "assistant"

    def test_window_limit_respected(
        self, tmp_sqlite: str
    ) -> None:
        session_repo = SessionRepository(tmp_sqlite)
        sid = session_repo.create(
            input_hash="h_ref2",
            input_type="pyspark",
            target="snowflake",
            prompt_version=1,
        )
        repo = RefinementRepository(tmp_sqlite)
        for i in range(10):
            role = "user" if i % 2 == 0 else "assistant"
            repo.add_message(sid, role, f"message {i}")

        window = repo.get_window(sid, window=4)
        assert len(window) == 4

    def test_empty_window_for_new_session(
        self, tmp_sqlite: str
    ) -> None:
        session_repo = SessionRepository(tmp_sqlite)
        sid = session_repo.create(
            input_hash="h_ref3",
            input_type="pyspark",
            target="snowflake",
            prompt_version=1,
        )
        repo = RefinementRepository(tmp_sqlite)
        window = repo.get_window(sid)
        assert window == []
