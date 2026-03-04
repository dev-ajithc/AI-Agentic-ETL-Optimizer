-- AI Agentic ETL Optimizer — Initial Schema
-- Migration: 001_initial
-- Run once at startup via db/repository.py

PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS sessions (
    id             TEXT PRIMARY KEY,
    created_at     TEXT NOT NULL,
    input_hash     TEXT NOT NULL,
    input_type     TEXT NOT NULL CHECK (input_type IN ('pyspark', 'prefect')),
    target         TEXT NOT NULL CHECK (target IN ('snowflake', 'delta_lake')),
    status         TEXT NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending','running','success','failed')),
    tokens_used    INTEGER,
    cost_usd       REAL,
    prompt_version INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS artifacts (
    id             TEXT PRIMARY KEY,
    session_id     TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    artifact_type  TEXT NOT NULL
                       CHECK (artifact_type IN
                              ('code','diff','compliance','validation','zip')),
    content        TEXT NOT NULL,
    content_hash   TEXT NOT NULL,
    created_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id     TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    event_data     TEXT,
    timestamp      TEXT NOT NULL,
    severity       TEXT NOT NULL DEFAULT 'INFO'
                       CHECK (severity IN
                              ('DEBUG','INFO','WARNING','ERROR','CRITICAL'))
);

CREATE TABLE IF NOT EXISTS pii_reports (
    id             TEXT PRIMARY KEY,
    session_id     TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    entities       TEXT NOT NULL,
    risk_level     TEXT NOT NULL
                       CHECK (risk_level IN ('low','medium','high','critical')),
    created_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS refinement_messages (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id     TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    role           TEXT NOT NULL CHECK (role IN ('user','assistant')),
    content        TEXT NOT NULL,
    created_at     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sessions_created_at
    ON sessions(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_artifacts_session
    ON artifacts(session_id);
CREATE INDEX IF NOT EXISTS idx_audit_session
    ON audit_log(session_id);
CREATE INDEX IF NOT EXISTS idx_pii_session
    ON pii_reports(session_id);
