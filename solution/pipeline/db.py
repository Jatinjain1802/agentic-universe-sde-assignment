"""SQLite storage layer.

Tables
------
leads
    One row per lead identity ``(workflow_id, phone_key)``. Phone numbers and
    names are never stored in plaintext: ``phone_key`` is a one-way HMAC used
    for joining, and everything PII lives inside the encrypted ``pii_enc``
    blob. ``batch_id`` is the file stem of the first valid row, permanently.
events
    The append-only interaction ledger. One row per unique ``event_id`` -
    including events that are currently orphans (``lead_id IS NULL``), so an
    event that arrived before its lead can be linked later. Nothing here is
    ever updated except the ``lead_id`` of an orphan being linked; overrides
    are new rows, never edits.
rejections / counters / reason_counters / ingested_files
    Bookkeeping. ``ingested_files`` makes re-running ingest on the same drop
    a no-op, which is what makes the pipeline idempotent.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

DDL = """
CREATE TABLE IF NOT EXISTS leads (
    lead_id      INTEGER PRIMARY KEY,
    workflow_id  TEXT NOT NULL,
    phone_key    TEXT NOT NULL,          -- HMAC-SHA256 of the canonical phone
    phone_last4  TEXT NOT NULL,          -- for the masked report column
    external_ref TEXT,                   -- schema field marked "reference": true
    batch_id     TEXT NOT NULL,          -- file stem of the first valid row
    attrs        TEXT NOT NULL DEFAULT '{}',  -- non-PII field values as JSON
    pii_enc      TEXT NOT NULL,          -- Fernet-encrypted JSON of PII values
    UNIQUE (workflow_id, phone_key)
);

CREATE TABLE IF NOT EXISTS events (
    event_id         TEXT PRIMARY KEY,
    event_type       TEXT NOT NULL,
    workflow_id      TEXT,
    call_id          TEXT,
    occurred_at_utc  TEXT,               -- ISO-8601 in UTC
    phone_key        TEXT,               -- HMAC of event phone; NULL for overrides
    lead_id          INTEGER REFERENCES leads(lead_id),  -- NULL while orphan
    disposition_code TEXT,               -- payload code for disposition.* events
    payload          TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_events_call ON events(call_id);
CREATE INDEX IF NOT EXISTS idx_events_lead ON events(lead_id);
CREATE INDEX IF NOT EXISTS idx_events_link ON events(workflow_id, phone_key);

CREATE TABLE IF NOT EXISTS rejections (
    workflow_id TEXT NOT NULL,
    batch_id    TEXT NOT NULL,
    row_number  INTEGER NOT NULL,        -- non-blank data row, 1-based
    reason      TEXT NOT NULL,
    PRIMARY KEY (workflow_id, batch_id, row_number)
);

CREATE TABLE IF NOT EXISTS counters (
    key   TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS reason_counters (
    reason TEXT PRIMARY KEY,
    value  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS ingested_files (
    path        TEXT PRIMARY KEY,        -- absolute file path
    ingested_at TEXT NOT NULL
);
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(DDL)
    return conn


def bump(conn: sqlite3.Connection, key: str, n: int = 1) -> None:
    if n == 0:
        return
    conn.execute(
        "INSERT INTO counters (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = value + excluded.value",
        (key, n),
    )


def bump_reason(conn: sqlite3.Connection, reason: str, n: int = 1) -> None:
    if n == 0:
        return
    conn.execute(
        "INSERT INTO reason_counters (reason, value) VALUES (?, ?) "
        "ON CONFLICT(reason) DO UPDATE SET value = value + excluded.value",
        (reason, n),
    )


def counter(conn: sqlite3.Connection, key: str) -> int:
    row = conn.execute("SELECT value FROM counters WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else 0


def reason_counters(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute("SELECT reason, value FROM reason_counters ORDER BY reason").fetchall()
    return {row["reason"]: row["value"] for row in rows}
