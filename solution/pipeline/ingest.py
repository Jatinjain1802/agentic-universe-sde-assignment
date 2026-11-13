"""Ingestion: lead CSVs and event JSONL files into the database.

Ordering rules from the spec:
* drops are processed in the order they are ingested;
* files inside a drop in filename order;
* rows/lines inside a file in file order.

Each file is ingested in a single transaction and recorded in
``ingested_files`` at commit time, so re-running ``ingest`` on the same drop
changes nothing and a crash mid-file simply rolls back.
"""
from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from . import db
from .crypto import PiiProtector
from .normalize import (
    normalize_header,
    parse_date,
    parse_decimal,
    parse_enum,
    parse_phone,
    parse_timestamp,
)
from .schema import WorkflowSchema

_EVENT_BATCH_SIZE = 5000


@dataclass
class _Lead:
    """In-memory working copy of a lead while ingesting."""

    lead_id: int | None
    workflow_id: str
    phone_key: str
    canonical_phone: str
    batch_id: str
    values: dict  # field name -> parsed value (None when still missing)


@dataclass
class IngestStats:
    """Per-run counters, persisted to the DB after every file."""

    lead_rows_total: int = 0
    lead_rows_rejected: int = 0
    duplicate_lead_rows_merged: int = 0
    field_warnings: int = 0
    events_total: int = 0
    malformed_events: int = 0
    duplicate_events_dropped: int = 0
    rejections_by_reason: dict[str, int] = field(default_factory=dict)

    def flush(self, conn) -> None:
        db.bump(conn, "lead_rows_total", self.lead_rows_total)
        db.bump(conn, "lead_rows_rejected", self.lead_rows_rejected)
        db.bump(conn, "duplicate_lead_rows_merged", self.duplicate_lead_rows_merged)
        db.bump(conn, "field_warnings", self.field_warnings)
        db.bump(conn, "events_total", self.events_total)
        db.bump(conn, "malformed_events", self.malformed_events)
        db.bump(conn, "duplicate_events_dropped", self.duplicate_events_dropped)
        for reason, n in self.rejections_by_reason.items():
            db.bump_reason(conn, reason, n)
        self.__init__()  # reset


class Pipeline:
    def __init__(self, conn, pii: PiiProtector, schemas: dict[str, WorkflowSchema]):
        self.conn = conn
        self.pii = pii
        self.schemas = schemas
        self.stats = IngestStats()
        # In-memory indexes, loaded once from the DB and kept in sync.
        self.leads: dict[tuple[str, str], _Lead] = {}
        self.call_lead: dict[str, int] = {}
        self.seen_events: set[str] = set()
        self._load_indexes()

    # ------------------------------------------------------------------ setup

    def _load_indexes(self) -> None:
        for row in self.conn.execute("SELECT * FROM leads"):
            values = json.loads(row["attrs"])
            pii_values = json.loads(self.pii.decrypt(row["pii_enc"]))
            values.update(pii_values)
            if row["external_ref"] is not None:
                ref_name = self.schemas[row["workflow_id"]].reference_field.name
                values[ref_name] = row["external_ref"]
            canonical = pii_values.get("phone", "")
            self.leads[(row["workflow_id"], row["phone_key"])] = _Lead(
                lead_id=row["lead_id"],
                workflow_id=row["workflow_id"],
                phone_key=row["phone_key"],
                canonical_phone=canonical,
                batch_id=row["batch_id"],
                values=values,
            )
        for row in self.conn.execute("SELECT event_id FROM events"):
            self.seen_events.add(row["event_id"])
        for row in self.conn.execute(
            "SELECT call_id, lead_id FROM events WHERE lead_id IS NOT NULL AND call_id IS NOT NULL"
        ):
            self.call_lead[row["call_id"]] = row["lead_id"]

    # ------------------------------------------------------------------- API

    def ingest_path(self, path: Path) -> None:
        """Ingest a drop directory, a directory containing drops, or one file."""
        path = Path(path)
        if path.is_file():
            self._ingest_files([path])
            return
        drops = sorted(p for p in path.iterdir() if p.is_dir() and p.name.startswith("drop_"))
        if drops:
            for drop in drops:
                self.ingest_path(drop)
            return
        # The directory itself is one drop.
        lead_files = sorted(path.glob("leads/**/*.csv"))
        event_files = sorted(path.glob("events/*.jsonl"))
        self._ingest_files(lead_files + event_files)
        self.relink_orphans()

    def _ingest_files(self, files: list[Path]) -> None:
        for file_path in files:
            resolved = str(file_path.resolve())
            already = self.conn.execute(
                "SELECT 1 FROM ingested_files WHERE path = ?", (resolved,)
            ).fetchone()
            if already:
                continue
            if file_path.suffix == ".csv":
                self._ingest_lead_file(file_path)
            elif file_path.suffix == ".jsonl":
                self._ingest_event_file(file_path)
            else:
                continue
            self.conn.execute(
                "INSERT INTO ingested_files (path, ingested_at) VALUES (?, ?)",
                (resolved, datetime.now(timezone.utc).isoformat()),
            )
            self.stats.flush(self.conn)
            self.conn.commit()
        self.relink_orphans()

    # ------------------------------------------------------------------ leads

    def _column_map(self, schema: WorkflowSchema, header_row: list[str]) -> dict[str, int]:
        """Map field name -> column index using normalised header matching.
        Columns that match nothing are ignored."""
        mapping: dict[str, int] = {}
        for idx, raw_header in enumerate(header_row):
            normalized = normalize_header(raw_header)
            for f in schema.fields:
                if f.name not in mapping and f.matches(normalized):
                    mapping[f.name] = idx
                    break
        return mapping

    def _row_values(self, schema: WorkflowSchema, colmap: dict[str, int], row: list[str]):
        """Parse one data row.

        Returns (values, rejection_reason, warnings). Exactly one of values /
        rejection_reason is set: the first failure in schema order wins, and
        warnings are only returned for rows that are not rejected.
        """

        def raw_of(f) -> str:
            idx = colmap.get(f.name)
            if idx is None or idx >= len(row):
                return ""
            return (row[idx] or "").strip()

        values: dict[str, object] = {}
        warnings = 0
        for f in schema.fields:
            raw = raw_of(f)
            if raw == "":
                if f.required:
                    return None, f"missing_required:{f.name}", 0
                values[f.name] = None
                continue
            if f.type == "string":
                parsed = raw
            elif f.type == "phone":
                parsed = parse_phone(raw)
            elif f.type == "date":
                parsed = parse_date(raw)
            elif f.type == "decimal":
                parsed = parse_decimal(raw)
            elif f.type == "enum":
                parsed = parse_enum(raw, f.values or [])
            else:
                parsed = raw  # unknown type: keep as string
            if parsed is None:
                if f.type == "phone":
                    return None, "invalid_phone", 0
                if f.required:
                    # Spec defines no reason for a present-but-invalid required
                    # non-phone field; we treat it as missing (see SUBMISSION.md).
                    return None, f"missing_required:{f.name}", 0
                values[f.name] = None
                warnings += 1
            else:
                values[f.name] = parsed.isoformat() if f.type == "date" else parsed
                if f.type == "decimal":
                    values[f.name] = str(parsed)
        return values, None, warnings

    def _ingest_lead_file(self, path: Path) -> None:
        batch_id = path.stem
        # Find the workflow from the path: .../leads/<workflow_id>/<file>.csv
        workflow_id = path.parent.name
        schema = self.schemas.get(workflow_id)
        if schema is None:
            return

        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            try:
                header = next(reader)
            except StopIteration:
                return
            colmap = self._column_map(schema, header)
            row_number = 0
            for row in reader:
                if all((cell or "").strip() == "" for cell in row):
                    continue  # blank rows are skipped and not counted
                row_number += 1
                self.stats.lead_rows_total += 1
                values, reason, warnings = self._row_values(schema, colmap, row)
                if reason is not None:
                    self.stats.lead_rows_rejected += 1
                    self.stats.rejections_by_reason[reason] = (
                        self.stats.rejections_by_reason.get(reason, 0) + 1
                    )
                    self.conn.execute(
                        "INSERT OR IGNORE INTO rejections (workflow_id, batch_id, row_number, reason)"
                        " VALUES (?, ?, ?, ?)",
                        (workflow_id, batch_id, row_number, reason),
                    )
                    continue
                self.stats.field_warnings += warnings
                self._merge_lead(schema, values, batch_id)
        self._flush_leads(schema)

    def _merge_lead(self, schema: WorkflowSchema, values: dict, batch_id: str) -> None:
        phone_key = self.pii.match_key(values["phone"])
        key = (schema.workflow_id, phone_key)
        existing = self.leads.get(key)
        if existing is None:
            self.leads[key] = _Lead(
                lead_id=None,
                workflow_id=schema.workflow_id,
                phone_key=phone_key,
                canonical_phone=values["phone"],
                batch_id=batch_id,
                values=dict(values),
            )
        else:
            # Merged duplicate: overwrite only fields whose new value is
            # non-null (last write wins). batch_id never changes.
            self.stats.duplicate_lead_rows_merged += 1
            for f in schema.fields:
                new_value = values.get(f.name)
                if new_value is not None:
                    existing.values[f.name] = new_value

    def _flush_leads(self, schema: WorkflowSchema) -> None:
        """Write new / merged leads for this workflow to the DB. Field
        defaults are applied at flush time, to fields that are still null
        after all merging so far (equivalent to applying them at the end,
        because later rows only overwrite with non-null values)."""
        for (wf, _), lead in self.leads.items():
            if wf != schema.workflow_id:
                continue
            for f in schema.fields:
                if f.default is not None and lead.values.get(f.name) is None:
                    lead.values[f.name] = f.default
            ref_name = schema.reference_field.name
            external_ref = lead.values.get(ref_name)
            attrs = {
                f.name: lead.values.get(f.name)
                for f in schema.fields
                if not f.pii and not f.reference
            }
            pii_values = {f.name: lead.values.get(f.name) for f in schema.fields if f.pii}
            pii_enc = self.pii.encrypt(json.dumps(pii_values, ensure_ascii=False))
            self.conn.execute(
                """
                INSERT INTO leads (workflow_id, phone_key, phone_last4, external_ref,
                                   batch_id, attrs, pii_enc)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(workflow_id, phone_key) DO UPDATE SET
                    phone_last4  = excluded.phone_last4,
                    external_ref = excluded.external_ref,
                    attrs        = excluded.attrs,
                    pii_enc      = excluded.pii_enc
                """,
                (
                    lead.workflow_id,
                    lead.phone_key,
                    lead.canonical_phone[-4:],
                    external_ref,
                    lead.batch_id,
                    json.dumps(attrs, ensure_ascii=False),
                    pii_enc,
                ),
            )
            if lead.lead_id is None:
                lead.lead_id = self.conn.execute(
                    "SELECT lead_id FROM leads WHERE workflow_id = ? AND phone_key = ?",
                    (lead.workflow_id, lead.phone_key),
                ).fetchone()["lead_id"]

    # ----------------------------------------------------------------- events

    def _ingest_event_file(self, path: Path) -> None:
        batch: list[tuple] = []
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue  # empty lines are not counted
                self.stats.events_total += 1
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    self.stats.malformed_events += 1
                    continue
                event_id = event.get("event_id")
                if not event_id:
                    self.stats.malformed_events += 1
                    continue
                occurred = parse_timestamp(str(event.get("occurred_at", "")))
                if occurred is None:
                    # Spec only calls invalid JSON / missing event_id malformed;
                    # we treat an unusable timestamp the same way (see SUBMISSION.md).
                    self.stats.malformed_events += 1
                    continue
                if event_id in self.seen_events:
                    self.stats.duplicate_events_dropped += 1
                    continue
                self.seen_events.add(event_id)

                event_type = str(event.get("event_type", ""))
                workflow_id = str(event.get("workflow_id", ""))
                call_id = event.get("call_id")
                phone_key = None
                lead_id = None
                if event_type == "disposition.override":
                    # Overrides carry no phone; they link through the call.
                    lead_id = self.call_lead.get(call_id)
                else:
                    raw_phone = str(event.get("phone") or "")
                    canonical = parse_phone(raw_phone) if raw_phone.strip() else None
                    if canonical is not None and workflow_id in self.schemas:
                        phone_key = self.pii.match_key(canonical)
                        lead = self.leads.get((workflow_id, phone_key))
                        lead_id = lead.lead_id if lead else None

                disposition_code = None
                if event_type.startswith("disposition."):
                    payload = event.get("payload") or {}
                    disposition_code = payload.get("disposition_code")

                batch.append(
                    (
                        event_id,
                        event_type,
                        workflow_id,
                        call_id,
                        occurred.isoformat(),
                        phone_key,
                        lead_id,
                        disposition_code,
                        json.dumps(event.get("payload") or {}, ensure_ascii=False),
                    )
                )
                if lead_id is not None and call_id:
                    self.call_lead[call_id] = lead_id
                if len(batch) >= _EVENT_BATCH_SIZE:
                    self._flush_events(batch)
        self._flush_events(batch)

    def _flush_events(self, batch: list[tuple]) -> None:
        if not batch:
            return
        self.conn.executemany(
            """
            INSERT OR IGNORE INTO events
                (event_id, event_type, workflow_id, call_id, occurred_at_utc,
                 phone_key, lead_id, disposition_code, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            batch,
        )
        batch.clear()

    # --------------------------------------------------------------- relink

    def relink_orphans(self) -> None:
        """Link events that arrived before their lead (or before the rest of
        their call). Runs until no more orphans can be linked, so a chain
        like override -> call event -> lead resolves fully."""
        while True:
            by_phone = self.conn.execute(
                """
                UPDATE events SET lead_id = (
                    SELECT l.lead_id FROM leads l
                    WHERE l.workflow_id = events.workflow_id
                      AND l.phone_key = events.phone_key)
                WHERE lead_id IS NULL AND phone_key IS NOT NULL AND EXISTS (
                    SELECT 1 FROM leads l
                    WHERE l.workflow_id = events.workflow_id
                      AND l.phone_key = events.phone_key)
                """
            ).rowcount
            by_call = self.conn.execute(
                """
                UPDATE events SET lead_id = (
                    SELECT e2.lead_id FROM events e2
                    WHERE e2.call_id = events.call_id AND e2.lead_id IS NOT NULL LIMIT 1)
                WHERE lead_id IS NULL AND event_type = 'disposition.override' AND EXISTS (
                    SELECT 1 FROM events e2
                    WHERE e2.call_id = events.call_id AND e2.lead_id IS NOT NULL)
                """
            ).rowcount
            self.conn.commit()
            if by_phone == 0 and by_call == 0:
                break
        # Refresh the call -> lead index for future overrides.
        self.call_lead = {
            row["call_id"]: row["lead_id"]
            for row in self.conn.execute(
                "SELECT call_id, lead_id FROM events"
                " WHERE lead_id IS NOT NULL AND call_id IS NOT NULL"
            )
        }
