"""Report generation: the five output files, derived from current DB state.

Reports always reflect everything ingested so far, so they are recomputed
from the ledger on every run. All dates shown are IST dates.
"""
from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from . import db
from .derive import CallFacts, effective_disposition, lead_bucket
from .normalize import IST
from .schema import Disposition


def _write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)


def _load_calls(conn) -> tuple[dict[str, CallFacts], dict[int, list[CallFacts]]]:
    """Build CallFacts per call_id from every linked event, plus a
    lead_id -> calls index. Orphans (lead_id NULL) are kept out of every
    report, exactly as the spec requires."""
    calls: dict[str, CallFacts] = {}
    cursor = conn.execute(
        "SELECT event_id, event_type, workflow_id, call_id, occurred_at_utc,"
        "       lead_id, disposition_code"
        " FROM events WHERE lead_id IS NOT NULL"
    )
    for row in cursor:
        call_id = row["call_id"]
        if not call_id:
            continue
        call = calls.get(call_id)
        if call is None:
            call = CallFacts(
                call_id=call_id,
                lead_id=row["lead_id"],
                workflow_id=row["workflow_id"],
            )
            calls[call_id] = call
        occurred = datetime.fromisoformat(row["occurred_at_utc"]) if row["occurred_at_utc"] else None
        etype = row["event_type"]
        if etype == "call.initiated":
            # Two initiated events for one call_id should not happen; keep the
            # earliest so the call's date is stable.
            if occurred is not None and (call.initiated_at is None or occurred < call.initiated_at):
                call.initiated_at = occurred
        elif etype == "call.connected":
            call.connected = True
        elif etype == "call.telephony_failed":
            call.telephony_failed = True
        elif etype == "disposition.set" and occurred is not None:
            call.sets.append((occurred, row["disposition_code"] or "", row["event_id"]))
        elif etype == "disposition.override" and occurred is not None:
            call.overrides.append((occurred, row["disposition_code"] or "", row["event_id"]))
    by_lead: dict[int, list[CallFacts]] = defaultdict(list)
    for call in calls.values():
        if call.is_call:
            by_lead[call.lead_id].append(call)
    return calls, by_lead


def _count_unknown_dispositions(conn, catalogues: dict[str, dict[str, Disposition]]) -> int:
    """Linked disposition.* events whose code is not in the workflow's
    catalogue. Orphans are excluded: an event that cannot be linked is
    counted as an orphan, not as unknown (spec processing order)."""
    total = 0
    cursor = conn.execute(
        "SELECT workflow_id, disposition_code, COUNT(*) AS n FROM events"
        " WHERE lead_id IS NOT NULL AND event_type LIKE 'disposition.%'"
        " GROUP BY workflow_id, disposition_code"
    )
    for row in cursor:
        known = catalogues.get(row["workflow_id"], {})
        if row["disposition_code"] not in known:
            total += row["n"]
    return total


def generate_reports(conn, catalogues: dict[str, dict[str, Disposition]], out_dir: Path) -> dict:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    leads = conn.execute("SELECT * FROM leads").fetchall()
    _, calls_by_lead = _load_calls(conn)

    # ---------------------------------------------------------------- leads
    lead_report_rows: list[list] = []
    # (workflow_id, batch_id) -> bucket -> count
    batch_buckets: dict[tuple[str, str], dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    # (workflow_id, ist_date) -> aggregates
    daily: dict[tuple[str, str], dict] = defaultdict(
        lambda: {"total": 0, "connected": 0, "success_leads": set()}
    )

    for lead in leads:
        workflow_id = lead["workflow_id"]
        catalogue = catalogues.get(workflow_id, {})
        known = set(catalogue)
        calls = calls_by_lead.get(lead["lead_id"], [])

        bucket = lead_bucket(calls, catalogue)
        batch_buckets[(workflow_id, lead["batch_id"])]["total"] += 1
        batch_buckets[(workflow_id, lead["batch_id"])][bucket] += 1

        connected_calls = sum(1 for c in calls if c.connected)
        ever_success = False
        current_disposition = ""
        latest_initiated: tuple[datetime, str] | None = None  # (initiated_at, call_id)
        latest_disp_at: tuple[datetime, str] | None = None
        for call in calls:
            eff = effective_disposition(call, known)
            if eff is not None and catalogue[eff].is_success:
                ever_success = True
            recency = (call.initiated_at, call.call_id)
            if latest_initiated is None or recency > latest_initiated:
                latest_initiated = recency
            if eff is not None and (latest_disp_at is None or recency > latest_disp_at):
                latest_disp_at = recency
                current_disposition = eff
            date_key = call.initiated_at.astimezone(IST).date().isoformat()
            day = daily[(workflow_id, date_key)]
            day["total"] += 1
            if call.connected:
                day["connected"] += 1
            if eff is not None and catalogue[eff].is_success:
                day["success_leads"].add(lead["lead_id"])
        last_call_at = (
            latest_initiated[0].astimezone(IST).isoformat() if latest_initiated else ""
        )

        lead_report_rows.append(
            [
                workflow_id,
                lead["external_ref"] or "",
                lead["batch_id"],
                "+91******" + lead["phone_last4"],
                bucket,
                len(calls),
                connected_calls,
                last_call_at,
                current_disposition,
                "true" if ever_success else "false",
            ]
        )

    # ------------------------------------------------------- batch_summary
    batch_rows = []
    for (workflow_id, batch_id), counts in batch_buckets.items():
        batch_rows.append(
            [
                workflow_id,
                batch_id,
                counts["total"],
                counts["yet_to_reach_out"],
                counts["in_progress_telephony_failed"],
                counts["in_progress_dialed"],
                counts["completed"],
            ]
        )
    batch_rows.sort(key=lambda r: (r[0], r[1]))
    _write_csv(
        out_dir / "batch_summary.csv",
        ["workflow_id", "batch_id", "total_leads", "yet_to_reach_out",
         "in_progress_telephony_failed", "in_progress_dialed", "completed"],
        batch_rows,
    )

    # ------------------------------------------------------- daily_overview
    daily_rows = []
    for (workflow_id, date_key), agg in daily.items():
        rate = agg["connected"] / agg["total"] if agg["total"] else 0.0
        daily_rows.append(
            [
                workflow_id,
                date_key,
                agg["total"],
                agg["connected"],
                f"{rate:.4f}",
                len(agg["success_leads"]),
            ]
        )
    daily_rows.sort(key=lambda r: (r[0], r[1]))
    _write_csv(
        out_dir / "daily_overview.csv",
        ["workflow_id", "date", "total_calls", "connected_calls",
         "connectivity_rate", "success_leads"],
        daily_rows,
    )

    # ---------------------------------------------------------- lead_report
    lead_report_rows.sort(key=lambda r: (r[0], r[1]))
    _write_csv(
        out_dir / "lead_report.csv",
        ["workflow_id", "external_ref", "batch_id", "phone_masked", "bucket",
         "total_calls", "connected_calls", "last_call_at", "current_disposition",
         "ever_success"],
        lead_report_rows,
    )

    # ----------------------------------------------------------- rejections
    rejection_rows = [
        [row["workflow_id"], row["batch_id"], row["row_number"], row["reason"]]
        for row in conn.execute(
            "SELECT workflow_id, batch_id, row_number, reason FROM rejections"
            " ORDER BY workflow_id, batch_id, row_number"
        )
    ]
    _write_csv(
        out_dir / "rejections.csv",
        ["workflow_id", "batch_id", "row_number", "reason"],
        rejection_rows,
    )

    # --------------------------------------------------------- data_quality
    orphan_count = conn.execute(
        "SELECT COUNT(*) AS n FROM events WHERE lead_id IS NULL"
    ).fetchone()["n"]
    quality = {
        "lead_rows_total": db.counter(conn, "lead_rows_total"),
        "lead_rows_rejected": db.counter(conn, "lead_rows_rejected"),
        "rejections_by_reason": db.reason_counters(conn),
        "duplicate_lead_rows_merged": db.counter(conn, "duplicate_lead_rows_merged"),
        "field_warnings": db.counter(conn, "field_warnings"),
        "events_total": db.counter(conn, "events_total"),
        "malformed_events": db.counter(conn, "malformed_events"),
        "duplicate_events_dropped": db.counter(conn, "duplicate_events_dropped"),
        "orphan_events": orphan_count,
        "unknown_disposition_events": _count_unknown_dispositions(conn, catalogues),
    }
    (out_dir / "data_quality.json").write_text(
        json.dumps(quality, indent=2) + "\n", encoding="utf-8"
    )
    return quality
