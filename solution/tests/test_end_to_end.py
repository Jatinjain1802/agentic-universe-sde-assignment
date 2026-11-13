"""End-to-end test on a hand-built mini dataset.

Every messy case from the spec is exercised against expected outputs that
were computed by hand: header aliases, blank rows, rejection reasons and row
numbers, duplicate merging (last-write-wins, permanent batch_id), PII at
rest, event dedupe, malformed lines, orphans that link later, unknown
dispositions, overrides without a phone, and idempotent re-ingest.
"""
import json
import sqlite3
from pathlib import Path

import pytest

from solution.pipeline import db as db_mod
from solution.pipeline.cli import main as cli_main

BATCH_001 = """customer_name,phone,policy_number,policy_expiry,premium_amount,preferred_language,agent_notes
Asha Rao,+91 98765 43210,POL-1,01/12/2026,"₹12,000",EN,first upload
,,,,,,
Bad Phone,12345,POL-2,01/12/2026,100,en,
,09876543211,POL-3,01/12/2026,100,en,
Bala,09876543211,POL-4,01/12/2026,N/A,Hindi,
Asha Rao,(+91) 9876543210,POL-1,,Rs. 13000,,
"""

# Alias headers, different order, duplicates of POL-1 plus a brand-new lead.
BATCH_002 = """Insured Name,Mobile No,Policy No,Expiry Date,Premium,Language
Asha Rao,91-98765-43210,POL-1,15-01-2027,,
Charu,+919876543212,POL-5,2027-02-01,INR 5000,ta
"""

CALL1_INIT = {"event_id": "e1", "event_type": "call.initiated",
              "workflow_id": "motor_renewal", "call_id": "call_1",
              "occurred_at": "2026-10-01T10:00:00+05:30",
              "phone": "+91 98765 43210", "payload": {}}
CALL1_CONN = {"event_id": "e2", "event_type": "call.connected",
              "workflow_id": "motor_renewal", "call_id": "call_1",
              "occurred_at": "2026-10-01T10:00:20+05:30",
              "phone": "9876543210", "payload": {}}
CALL1_END = {"event_id": "e3", "event_type": "call.ended",
             "workflow_id": "motor_renewal", "call_id": "call_1",
             "occurred_at": "2026-10-01T10:03:20+05:30",
             "phone": "9876543210", "payload": {"duration_sec": 180, "end_reason": "completed"}}
CALL1_SET = {"event_id": "e4", "event_type": "disposition.set",
             "workflow_id": "motor_renewal", "call_id": "call_1",
             "occurred_at": "2026-10-01T10:05:00Z",
             "phone": "91-98765-43210", "payload": {"disposition_code": "renewed"}}
ORPHAN_INIT = {"event_id": "e5", "event_type": "call.initiated",
               "workflow_id": "motor_renewal", "call_id": "call_x",
               "occurred_at": "2026-10-01T11:00:00+05:30",
               "phone": "+91 99999 88888", "payload": {}}
# call_3 belongs to Charu, whose lead only arrives in drop_2.
CALL3_INIT = {"event_id": "e6", "event_type": "call.initiated",
              "workflow_id": "motor_renewal", "call_id": "call_3",
              "occurred_at": "2026-10-03T10:00:00Z",
              "phone": "919876543212", "payload": {}}
CALL2_INIT = {"event_id": "e7", "event_type": "call.initiated",
              "workflow_id": "motor_renewal", "call_id": "call_2",
              "occurred_at": "2026-10-02 09:00:00",  # naive => IST
              "phone": "09876543211", "payload": {}}
CALL2_CONN = {"event_id": "e8", "event_type": "call.connected",
              "workflow_id": "motor_renewal", "call_id": "call_2",
              "occurred_at": "2026-10-02T09:01:00+05:30",
              "phone": "09876543211", "payload": {}}
CALL2_SET_UNKNOWN = {"event_id": "e9", "event_type": "disposition.set",
                     "workflow_id": "motor_renewal", "call_id": "call_2",
                     "occurred_at": "2026-10-02T03:35:00Z",
                     "phone": "09876543211", "payload": {"disposition_code": "RENEWD"}}
# override for call_1 arrives in drop_2 with NO phone; it wins over the set.
CALL1_OVERRIDE = {"event_id": "e10", "event_type": "disposition.override",
                  "workflow_id": "motor_renewal", "call_id": "call_1",
                  "occurred_at": "2026-10-02 20:00:00",
                  "payload": {"disposition_code": "not_interested",
                              "overridden_by": "sup_1", "reason": "qa_review"}}
CALL3_CONN = {"event_id": "e11", "event_type": "call.connected",
              "workflow_id": "motor_renewal", "call_id": "call_3",
              "occurred_at": "2026-10-03T15:30:30+05:30",
              "phone": "+919876543212", "payload": {}}
CALL3_SET = {"event_id": "e12", "event_type": "disposition.set",
             "workflow_id": "motor_renewal", "call_id": "call_3",
             "occurred_at": "2026-10-03T15:35:00+05:30",
             "phone": "+919876543212", "payload": {"disposition_code": "callback_requested"}}


def _jsonl(*events) -> str:
    return "\n".join(json.dumps(e) for e in events) + "\n"


@pytest.fixture()
def mini_data(tmp_path: Path) -> Path:
    root = tmp_path / "data"
    d1 = root / "drop_1"
    d2 = root / "drop_2"
    (d1 / "leads" / "motor_renewal").mkdir(parents=True)
    (d2 / "leads" / "motor_renewal").mkdir(parents=True)
    (d1 / "events").mkdir(parents=True)
    (d2 / "events").mkdir(parents=True)
    (d1 / "leads" / "motor_renewal" / "batch_001.csv").write_text(BATCH_001, encoding="utf-8")
    (d2 / "leads" / "motor_renewal" / "batch_002.csv").write_text(BATCH_002, encoding="utf-8")
    # one malformed line and one exact duplicate line are added inline
    (d1 / "events" / "events_0001.jsonl").write_text(
        _jsonl(CALL1_INIT, CALL1_CONN, CALL1_END, CALL1_SET, ORPHAN_INIT,
               CALL3_INIT, CALL2_INIT, CALL2_CONN, CALL2_SET_UNKNOWN)
        + '{"event_id": "e-dup", "event_type": "call.initiated", "workflow_id": '
          '"motor_renewal", "call_id": "call_1", "occurred_at": '
          '"2026-10-01T10:00:00+05:30", "phone": "+91 98765 43210", "payload": {}}\n'
        + '{"event_id": "e-dup", "event_type": "call.initiated", "workflow_id": '
          '"motor_renewal", "call_id": "call_1", "occurred_at": '
          '"2026-10-01T10:00:00+05:30", "phone": "+91 98765 43210", "payload": {}}\n'
        + "{not valid json at all\n",
        encoding="utf-8",
    )
    (d2 / "events" / "events_1001.jsonl").write_text(
        _jsonl(CALL1_OVERRIDE, CALL3_CONN, CALL3_SET), encoding="utf-8"
    )
    return root


def _run(db_path: Path, *argv: str) -> None:
    assert cli_main(["--db", str(db_path), *argv]) == 0


def test_mini_dataset(mini_data: Path, tmp_path: Path):
    db_path = tmp_path / "pipeline.db"

    # ---- after drop_1: partial state, call_3's event is still an orphan
    _run(db_path, "ingest", str(mini_data / "drop_1"))
    _run(db_path, "report", "--out", str(tmp_path / "r1"))
    q1 = json.loads((tmp_path / "r1" / "data_quality.json").read_text())
    assert q1["orphan_events"] == 2  # unknown phone + call_3 waiting for its lead

    # ---- full ingest
    _run(db_path, "ingest", str(mini_data / "drop_2"))
    _run(db_path, "report", "--out", str(tmp_path / "r2"))
    out = tmp_path / "r2"

    quality = json.loads((out / "data_quality.json").read_text())
    assert quality == {
        "lead_rows_total": 7,
        "lead_rows_rejected": 2,
        "rejections_by_reason": {
            "invalid_phone": 1,
            "missing_required:customer_name": 1,
        },
        "duplicate_lead_rows_merged": 2,
        "field_warnings": 2,
        "events_total": 15,
        "malformed_events": 1,
        "duplicate_events_dropped": 1,
        "orphan_events": 1,  # call_3 linked once its lead arrived
        "unknown_disposition_events": 1,  # "RENEWD" is not in the catalogue
    }

    assert (out / "rejections.csv").read_text().splitlines() == [
        "workflow_id,batch_id,row_number,reason",
        "motor_renewal,batch_001,2,invalid_phone",
        "motor_renewal,batch_001,3,missing_required:customer_name",
    ]

    assert (out / "batch_summary.csv").read_text().splitlines() == [
        "workflow_id,batch_id,total_leads,yet_to_reach_out,"
        "in_progress_telephony_failed,in_progress_dialed,completed",
        "motor_renewal,batch_001,2,0,0,1,1",
        "motor_renewal,batch_002,1,0,0,1,0",
    ]

    assert (out / "daily_overview.csv").read_text().splitlines() == [
        "workflow_id,date,total_calls,connected_calls,connectivity_rate,success_leads",
        "motor_renewal,2026-10-01,1,1,1.0000,0",  # override removed the success
        "motor_renewal,2026-10-02,1,1,1.0000,0",
        "motor_renewal,2026-10-03,1,1,1.0000,0",
    ]

    assert (out / "lead_report.csv").read_text().splitlines() == [
        "workflow_id,external_ref,batch_id,phone_masked,bucket,total_calls,"
        "connected_calls,last_call_at,current_disposition,ever_success",
        "motor_renewal,POL-1,batch_001,+91******3210,completed,1,1,"
        "2026-10-01T10:00:00+05:30,not_interested,false",
        "motor_renewal,POL-4,batch_001,+91******3211,in_progress_dialed,1,1,"
        "2026-10-02T09:00:00+05:30,,false",
        "motor_renewal,POL-5,batch_002,+91******3212,in_progress_dialed,1,1,"
        "2026-10-03T15:30:00+05:30,callback_requested,false",
    ]

    # ---- merged values: last non-null write wins, batch_id stays batch_001
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    lead = conn.execute("SELECT * FROM leads WHERE external_ref = 'POL-1'").fetchone()
    attrs = json.loads(lead["attrs"])
    assert lead["batch_id"] == "batch_001"
    assert attrs["premium_amount"] == "13000.00"   # from the drop_1 duplicate
    assert attrs["policy_expiry"] == "2027-01-15"  # from the drop_2 duplicate
    assert attrs["preferred_language"] == "en"     # normalised from "EN"
    bala = conn.execute("SELECT * FROM leads WHERE external_ref = 'POL-4'").fetchone()
    assert json.loads(bala["attrs"])["preferred_language"] == "en"  # default

    # ---- PII is not stored in plaintext anywhere in the database file
    blob = db_path.read_bytes()
    assert b"Asha" not in blob
    assert b"9876543210" not in blob

    # ---- idempotency: re-ingesting both drops changes nothing
    _run(db_path, "ingest", str(mini_data / "drop_1"))
    _run(db_path, "ingest", str(mini_data / "drop_2"))
    _run(db_path, "report", "--out", str(tmp_path / "r3"))
    for name in ["batch_summary.csv", "daily_overview.csv", "lead_report.csv",
                 "rejections.csv", "data_quality.json"]:
        assert (tmp_path / "r3" / name).read_bytes() == (out / name).read_bytes(), name

    # ---- ingest-order independence: one-shot ingest of the parent dir
    db2 = tmp_path / "pipeline2.db"
    _run(db2, "ingest", str(mini_data))
    _run(db2, "report", "--out", str(tmp_path / "r4"))
    for name in ["batch_summary.csv", "daily_overview.csv", "lead_report.csv",
                 "rejections.csv", "data_quality.json"]:
        assert (tmp_path / "r4" / name).read_bytes() == (out / name).read_bytes(), name
