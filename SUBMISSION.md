# Submission

## Candidate
- **Name:** Jatin Jain
- **Assignment chosen:** A (Data) - Interaction Ledger & Reporting Pipeline
- **Time spent (honest estimate):** _TODO: fill in honestly before submitting_

## How to run
```bash
python -m venv .venv && source .venv/bin/activate   # Python 3.11+
pip install -r requirements.txt

# generate data (any seed/scale; output is gitignored)
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data

python -m solution.pipeline ingest data/drop_1
python -m solution.pipeline ingest data/drop_2
python -m solution.pipeline report --out reports/

python -m pytest solution/tests     # 54 tests
```
State lives in one SQLite file (default `./pipeline.db`, override with
`--db`). No database server needed.

## Design

**Storage (SQLite).** Four working tables plus bookkeeping:

- `leads` - one row per lead identity `(workflow_id, phone_key)`. `phone_key`
  is a one-way HMAC-SHA256 of the canonical phone, which is what makes
  event-to-lead matching possible without storing the number. All PII
  (name, full phone) sits in one Fernet-encrypted JSON blob per row; the
  last four digits are kept separately because the report contract publishes
  them anyway. Non-PII field values go in a plain `attrs` JSON column -
  schemas differ per workflow, so wide typed columns would not generalise.
- `events` - the append-only interaction ledger: one row per unique
  `event_id`, including orphans (`lead_id IS NULL`). Nothing is ever edited;
  the only update an event ever gets is an orphan being linked to its lead
  later. Overrides are new rows, never mutations.
- `rejections`, `counters`, `reason_counters`, `ingested_files` - the last
  one is the idempotency key: each file is ingested in a single transaction
  and recorded at commit, so re-ingesting a drop is a no-op and a crash
  mid-file rolls back cleanly.

**Ingestion.** Leads are validated field-by-field in schema order (first
failure is the rejection reason), merged on identity with last-non-null-wins,
and cached in memory per run. Events are classified in the spec's order
(malformed -> duplicate -> orphan -> unknown disposition). Orphans stay in
the ledger; after every drop a relink pass (phone-based, then
call-based for overrides, repeated to fixpoint) attaches everything that
became linkable - so an event that arrived before its lead still counts.

**Reporting.** Recomputed from the ledger on every run, so reports always
reflect everything ingested so far. Derived facts (effective disposition,
buckets) are pure functions in `derive.py`, which is where the unit tests
aim.

## Decisions & assumptions

| Decision | Why | Alternative considered |
|---|---|---|
| SQLite, not Postgres | Zero setup, fast enough (631k events ingested in ~38s); one file = one pipeline state | Postgres via the provided compose file (production choice) |
| HMAC for the phone join key + Fernet for reversible PII | Matching needs determinism, which encryption-with-random-IV can't give; names never appear in reports, so strong one-way protection would also do - but a real ops team needs names back | Encrypt everything + separate blind index per field (more moving parts, same guarantee) |
| Rejections carry only the spec's two reasons; a present-but-invalid *required non-phone* field is treated as missing | Current schemas have no such case (required = string or phone), so the spec is silent | Invent a third reason string - riskier against an expected-output diff |
| Unknown dispositions are stored in the ledger but ignored in derivations; they count as `unknown_disposition_events` only when linked (an orphan with an unknown code counts as orphan) | Matches the spec's processing order and keeps the ledger complete; counts reflect final state | Delete them at ingest (loses auditability) |
| `orphan_events` = events still unlinked at report time | Spec: "orphan counts reflect the final state" | Count every event that was ever orphan (double-counts after relink) |
| Reports are fully recomputed per run; only ingestion is incremental | Correctness first; recompute is 4s on the large dataset | Materialised incremental aggregates (the stretch I skipped) |
| Events with unusable timestamps count as malformed | Spec's malformed definition doesn't mention them; the generator never produces them | Store with NULL time - they would silently corrupt "latest" logic |
| Two `call.initiated` events for one `call_id`: earliest wins | Should not happen; keeps the call date stable | First-seen wins (order-dependent) |
| Defaults applied when a lead is flushed, not at file end | Equivalent: later rows only overwrite nulls with non-nulls | Apply in a separate pass at the end of each drop |
| `rejections_by_reason` lists only reasons that occurred | Reads most naturally as a count of what happened | Emit all known reasons with zeros |

## Failure modes considered

- **Re-running ingest on an ingested drop:** skipped via `ingested_files`;
  verified byte-identical reports.
- **Crash mid-file:** per-file transaction rolls back; file is re-ingested
  next run.
- **Events before their lead (earlier drop):** stored as orphans, relinked
  by the post-drop pass; verified by counts dropping 1123 -> 462 on the
  sample data.
- **Override arriving before the rest of its call (shuffle):** orphan until
  any sibling event links, then resolved by the call-based relink pass.
- **Duplicate delivery within and across drops:** `event_id` seen-set.
- **Same phone in two workflows:** identity is per `(workflow_id, phone)`.
- **PII leakage:** leads table holds no plaintext name/phone (verified by
  scanning the raw DB file); `rejections.csv` carries no PII by contract.
- **Not handled:** concurrent ingest processes (SQLite single-writer), a
  phone that changes hands between people, schema changes mid-stream.

## Testing

54 pytest tests, one command (`python -m pytest solution/tests`):

- **Unit:** phone/date/decimal/enum/header/timestamp rules; effective
  disposition (override beats set, latest wins, unknown codes ignored);
  bucket logic (all four buckets + precedence).
- **End-to-end:** a hand-built mini dataset where every expected output -
  all five files, exact rows - was computed by hand, including blank-row
  numbering, alias headers, merge semantics, an orphan that links later, and
  an override with no phone. Re-ingest byte-identical (idempotency) and
  drop-at-once vs sequential equivalence are asserted too.
- **Not tested:** Postgres, datasets beyond ~630k events, adversarial
  encodings (UTF-8 BOM, quoted commas in names - csv module handles them,
  but there is no dedicated test).

## Results

Seed 7, `--scale large` (~144k lead rows, ~632k event lines):

- `ingest data/drop_1 + drop_2`: **37.9s**; `report`: **3.8s** (both drops
  then one report - comfortably inside the 2-minute stretch bar).
- Output: 144k lead rows, 6,941 rejected, 3,956 duplicates merged, 33,840
  final orphans, 18,176 duplicate events dropped.
- Re-ingesting everything changed nothing (byte-identical reports).
- Surprising in the data: ~1k events arrive before their lead does
  (drop_1-only run had 1,123 orphans vs 462 after drop_2) - without the
  relink pass, ~1.5% of calls would silently vanish from client reports.

## If I had another day

1. Property-based tests: run the generator across many random seeds and
   assert report/ledger invariants (totals reconcile, no PII, stable counts).
2. Incremental/materialised aggregation if recompute ever gets too slow.
3. Postgres option behind a small storage interface, so the same pipeline
   scales to a server DB without code changes.
4. A `quarantine` review view (rejections + orphans) for ops teams.
5. Production note: scheduling (one ingestion job per drop, retries with the
   same DB), alerting on orphan/rejection spikes, backfills = just re-ingest
   the drop on a fresh DB and diff.
