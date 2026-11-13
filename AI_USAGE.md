# AI Usage Disclosure

> _TODO before submitting: read this and adjust anything that doesn't match
> what you actually did. You must be able to defend every line of the code
> in the live walkthrough - read it, run it, change something small._

## Tools used
Claude (Anthropic), via an AI assistant, plus the usual docs.

## What I used them for
- Scaffolding the package layout (CLI, ingest, report modules) and the first
  draft of most functions.
- First draft of the normalisation rules and the unit tests for them.
- Rubber-ducking the tricky spec points: orphan relinking, override linking
  without a phone, last-non-null-wins merging.

## What I wrote or decided myself
- The assignment choice (A) and the storage layout: one leads table keyed by
  HMAC of the phone, an append-only events ledger, per-file ingest registry
  for idempotency.
- Every judgement call in the `SUBMISSION.md` decisions table - I checked
  each one against the spec and the generator's actual output.
- Reviewing and correcting the merge/default ordering and the
  orphan-count semantics ("final state").

## Where the AI was wrong or unhelpful
- It initially classified unknown dispositions at ingest time, which would
  have double-counted an event that was orphaned first and linked later.
  I caught it by tracing the spec's processing order and switched to
  computing the count from final ledger state.
- Its first effective-disposition draft preferred the latest *set* over an
  *override* when the set was newer; the spec says any override wins. Found
  by a unit test.

## How I verified AI-generated code
- 54 automated tests, including a hand-computed end-to-end dataset where I
  knew the exact expected rows of all five report files.
- Ran the real generator at both scales; reconciled report totals against
  direct SQL counts on the ledger; scanned the raw DB file bytes to confirm
  no plaintext PII.
- Re-ran ingest to confirm byte-identical outputs (idempotency).
