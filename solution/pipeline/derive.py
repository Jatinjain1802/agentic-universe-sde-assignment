"""Derived facts: calls, effective dispositions and lead buckets.

These are pure functions over plain data structures so the rules most likely
to break (effective disposition, bucket logic) can be unit-tested without a
database.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from .schema import Disposition

BUCKET_YET_TO_REACH_OUT = "yet_to_reach_out"
BUCKET_COMPLETED = "completed"
BUCKET_TELEPHONY_FAILED = "in_progress_telephony_failed"
BUCKET_DIALED = "in_progress_dialed"


@dataclass
class CallFacts:
    """Everything the ledger knows about one call_id."""

    call_id: str
    lead_id: int
    workflow_id: str
    initiated_at: datetime | None = None
    connected: bool = False
    telephony_failed: bool = False
    # (occurred_at, code, event_id) - event_id is the deterministic tie-break.
    sets: list[tuple[datetime, str, str]] = field(default_factory=list)
    overrides: list[tuple[datetime, str, str]] = field(default_factory=list)

    @property
    def is_call(self) -> bool:
        """A call is counted only if its call.initiated event is present."""
        return self.initiated_at is not None


def effective_disposition(call: CallFacts, known_codes: set[str]) -> str | None:
    """Latest known override by occurred_at; else latest known set; else None.

    Codes not present in the workflow's disposition catalogue are ignored.
    The original events are never edited - the override simply wins here.
    """
    candidates = [entry for entry in call.overrides if entry[1] in known_codes]
    if not candidates:
        candidates = [entry for entry in call.sets if entry[1] in known_codes]
    if not candidates:
        return None
    return max(candidates, key=lambda entry: (entry[0], entry[2]))[1]


def lead_bucket(calls: list[CallFacts], catalogue: dict[str, Disposition]) -> str:
    """First matching bucket wins (spec order)."""
    if not calls:
        return BUCKET_YET_TO_REACH_OUT
    known = set(catalogue)
    for call in calls:
        eff = effective_disposition(call, known)
        if eff is not None and catalogue[eff].is_terminal:
            return BUCKET_COMPLETED
    if all(call.telephony_failed for call in calls):
        return BUCKET_TELEPHONY_FAILED
    return BUCKET_DIALED
