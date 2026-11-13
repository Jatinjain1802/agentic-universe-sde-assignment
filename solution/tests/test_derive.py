"""Unit tests for effective dispositions and lead buckets."""
from datetime import datetime, timezone

from solution.pipeline.derive import CallFacts, effective_disposition, lead_bucket
from solution.pipeline.schema import Disposition

T0 = datetime(2026, 10, 1, 10, 0, tzinfo=timezone.utc)
T1 = datetime(2026, 10, 1, 11, 0, tzinfo=timezone.utc)
T2 = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)

CATALOGUE = {
    "renewed": Disposition("renewed", "Renewed", is_terminal=True, is_success=True),
    "not_interested": Disposition("not_interested", "No", is_terminal=True, is_success=False),
    "callback_requested": Disposition(
        "callback_requested", "Later", is_terminal=False, is_success=False
    ),
}
KNOWN = set(CATALOGUE)


def make_call(**kwargs) -> CallFacts:
    kwargs.setdefault("call_id", "c1")
    return CallFacts(lead_id=1, workflow_id="motor_renewal", **kwargs)


class TestEffectiveDisposition:
    def test_no_disposition(self):
        assert effective_disposition(make_call(), KNOWN) is None

    def test_single_set(self):
        call = make_call(sets=[(T0, "renewed", "e1")])
        assert effective_disposition(call, KNOWN) == "renewed"

    def test_latest_set_wins(self):
        call = make_call(sets=[(T1, "renewed", "e2"), (T0, "callback_requested", "e1")])
        assert effective_disposition(call, KNOWN) == "renewed"

    def test_override_beats_set_even_when_set_is_later(self):
        call = make_call(
            sets=[(T2, "callback_requested", "e3")],
            overrides=[(T1, "not_interested", "e2")],
        )
        assert effective_disposition(call, KNOWN) == "not_interested"

    def test_latest_override_wins(self):
        call = make_call(
            overrides=[(T0, "not_interested", "e1"), (T1, "renewed", "e2")],
        )
        assert effective_disposition(call, KNOWN) == "renewed"

    def test_unknown_code_ignored(self):
        call = make_call(sets=[(T0, "RENEWD", "e1")])
        assert effective_disposition(call, KNOWN) is None

    def test_unknown_override_falls_back_to_set(self):
        call = make_call(
            sets=[(T0, "renewed", "e1")],
            overrides=[(T1, "call_back", "e2")],
        )
        assert effective_disposition(call, KNOWN) == "renewed"

    def test_tie_break_on_event_id(self):
        call = make_call(sets=[(T0, "renewed", "e2"), (T0, "not_interested", "e1")])
        assert effective_disposition(call, KNOWN) == "renewed"


class TestLeadBucket:
    def test_no_calls(self):
        assert lead_bucket([], CATALOGUE) == "yet_to_reach_out"

    def test_any_terminal_disposition_completes(self):
        ringing = make_call(call_id="c1", initiated_at=T0)
        done = make_call(
            call_id="c2", initiated_at=T1, connected=True, sets=[(T1, "renewed", "e1")]
        )
        assert lead_bucket([ringing, done], CATALOGUE) == "completed"

    def test_all_telephony_failed(self):
        c1 = make_call(call_id="c1", initiated_at=T0, telephony_failed=True)
        c2 = make_call(call_id="c2", initiated_at=T1, telephony_failed=True)
        assert lead_bucket([c1, c2], CATALOGUE) == "in_progress_telephony_failed"

    def test_mixed_calls_are_dialed(self):
        c1 = make_call(call_id="c1", initiated_at=T0, telephony_failed=True)
        c2 = make_call(call_id="c2", initiated_at=T1, connected=True)
        assert lead_bucket([c1, c2], CATALOGUE) == "in_progress_dialed"

    def test_non_terminal_disposition_still_dialed(self):
        call = make_call(
            call_id="c1", initiated_at=T0, connected=True,
            sets=[(T0, "callback_requested", "e1")],
        )
        assert lead_bucket([call], CATALOGUE) == "in_progress_dialed"

    def test_terminal_via_override(self):
        call = make_call(
            call_id="c1", initiated_at=T0, connected=True,
            sets=[(T0, "callback_requested", "e1")],
            overrides=[(T1, "not_interested", "e2")],
        )
        assert lead_bucket([call], CATALOGUE) == "completed"
