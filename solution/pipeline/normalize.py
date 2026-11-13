"""Value normalisation and validation.

Every parser returns ``None`` for an *invalid* value. Callers decide whether
the raw value was *missing* (empty after trimming) or *present but invalid*,
because the spec treats the two cases differently.

All parsers are pure functions so they are trivial to unit-test.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Optional

# India Standard Time: naive event timestamps mean IST, and every date shown
# in the reports is an IST date.
IST = timezone(timedelta(hours=5, minutes=30), name="IST")

_HEADER_SEPARATORS = re.compile(r"[_\s]+")
_NON_DIGITS = re.compile(r"\D+")
_AMOUNT_PREFIX = re.compile(r"^\s*(?:₹|rs\.?|inr)\s*", re.IGNORECASE)

_DATE_FORMATS = (
    "%Y-%m-%d",  # 2026-10-05
    "%d/%m/%Y",  # 05/10/2026 (day-first)
    "%d-%m-%Y",  # 05-10-2026 (day-first)
    "%d-%b-%Y",  # 05-Oct-2026 (month name, case-insensitive)
)


def normalize_header(header: str) -> str:
    """Normalise a CSV header for matching: lower-case, trim, and collapse
    every run of underscores or whitespace into a single space."""
    return _HEADER_SEPARATORS.sub(" ", header.strip().lower())


def parse_phone(raw: str) -> Optional[str]:
    """Normalise an Indian mobile number to ``+91XXXXXXXXXX``.

    Strip every non-digit; drop a leading ``91`` (12 digits) or ``0``
    (11 digits); the result is valid only when it is exactly 10 digits
    starting with 6, 7, 8 or 9. Returns None when invalid.
    """
    digits = _NON_DIGITS.sub("", raw)
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    if len(digits) == 10 and digits[0] in "6789":
        return "+91" + digits
    return None


def parse_date(raw: str) -> Optional[date]:
    """Parse a date in any accepted format. Impossible dates are invalid."""
    s = raw.strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def parse_decimal(raw: str) -> Optional[Decimal]:
    """Parse a non-negative money amount, rounded to 2 places.

    Accepts an optional ``₹`` / ``Rs`` / ``Rs.`` / ``INR`` prefix
    (case-insensitive) and comma thousands separators.
    """
    s = _AMOUNT_PREFIX.sub("", raw.strip()).replace(",", "")
    if not s:
        return None
    try:
        amount = Decimal(s)
    except InvalidOperation:
        return None
    if amount < 0:
        return None
    return amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def parse_enum(raw: str, values: list[str]) -> Optional[str]:
    """Match case-insensitively against the allowed values; store lower-case."""
    candidate = raw.strip().lower()
    lowered = [v.lower() for v in values]
    return candidate if candidate in lowered else None


def parse_timestamp(raw: str) -> Optional[datetime]:
    """Parse an event timestamp into an aware UTC datetime.

    Accepted forms: UTC (``...Z``), with offset (``...+05:30``) and naive
    (``2026-10-03 10:01:00``). Naive means IST.
    """
    s = raw.strip()
    try:
        if s.endswith("Z"):
            dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        elif "T" in s:
            dt = datetime.fromisoformat(s)
        else:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=IST)
    except ValueError:
        return None
    return dt.astimezone(timezone.utc)
