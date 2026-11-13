"""Unit tests for value normalisation - the rules most likely to break."""
from datetime import timezone

from solution.pipeline.normalize import (
    IST,
    normalize_header,
    parse_date,
    parse_decimal,
    parse_enum,
    parse_phone,
    parse_timestamp,
)


class TestNormalizeHeader:
    def test_lowercase_trim(self):
        assert normalize_header("  Mobile No ") == "mobile no"

    def test_underscores_become_spaces(self):
        assert normalize_header("policy_number") == "policy number"

    def test_runs_collapse(self):
        assert normalize_header("Customer__  Name") == "customer name"


class TestParsePhone:
    def test_plain_10_digits(self):
        assert parse_phone("9876543210") == "+919876543210"

    def test_plus91_with_spaces(self):
        assert parse_phone("+91 98765 43210") == "+919876543210"

    def test_leading_zero(self):
        assert parse_phone("09876543210") == "+919876543210"

    def test_91_with_dashes(self):
        assert parse_phone("91-98765-43210") == "+919876543210"

    def test_parenthesised_country_code(self):
        assert parse_phone("(+91) 9876543210") == "+919876543210"

    def test_plus91_no_space(self):
        assert parse_phone("+919876543210") == "+919876543210"

    def test_nine_digits_invalid(self):
        assert parse_phone("987654321") is None

    def test_bad_prefix_invalid(self):
        assert parse_phone("5876543210") is None

    def test_landline_invalid(self):
        # 022-2000-1234 -> strip 0 -> 10 digits starting with 2 -> invalid
        assert parse_phone("022-2000-1234") is None

    def test_us_number_invalid(self):
        assert parse_phone("+1 415 555 0134") is None

    def test_text_invalid(self):
        assert parse_phone("N/A") is None


class TestParseDate:
    def test_iso(self):
        assert parse_date("2026-12-01").isoformat() == "2026-12-01"

    def test_day_first_slash(self):
        assert parse_date("01/12/2026").isoformat() == "2026-12-01"

    def test_day_first_dash(self):
        assert parse_date("05-10-2026").isoformat() == "2026-10-05"

    def test_month_name(self):
        assert parse_date("01-Dec-2026").isoformat() == "2026-12-01"

    def test_month_name_any_case(self):
        assert parse_date("01-dEc-2026").isoformat() == "2026-12-01"

    def test_impossible_date_invalid(self):
        assert parse_date("31/02/2026") is None

    def test_month_13_invalid(self):
        assert parse_date("2026-13-01") is None

    def test_text_invalid(self):
        assert parse_date("soon") is None
        assert parse_date("TBD") is None


class TestParseDecimal:
    def test_plain(self):
        assert str(parse_decimal("12345.67")) == "12345.67"

    def test_integer_rounds_to_2dp(self):
        assert str(parse_decimal("8000")) == "8000.00"

    def test_rupee_prefix_with_commas(self):
        assert str(parse_decimal("₹12,345")) == "12345.00"

    def test_rs_dot_prefix(self):
        assert str(parse_decimal("Rs. 4500")) == "4500.00"

    def test_inr_prefix_case_insensitive(self):
        assert str(parse_decimal("inr 4500.5")) == "4500.50"

    def test_negative_invalid(self):
        assert parse_decimal("-100") is None

    def test_text_invalid(self):
        assert parse_decimal("twelve thousand") is None
        assert parse_decimal("N/A") is None
        assert parse_decimal("-") is None


class TestParseEnum:
    VALUES = ["en", "hi", "ta"]

    def test_exact(self):
        assert parse_enum("hi", self.VALUES) == "hi"

    def test_case_insensitive_stored_lower(self):
        assert parse_enum("EN", self.VALUES) == "en"

    def test_surrounding_space(self):
        assert parse_enum(" ta ", self.VALUES) == "ta"

    def test_full_name_invalid(self):
        assert parse_enum("Hindi", self.VALUES) is None
        assert parse_enum("english", self.VALUES) is None


class TestParseTimestamp:
    def test_utc_z(self):
        dt = parse_timestamp("2026-10-05T11:12:49Z")
        assert dt.tzinfo == timezone.utc
        assert dt.isoformat() == "2026-10-05T11:12:49+00:00"

    def test_offset(self):
        dt = parse_timestamp("2026-10-05T16:42:37+05:30")
        assert dt.isoformat() == "2026-10-05T11:12:37+00:00"

    def test_naive_means_ist(self):
        dt = parse_timestamp("2026-10-05 16:42:37")
        assert dt.isoformat() == "2026-10-05T11:12:37+00:00"

    def test_all_three_forms_agree(self):
        a = parse_timestamp("2026-10-05T11:12:49Z")
        b = parse_timestamp("2026-10-05T16:42:49+05:30")
        c = parse_timestamp("2026-10-05 16:42:49")
        assert a == b == c

    def test_ist_date_conversion(self):
        # 23:30 UTC is already the next day in IST
        dt = parse_timestamp("2026-10-05T23:30:00Z")
        assert dt.astimezone(IST).date().isoformat() == "2026-10-06"

    def test_garbage_invalid(self):
        assert parse_timestamp("not a timestamp") is None
