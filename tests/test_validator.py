"""
Tests for src/manual_excel_loader/validator.py

Run with:
    pytest tests/test_validator.py -v
"""
from __future__ import annotations

from datetime import datetime

import pytest

from manual_excel_loader.enums import DatabaseType
from manual_excel_loader.exceptions import UnsupportedDataTypeError
from manual_excel_loader.result import Err, Ok
from manual_excel_loader.validator import (
    get_validator,
    validate_boolean_gp,
    validate_datetime,
    validate_decimal,
    validate_float,
    validate_integer,
    validate_interval,
    validate_string,
    validate_uuid,
)

GP = DatabaseType.GREENPLUM
CH = DatabaseType.CLICKHOUSE


# ── validate_integer ──────────────────────────────────────────────────────────

class TestValidateInteger:

    def test_valid_positive(self):
        assert validate_integer("42", -100, 100) == Ok(42)

    def test_valid_negative(self):
        assert validate_integer("-10", -100, 100) == Ok(-10)

    def test_zero_is_ok_not_falsy(self):
        # Ok(0) must not be treated as falsy
        assert validate_integer("0", -10, 10) == Ok(0)

    def test_upper_boundary_inclusive(self):
        assert validate_integer("32767", -32768, 32767) == Ok(32767)

    def test_lower_boundary_inclusive(self):
        assert validate_integer("-32768", -32768, 32767) == Ok(-32768)

    def test_above_upper_returns_err(self):
        result = validate_integer("32768", -32768, 32767)
        assert isinstance(result, Err)
        assert "32768" in result.message

    def test_below_lower_returns_err(self):
        assert isinstance(validate_integer("-32769", -32768, 32767), Err)

    def test_non_numeric_string_returns_err(self):
        assert isinstance(validate_integer("abc", -100, 100), Err)

    def test_float_string_returns_err(self):
        # int("3.14") raises ValueError — user must not pass floats to int columns
        assert isinstance(validate_integer("3.14", 0, 100), Err)

    def test_empty_string_returns_err(self):
        assert isinstance(validate_integer("", -100, 100), Err)

    def test_none_returns_err(self):
        assert isinstance(validate_integer(None, -100, 100), Err)

    def test_native_int_accepted(self):
        # openpyxl may already return int from a cell
        assert validate_integer(42, -100, 100) == Ok(42)

    def test_result_value_is_int_not_string(self):
        result = validate_integer("7", 0, 100)
        assert isinstance(result, Ok)
        assert type(result.value) is int


# ── validate_float ────────────────────────────────────────────────────────────

class TestValidateFloat:

    def test_valid_float_string(self):
        result = validate_float("3.14", -1e10, 1e10)
        assert isinstance(result, Ok)
        assert abs(result.value - 3.14) < 1e-9

    def test_integer_string_accepted(self):
        assert validate_float("5", -100.0, 100.0) == Ok(5.0)

    def test_zero_is_ok_not_falsy(self):
        assert validate_float("0.0", -1.0, 1.0) == Ok(0.0)

    def test_out_of_range_returns_err(self):
        assert isinstance(validate_float("1e400", -3.4e38, 3.4e38), Err)

    def test_non_numeric_returns_err(self):
        assert isinstance(validate_float("hello", -100.0, 100.0), Err)

    def test_none_returns_err(self):
        assert isinstance(validate_float(None, -100.0, 100.0), Err)

    def test_result_value_is_float(self):
        result = validate_float("2.5", -10.0, 10.0)
        assert isinstance(result, Ok)
        assert type(result.value) is float


# ── validate_decimal ──────────────────────────────────────────────────────────

class TestValidateDecimal:

    def test_valid_decimal(self):
        result = validate_decimal("3.14", precision=10, scale=4)
        assert isinstance(result, Ok)

    def test_comma_separator_returns_err(self):
        result = validate_decimal("3,14", precision=10, scale=4)
        assert isinstance(result, Err)
        assert "dot" in result.message or "comma" in result.message

    def test_precision_exceeded_returns_err(self):
        # "123456789.12" has 11 digits total, precision=5
        result = validate_decimal("123456789.12", precision=5, scale=2)
        assert isinstance(result, Err)

    def test_fractional_exceeds_scale_returns_err(self):
        # 3.14159265 имеет 8 знаков после запятой — не влезает в scale=2 без потери данных
        result = validate_decimal("3.14159265", precision=32, scale=2)
        assert isinstance(result, Err)

    def test_value_within_scale_ok(self):
        result = validate_decimal("3.14", precision=32, scale=2)
        assert result == Ok(3.14)

    def test_non_numeric_returns_err(self):
        assert isinstance(validate_decimal("abc", 10, 2), Err)

    def test_zero_is_valid(self):
        assert isinstance(validate_decimal("0", 10, 2), Ok)

    def test_negative_value_accepted(self):
        result = validate_decimal("-99.99", precision=10, scale=2)
        assert isinstance(result, Ok)

    def test_scientific_notation_large_int_returns_err(self):
        # 1e10 = 10_000_000_000 — 11 целых цифр, не влезает в decimal(5, 2)
        result = validate_decimal("1e10", precision=5, scale=2)
        assert isinstance(result, Err)

    def test_scientific_notation_within_precision_ok(self):
        # 1e2 = 100 — 3 целых цифры, влезает в decimal(10, 2)
        result = validate_decimal("1e2", precision=10, scale=2)
        assert isinstance(result, Ok)

    def test_infinity_returns_err(self):
        assert isinstance(validate_decimal("inf", 32, 8), Err)

    def test_nan_returns_err(self):
        assert isinstance(validate_decimal("nan", 32, 8), Err)

    def test_negative_inf_returns_err(self):
        assert isinstance(validate_decimal("-inf", 32, 8), Err)


# ── validate_string ───────────────────────────────────────────────────────────

class TestValidateString:

    def test_regular_string(self):
        assert validate_string("hello") == Ok("hello")

    def test_integer_coerced_to_string(self):
        assert validate_string(42) == Ok("42")

    def test_empty_string_ok(self):
        assert validate_string("") == Ok("")

    def test_none_coerced_to_string(self):
        # validate_string never fails — None handling is the caller's job
        result = validate_string(None)
        assert isinstance(result, Ok)
        assert result.value == "None"


# ── validate_datetime ─────────────────────────────────────────────────────────

class TestValidateDatetime:

    def test_iso_date_string(self):
        result = validate_datetime(
            "2024-01-15",
            datetime(1, 1, 1),
            datetime(9999, 12, 31),
            fmt="%Y-%m-%d",
        )
        assert result == Ok("2024-01-15")

    def test_datetime_string_with_time(self):
        result = validate_datetime(
            "2024-01-15 10:30:00",
            datetime(1, 1, 1),
            datetime(9999, 12, 31, 23, 59, 59),
        )
        assert isinstance(result, Ok)
        assert result.value == "2024-01-15 10:30:00"

    def test_datetime_object_accepted(self):
        result = validate_datetime(
            datetime(2024, 6, 1, 12, 0, 0),
            datetime(1, 1, 1),
            datetime(9999, 12, 31, 23, 59, 59),
        )
        assert isinstance(result, Ok)

    def test_out_of_range_returns_err(self):
        # CH Date max is 2149-06-06
        result = validate_datetime(
            "2200-01-01",
            datetime(1970, 1, 1),
            datetime(2149, 6, 6),
            fmt="%Y-%m-%d",
        )
        assert isinstance(result, Err)
        assert "range" in result.message

    def test_unparseable_string_returns_err(self):
        result = validate_datetime(
            "not-a-date",
            datetime(1, 1, 1),
            datetime(9999, 12, 31),
        )
        assert isinstance(result, Err)

    def test_boundary_date_included(self):
        # Exactly at CH Date upper boundary
        result = validate_datetime(
            "2149-06-06",
            datetime(1970, 1, 1),
            datetime(2149, 6, 6),
            fmt="%Y-%m-%d",
        )
        assert isinstance(result, Ok)


# ── validate_boolean_gp ───────────────────────────────────────────────────────

class TestValidateBooleanGp:

    @pytest.mark.parametrize("value", [
        "true", "false", "True", "FALSE", "TRUE",
        "1", "0",
        "t", "f",
        "y", "n",
        "yes", "no",
        "on", "off",
        "null",
    ])
    def test_all_valid_values(self, value):
        assert isinstance(validate_boolean_gp(value), Ok)

    def test_invalid_value_returns_err(self):
        result = validate_boolean_gp("maybe")
        assert isinstance(result, Err)
        assert "allowed" in result.message

    def test_empty_string_returns_err(self):
        assert isinstance(validate_boolean_gp(""), Err)

    def test_result_is_normalised_to_lowercase(self):
        result = validate_boolean_gp("TRUE")
        assert isinstance(result, Ok)
        assert result.value == "true"

    def test_whitespace_stripped(self):
        result = validate_boolean_gp("  yes  ")
        assert isinstance(result, Ok)


# ── validate_uuid ─────────────────────────────────────────────────────────────

class TestValidateUuid:

    VALID = "123e4567-e89b-12d3-a456-426614174000"

    def test_valid_lowercase_uuid(self):
        assert validate_uuid(self.VALID) == Ok(self.VALID)

    def test_valid_uppercase_uuid(self):
        assert isinstance(validate_uuid(self.VALID.upper()), Ok)

    def test_missing_hyphens_returns_err(self):
        assert isinstance(validate_uuid("123e4567e89b12d3a456426614174000"), Err)

    def test_too_short_returns_err(self):
        assert isinstance(validate_uuid("123e4567-e89b-12d3-a456"), Err)

    def test_invalid_characters_returns_err(self):
        assert isinstance(validate_uuid("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"), Err)

    def test_empty_string_returns_err(self):
        assert isinstance(validate_uuid(""), Err)


# ── validate_interval ─────────────────────────────────────────────────────────

class TestValidateInterval:

    @pytest.mark.parametrize("value", [
        "1 year",
        "2 months",
        "3 days",
        "1 year 2 months",
        "5 hours 30 minutes",
        "-1 day",
    ])
    def test_valid_intervals(self, value):
        assert isinstance(validate_interval(value), Ok)

    def test_invalid_format_returns_err(self):
        assert isinstance(validate_interval("forever"), Err)

    def test_empty_string_returns_err(self):
        assert isinstance(validate_interval(""), Err)


# ── get_validator — GreenPlum ─────────────────────────────────────────────────

class TestGetValidatorGP:

    def test_returns_callable(self):
        assert callable(get_validator("integer", GP))

    def test_smallint_valid(self):
        assert isinstance(get_validator("smallint", GP)("100"), Ok)

    def test_smallint_overflow(self):
        assert isinstance(get_validator("smallint", GP)("99999"), Err)

    def test_integer_max(self):
        assert get_validator("integer", GP)("2147483647") == Ok(2147483647)

    def test_bigint(self):
        assert isinstance(get_validator("bigint", GP)("9223372036854775807"), Ok)

    def test_real(self):
        assert isinstance(get_validator("real", GP)("1.5"), Ok)

    def test_double_precision(self):
        assert isinstance(get_validator("double precision", GP)("1.23456789"), Ok)

    def test_text(self):
        assert get_validator("text", GP)("anything") == Ok("anything")

    def test_boolean_true(self):
        assert isinstance(get_validator("boolean", GP)("true"), Ok)

    def test_boolean_invalid(self):
        assert isinstance(get_validator("boolean", GP)("maybe"), Err)

    def test_date(self):
        assert isinstance(get_validator("date", GP)("2024-06-01"), Ok)

    def test_time(self):
        assert isinstance(get_validator("time", GP)("14:30:00"), Ok)

    def test_timestamp(self):
        assert isinstance(get_validator("timestamp", GP)("2024-06-01 12:00:00"), Ok)

    def test_timestamp_without_time_zone(self):
        assert isinstance(
            get_validator("timestamp without time zone", GP)("2024-01-01 00:00:00"), Ok
        )

    def test_interval_valid(self):
        assert isinstance(get_validator("interval", GP)("3 days"), Ok)

    def test_tsrange_accepted_as_string(self):
        assert isinstance(get_validator("tsrange", GP)("[2020-01-01,2021-01-01)"), Ok)

    def test_uuid(self):
        assert isinstance(
            get_validator("uuid", GP)("123e4567-e89b-12d3-a456-426614174000"), Ok
        )

    def test_decimal_with_params(self):
        v = get_validator("decimal(10,2)", GP)
        assert isinstance(v("3.14"), Ok)
        assert isinstance(v("abc"), Err)

    def test_numeric_with_params(self):
        v = get_validator("numeric(18,4)", GP)
        assert isinstance(v("12345.6789"), Ok)

    def test_varchar_within_limit(self):
        assert isinstance(get_validator("varchar(5)", GP)("hello"), Ok)

    def test_varchar_exceeds_limit(self):
        assert isinstance(get_validator("varchar(5)", GP)("toolong"), Err)

    def test_character_varying_alias(self):
        assert isinstance(get_validator("character varying(10)", GP)("hi"), Ok)

    def test_char_exact_length(self):
        assert isinstance(get_validator("char(3)", GP)("abc"), Ok)

    def test_char_wrong_length_short(self):
        assert isinstance(get_validator("char(3)", GP)("ab"), Err)

    def test_char_wrong_length_long(self):
        assert isinstance(get_validator("char(3)", GP)("abcd"), Err)

    def test_unknown_type_raises(self):
        with pytest.raises(UnsupportedDataTypeError):
            get_validator("jsonb", GP)

    def test_type_name_case_insensitive(self):
        assert isinstance(get_validator("INTEGER", GP)("1"), Ok)

    def test_type_name_strips_whitespace(self):
        assert isinstance(get_validator("  integer  ", GP)("1"), Ok)


# ── get_validator — ClickHouse ────────────────────────────────────────────────

class TestGetValidatorCH:

    def test_int8_valid(self):
        assert get_validator("int8", CH)("127") == Ok(127)

    def test_int8_overflow(self):
        assert isinstance(get_validator("int8", CH)("128"), Err)

    def test_int32(self):
        assert isinstance(get_validator("int32", CH)("100"), Ok)

    def test_int64(self):
        assert isinstance(get_validator("int64", CH)("-1"), Ok)

    def test_int128(self):
        assert isinstance(get_validator("int128", CH)("0"), Ok)

    def test_uint8_zero(self):
        assert get_validator("uint8", CH)("0") == Ok(0)

    def test_uint8_negative_returns_err(self):
        assert isinstance(get_validator("uint8", CH)("-1"), Err)

    def test_uint64_max(self):
        assert isinstance(get_validator("uint64", CH)("18446744073709551615"), Ok)

    def test_float32(self):
        assert isinstance(get_validator("float32", CH)("1.5"), Ok)

    def test_float64(self):
        assert isinstance(get_validator("float64", CH)("-1.23e100"), Ok)

    def test_string(self):
        assert isinstance(get_validator("string", CH)("anything"), Ok)

    def test_bool_true(self):
        assert isinstance(get_validator("bool", CH)("true"), Ok)

    def test_date_within_range(self):
        assert isinstance(get_validator("date", CH)("2024-01-01"), Ok)

    def test_date_upper_boundary(self):
        # CH Date max: 2149-06-06
        assert isinstance(get_validator("date", CH)("2149-06-06"), Ok)

    def test_date_beyond_upper_boundary(self):
        assert isinstance(get_validator("date", CH)("2150-01-01"), Err)

    def test_date32_extended_range(self):
        # CH Date32: up to 2299-12-31
        assert isinstance(get_validator("date32", CH)("2200-01-01"), Ok)

    def test_datetime_upper_boundary(self):
        # CH DateTime max: 2106-02-07 06:28:15
        assert isinstance(get_validator("datetime", CH)("2106-02-07 06:28:15"), Ok)

    def test_datetime_beyond_upper_boundary(self):
        assert isinstance(get_validator("datetime", CH)("2107-01-01 00:00:00"), Err)

    def test_datetime64_extended_range(self):
        assert isinstance(get_validator("datetime64", CH)("2200-06-01 12:00:00"), Ok)

    def test_uuid(self):
        assert isinstance(
            get_validator("uuid", CH)("123e4567-e89b-12d3-a456-426614174000"), Ok
        )

    def test_fixedstring_within_limit(self):
        assert isinstance(get_validator("fixedstring(5)", CH)("hello"), Ok)

    def test_fixedstring_exceeds_limit(self):
        assert isinstance(get_validator("fixedstring(3)", CH)("toolong"), Err)

    def test_decimal_ch(self):
        v = get_validator("decimal(10,2)", CH)
        assert isinstance(v("9.99"), Ok)

    def test_unknown_type_raises(self):
        with pytest.raises(UnsupportedDataTypeError):
            get_validator("xml", CH)

    def test_gp_type_not_found_in_ch(self):
        # "text" is a GP type, not CH — CH uses "string"
        with pytest.raises(UnsupportedDataTypeError):
            get_validator("text", CH)