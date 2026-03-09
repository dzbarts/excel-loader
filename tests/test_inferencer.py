"""
tests/test_inferencer.py
========================
Tests for manual_excel_loader.inferencer — infer_types().

SheetData is constructed synthetically: no real Excel file required.
"""
from __future__ import annotations

from datetime import date, datetime, time

import pytest

from manual_excel_loader.enums import DatabaseType
from manual_excel_loader.inferencer import SAMPLE_SIZE, infer_types
from manual_excel_loader.readers.excel_reader import SheetData

GP = DatabaseType.GREENPLUM
CH = DatabaseType.CLICKHOUSE


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make(headers: list[str], rows: list[tuple]) -> SheetData:
    """Construct a SheetData from a plain list of rows."""
    return SheetData(headers=headers, rows=iter(rows))


# ── GP single-column type inference ──────────────────────────────────────────

class TestInferGPSingleColumn:

    def test_all_int_returns_bigint(self):
        sd = _make(["col"], [(1,), (2,), (3,)])
        assert infer_types(sd, GP) == {"col": "bigint"}

    def test_mixed_int_and_float_returns_decimal(self):
        sd = _make(["col"], [(1,), (2.5,), (3,)])
        assert infer_types(sd, GP) == {"col": "decimal(18,6)"}

    def test_all_float_returns_decimal(self):
        sd = _make(["col"], [(1.1,), (2.2,)])
        assert infer_types(sd, GP) == {"col": "decimal(18,6)"}

    def test_datetime_values_returns_timestamp(self):
        sd = _make(["col"], [(datetime(2024, 1, 1, 12, 0),), (datetime(2024, 6, 1),)])
        assert infer_types(sd, GP) == {"col": "timestamp"}

    def test_date_only_returns_date(self):
        # date objects (not datetime) → "date"
        sd = _make(["col"], [(date(2024, 1, 1),), (date(2024, 6, 1),)])
        assert infer_types(sd, GP) == {"col": "date"}

    def test_time_returns_time(self):
        sd = _make(["col"], [(time(12, 30),), (time(8, 0),)])
        assert infer_types(sd, GP) == {"col": "time"}

    def test_bool_returns_boolean(self):
        sd = _make(["col"], [(True,), (False,)])
        assert infer_types(sd, GP) == {"col": "boolean"}

    def test_str_values_returns_text(self):
        sd = _make(["col"], [("hello",), ("world",)])
        assert infer_types(sd, GP) == {"col": "text"}

    def test_any_str_wins_over_numeric(self):
        # Mixed int + str → text
        sd = _make(["col"], [(1,), ("two",), (3,)])
        assert infer_types(sd, GP) == {"col": "text"}

    def test_empty_column_all_none_returns_text(self):
        sd = _make(["col"], [(None,), (None,), (None,)])
        assert infer_types(sd, GP) == {"col": "text"}

    def test_datetime_wins_over_date(self):
        # Column has both date and datetime → timestamp (datetime wins)
        sd = _make(["col"], [(date(2024, 1, 1),), (datetime(2024, 6, 1, 10, 0),)])
        assert infer_types(sd, GP) == {"col": "timestamp"}

    def test_str_wins_over_datetime(self):
        sd = _make(["col"], [(datetime(2024, 1, 1),), ("not a date",)])
        assert infer_types(sd, GP) == {"col": "text"}


# ── CH single-column type inference ──────────────────────────────────────────

class TestInferCHSingleColumn:

    def test_int_returns_int64(self):
        sd = _make(["col"], [(10,), (20,)])
        assert infer_types(sd, CH) == {"col": "Int64"}

    def test_float_returns_float64(self):
        sd = _make(["col"], [(1.5,), (2.7,)])
        assert infer_types(sd, CH) == {"col": "Float64"}

    def test_mixed_int_float_returns_float64(self):
        sd = _make(["col"], [(1,), (2.5,)])
        assert infer_types(sd, CH) == {"col": "Float64"}

    def test_datetime_returns_datetime(self):
        sd = _make(["col"], [(datetime(2024, 1, 1, 12, 0),)])
        assert infer_types(sd, CH) == {"col": "DateTime"}

    def test_date_returns_date32(self):
        sd = _make(["col"], [(date(2024, 1, 1),)])
        assert infer_types(sd, CH) == {"col": "Date32"}

    def test_time_returns_string(self):
        # CH has no native Time type — falls back to String
        sd = _make(["col"], [(time(10, 30),)])
        assert infer_types(sd, CH) == {"col": "String"}

    def test_bool_returns_bool(self):
        sd = _make(["col"], [(True,), (False,)])
        assert infer_types(sd, CH) == {"col": "Bool"}

    def test_str_returns_string(self):
        sd = _make(["col"], [("hello",)])
        assert infer_types(sd, CH) == {"col": "String"}

    def test_empty_column_all_none_returns_string(self):
        sd = _make(["col"], [(None,), (None,)])
        assert infer_types(sd, CH) == {"col": "String"}

    def test_any_str_wins_over_numeric_ch(self):
        sd = _make(["col"], [(1,), ("text",)])
        assert infer_types(sd, CH) == {"col": "String"}


# ── Multiple columns ──────────────────────────────────────────────────────────

class TestInferMultipleColumns:

    def test_gp_multi_column_independent_inference(self):
        sd = _make(
            ["id", "name", "amount", "created"],
            [
                (1, "Alice", 99.99, date(2024, 1, 1)),
                (2, "Bob", 150.0, date(2024, 6, 1)),
            ],
        )
        result = infer_types(sd, GP)
        assert result["id"] == "bigint"
        assert result["name"] == "text"
        assert result["amount"] == "decimal(18,6)"
        assert result["created"] == "date"

    def test_ch_multi_column_independent_inference(self):
        sd = _make(
            ["ts", "val", "flag"],
            [
                (datetime(2024, 1, 1, 0, 0), 42, True),
                (datetime(2024, 2, 1, 0, 0), 43, False),
            ],
        )
        result = infer_types(sd, CH)
        assert result["ts"] == "DateTime"
        assert result["val"] == "Int64"
        assert result["flag"] == "Bool"

    def test_column_order_preserved(self):
        headers = ["z_col", "a_col", "m_col"]
        sd = _make(headers, [("x", 1, True)])
        result = infer_types(sd, GP)
        assert list(result.keys()) == headers

    def test_none_mixed_with_values_ignored_in_sample(self):
        # None values are excluded from sample; remaining int values → bigint
        sd = _make(["col"], [(None,), (5,), (None,), (10,)])
        assert infer_types(sd, GP) == {"col": "bigint"}


# ── SAMPLE_SIZE limit ─────────────────────────────────────────────────────────

class TestInferSampleSizeLimit:

    def test_only_first_200_rows_sampled(self):
        """Rows beyond SAMPLE_SIZE are not consumed.

        We put int values in the first 200 rows and str values after.
        With the 200-row cap the result must be "bigint", not "text".
        """
        int_rows = [(i,) for i in range(SAMPLE_SIZE)]
        str_rows = [("should_be_ignored",)] * 50
        sd = _make(["col"], int_rows + str_rows)
        assert infer_types(sd, GP) == {"col": "bigint"}

    def test_exactly_sample_size_rows(self):
        rows = [(i,) for i in range(SAMPLE_SIZE)]
        sd = _make(["col"], rows)
        assert infer_types(sd, GP) == {"col": "bigint"}

    def test_fewer_than_sample_size_rows(self):
        rows = [(i,) for i in range(10)]
        sd = _make(["col"], rows)
        assert infer_types(sd, GP) == {"col": "bigint"}

    def test_sample_size_constant_is_200(self):
        assert SAMPLE_SIZE == 200
