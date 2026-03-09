"""
tests/test_ddl_generator.py
============================
Tests for manual_excel_loader.ddl_generator — generate_ddl().
"""
from __future__ import annotations

import pytest

from manual_excel_loader.ddl_generator import generate_ddl
from manual_excel_loader.enums import DatabaseType, TimestampField

GP = DatabaseType.GREENPLUM
CH = DatabaseType.CLICKHOUSE

SCHEME = "my_schema"
TABLE = "my_table"

COLUMNS = {
    "id": "bigint",
    "name": "text",
    "amount": "decimal(18,6)",
    "created": "date",
}


# ── GP DDL ────────────────────────────────────────────────────────────────────

class TestGenerateDDLGP:

    def test_contains_create_table(self):
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, GP)
        assert "CREATE TABLE" in ddl

    def test_contains_distributed_randomly(self):
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, GP)
        assert "DISTRIBUTED RANDOMLY" in ddl

    def test_contains_scheme_and_table(self):
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, GP)
        assert SCHEME in ddl
        assert TABLE in ddl

    def test_all_column_names_present(self):
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, GP)
        for col in COLUMNS:
            assert col in ddl

    def test_all_column_types_present(self):
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, GP)
        for dtype in COLUMNS.values():
            assert dtype in ddl

    def test_column_order_preserved(self):
        """Python 3.7+ dicts maintain insertion order; DDL must reflect it."""
        ordered = {"z_col": "text", "a_col": "bigint", "m_col": "date"}
        ddl = generate_ddl(ordered, SCHEME, TABLE, GP)
        pos_z = ddl.index("z_col")
        pos_a = ddl.index("a_col")
        pos_m = ddl.index("m_col")
        assert pos_z < pos_a < pos_m

    def test_timestamp_col_added_when_not_in_columns(self):
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, GP, timestamp_col="load_dttm")
        assert "load_dttm" in ddl
        assert "timestamp" in ddl

    def test_timestamp_col_not_duplicated_when_already_in_columns(self):
        cols_with_ts = {**COLUMNS, "load_dttm": "timestamp"}
        ddl = generate_ddl(cols_with_ts, SCHEME, TABLE, GP, timestamp_col="load_dttm")
        assert ddl.count("load_dttm") == 1

    def test_no_timestamp_col_when_none(self):
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, GP, timestamp_col=None)
        # Should not contain any extra timestamp beyond what's in COLUMNS
        # (COLUMNS has no timestamp column)
        assert ddl.count("load_dttm") == 0
        assert ddl.count("write_ts") == 0

    def test_timestamp_col_as_enum(self):
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, GP, timestamp_col=TimestampField.LOAD_DTTM)
        assert "load_dttm" in ddl

    def test_timestamp_col_as_string(self):
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, GP, timestamp_col="my_ts")
        assert "my_ts" in ddl

    def test_timestamp_enum_write_ts(self):
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, GP, timestamp_col=TimestampField.WRITE_TS)
        assert "write_ts" in ddl

    def test_empty_columns_produces_valid_ddl(self):
        ddl = generate_ddl({}, SCHEME, TABLE, GP)
        assert "CREATE TABLE" in ddl
        assert "DISTRIBUTED RANDOMLY" in ddl

    def test_returns_string(self):
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, GP)
        assert isinstance(ddl, str)
        assert len(ddl) > 0

    def test_does_not_contain_nullable(self):
        # GP DDL should not wrap types in Nullable()
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, GP)
        assert "Nullable" not in ddl

    def test_contains_null_modifier(self):
        # GP columns include NULL modifier
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, GP)
        assert "NULL" in ddl


# ── CH DDL ────────────────────────────────────────────────────────────────────

class TestGenerateDDLCH:

    def test_contains_create_table(self):
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, CH)
        assert "CREATE TABLE" in ddl

    def test_contains_engine_mergetree(self):
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, CH)
        assert "ENGINE = MergeTree" in ddl

    def test_contains_order_by_tuple(self):
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, CH)
        assert "ORDER BY tuple()" in ddl

    def test_contains_scheme_and_table(self):
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, CH)
        assert SCHEME in ddl
        assert TABLE in ddl

    def test_columns_wrapped_in_nullable(self):
        simple_cols = {"col": "String"}
        ddl = generate_ddl(simple_cols, SCHEME, TABLE, CH)
        assert "Nullable(String)" in ddl

    def test_all_columns_wrapped_in_nullable(self):
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, CH)
        for dtype in COLUMNS.values():
            assert f"Nullable({dtype})" in ddl

    def test_timestamp_col_added_as_nullable_datetime(self):
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, CH, timestamp_col="load_dttm")
        assert "load_dttm" in ddl
        assert "Nullable(DateTime())" in ddl

    def test_timestamp_col_not_duplicated_when_already_in_columns(self):
        cols_with_ts = {**COLUMNS, "load_dttm": "DateTime"}
        ddl = generate_ddl(cols_with_ts, SCHEME, TABLE, CH, timestamp_col="load_dttm")
        assert ddl.count("load_dttm") == 1

    def test_timestamp_col_as_enum_ch(self):
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, CH, timestamp_col=TimestampField.LOAD_DTTM)
        assert "load_dttm" in ddl
        assert "Nullable(DateTime())" in ddl

    def test_timestamp_col_as_string_ch(self):
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, CH, timestamp_col="my_ts")
        assert "my_ts" in ddl
        assert "Nullable(DateTime())" in ddl

    def test_empty_columns_produces_valid_ddl(self):
        ddl = generate_ddl({}, SCHEME, TABLE, CH)
        assert "CREATE TABLE" in ddl
        assert "ENGINE = MergeTree" in ddl

    def test_column_order_preserved_ch(self):
        ordered = {"z_col": "String", "a_col": "Int64", "m_col": "Date32"}
        ddl = generate_ddl(ordered, SCHEME, TABLE, CH)
        pos_z = ddl.index("z_col")
        pos_a = ddl.index("a_col")
        pos_m = ddl.index("m_col")
        assert pos_z < pos_a < pos_m

    def test_contains_allow_nullable_key_setting(self):
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, CH)
        assert "allow_nullable_key" in ddl

    def test_returns_string_ch(self):
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, CH)
        assert isinstance(ddl, str)
        assert len(ddl) > 0

    def test_does_not_contain_distributed_randomly(self):
        ddl = generate_ddl(COLUMNS, SCHEME, TABLE, CH)
        assert "DISTRIBUTED RANDOMLY" not in ddl


# ── Shared behaviour ──────────────────────────────────────────────────────────

class TestGenerateDDLShared:

    @pytest.mark.parametrize("db_type", [GP, CH])
    def test_single_column_ddl(self, db_type):
        ddl = generate_ddl({"col": "bigint"}, SCHEME, TABLE, db_type)
        assert "col" in ddl
        assert "CREATE TABLE" in ddl

    @pytest.mark.parametrize("db_type", [GP, CH])
    def test_full_qualified_table_name_in_ddl(self, db_type):
        ddl = generate_ddl({"x": "text"}, "sc", "tb", db_type)
        assert "sc" in ddl
        assert "tb" in ddl

    @pytest.mark.parametrize("db_type", [GP, CH])
    def test_timestamp_col_none_does_not_add_extra_column(self, db_type):
        cols = {"id": "bigint"}
        ddl_with = generate_ddl(cols, SCHEME, TABLE, db_type, timestamp_col="ts")
        ddl_without = generate_ddl(cols, SCHEME, TABLE, db_type, timestamp_col=None)
        # DDL with timestamp_col must be longer (extra column definition added)
        assert len(ddl_with) > len(ddl_without)
