"""
tests/test_db_schema.py
=======================
Tests for manual_excel_loader.db_schema — get_table_columns() and table_exists().

All database connections are mocked; no real GP/CH instance required.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from manual_excel_loader.db_schema import get_table_columns, table_exists
from manual_excel_loader.enums import DatabaseType

GP = DatabaseType.GREENPLUM
CH = DatabaseType.CLICKHOUSE

SCHEME = "my_schema"
TABLE = "my_table"


# ── GreenPlum ─────────────────────────────────────────────────────────────────

class TestGetTableColumnsGP:

    def _mock_conn(self, fetchall_return):
        """Return a mock psycopg2 connection whose cursor.fetchall() returns the given value."""
        cur = MagicMock()
        cur.fetchall.return_value = fetchall_return
        cur.__enter__ = MagicMock(return_value=cur)
        cur.__exit__ = MagicMock(return_value=False)
        conn = MagicMock()
        conn.cursor.return_value = cur
        return conn

    def test_table_found_returns_dict(self):
        rows = [("id", "bigint"), ("name", "text")]
        conn = self._mock_conn(rows)
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            result = get_table_columns(SCHEME, TABLE, GP)
        assert result == {"id": "bigint", "name": "text"}

    def test_table_not_found_returns_none(self):
        conn = self._mock_conn([])
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            result = get_table_columns(SCHEME, TABLE, GP)
        assert result is None

    def test_column_order_preserved(self):
        rows = [("z_col", "text"), ("a_col", "bigint"), ("m_col", "date")]
        conn = self._mock_conn(rows)
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            result = get_table_columns(SCHEME, TABLE, GP)
        assert list(result.keys()) == ["z_col", "a_col", "m_col"]

    def test_character_varying_type_preserved(self):
        rows = [("name", "character varying(255)")]
        conn = self._mock_conn(rows)
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            result = get_table_columns(SCHEME, TABLE, GP)
        assert result["name"] == "character varying(255)"

    def test_numeric_type_with_precision_and_scale(self):
        rows = [("amount", "numeric(18,6)")]
        conn = self._mock_conn(rows)
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            result = get_table_columns(SCHEME, TABLE, GP)
        assert result["amount"] == "numeric(18,6)"

    def test_integer_type_preserved(self):
        rows = [("id", "integer")]
        conn = self._mock_conn(rows)
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            result = get_table_columns(SCHEME, TABLE, GP)
        assert result["id"] == "integer"

    def test_conn_close_called(self):
        conn = self._mock_conn([("id", "bigint")])
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            get_table_columns(SCHEME, TABLE, GP)
        conn.close.assert_called_once()

    def test_conn_close_called_even_on_empty_result(self):
        conn = self._mock_conn([])
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            get_table_columns(SCHEME, TABLE, GP)
        conn.close.assert_called_once()

    def test_single_column_table(self):
        rows = [("only_col", "boolean")]
        conn = self._mock_conn(rows)
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            result = get_table_columns(SCHEME, TABLE, GP)
        assert result == {"only_col": "boolean"}


# ── ClickHouse ────────────────────────────────────────────────────────────────

class TestGetTableColumnsCH:

    def _mock_client(self, execute_return):
        client = MagicMock()
        client.execute.return_value = execute_return
        return client

    def test_table_found_returns_dict(self):
        rows = [("id", "Int64"), ("name", "String")]
        client = self._mock_client(rows)
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            result = get_table_columns(SCHEME, TABLE, CH)
        assert result == {"id": "Int64", "name": "String"}

    def test_table_not_found_returns_none(self):
        client = self._mock_client([])
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            result = get_table_columns(SCHEME, TABLE, CH)
        assert result is None

    def test_nullable_string_unwrapped(self):
        rows = [("col", "Nullable(String)")]
        client = self._mock_client(rows)
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            result = get_table_columns(SCHEME, TABLE, CH)
        assert result["col"] == "String"

    def test_nullable_datetime_unwrapped(self):
        rows = [("ts", "Nullable(DateTime)")]
        client = self._mock_client(rows)
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            result = get_table_columns(SCHEME, TABLE, CH)
        assert result["ts"] == "DateTime"

    def test_nullable_int64_unwrapped(self):
        rows = [("val", "Nullable(Int64)")]
        client = self._mock_client(rows)
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            result = get_table_columns(SCHEME, TABLE, CH)
        assert result["val"] == "Int64"

    def test_plain_int64_not_changed(self):
        rows = [("val", "Int64")]
        client = self._mock_client(rows)
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            result = get_table_columns(SCHEME, TABLE, CH)
        assert result["val"] == "Int64"

    def test_plain_string_not_changed(self):
        rows = [("col", "String")]
        client = self._mock_client(rows)
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            result = get_table_columns(SCHEME, TABLE, CH)
        assert result["col"] == "String"

    def test_mixed_nullable_and_plain_types(self):
        rows = [("a", "Nullable(Float64)"), ("b", "Date32"), ("c", "Nullable(Bool)")]
        client = self._mock_client(rows)
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            result = get_table_columns(SCHEME, TABLE, CH)
        assert result == {"a": "Float64", "b": "Date32", "c": "Bool"}

    def test_column_order_preserved_ch(self):
        rows = [("z", "String"), ("a", "Int64"), ("m", "Date32")]
        client = self._mock_client(rows)
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            result = get_table_columns(SCHEME, TABLE, CH)
        assert list(result.keys()) == ["z", "a", "m"]

    def test_nullable_with_nested_type(self):
        rows = [("col", "Nullable(Decimal(18,6))")]
        client = self._mock_client(rows)
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            result = get_table_columns(SCHEME, TABLE, CH)
        assert result["col"] == "Decimal(18,6)"


# ── table_exists ──────────────────────────────────────────────────────────────

class TestTableExists:

    def test_returns_true_when_table_found_gp(self):
        with patch(
            "manual_excel_loader.db_schema.get_table_columns",
            return_value={"id": "bigint"},
        ):
            assert table_exists(SCHEME, TABLE, GP) is True

    def test_returns_false_when_table_not_found_gp(self):
        with patch(
            "manual_excel_loader.db_schema.get_table_columns",
            return_value=None,
        ):
            assert table_exists(SCHEME, TABLE, GP) is False

    def test_returns_true_when_table_found_ch(self):
        with patch(
            "manual_excel_loader.db_schema.get_table_columns",
            return_value={"col": "String"},
        ):
            assert table_exists(SCHEME, TABLE, CH) is True

    def test_returns_false_when_table_not_found_ch(self):
        with patch(
            "manual_excel_loader.db_schema.get_table_columns",
            return_value=None,
        ):
            assert table_exists(SCHEME, TABLE, CH) is False

    def test_empty_dict_is_truthy(self):
        # An empty dict (zero columns) is a valid return from get_table_columns
        # and means the table exists (edge case)
        with patch(
            "manual_excel_loader.db_schema.get_table_columns",
            return_value={},
        ):
            assert table_exists(SCHEME, TABLE, GP) is True
