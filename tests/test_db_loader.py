"""
tests/test_db_loader.py
========================
Tests for manual_excel_loader.db_loader — load_to_db().

All database connections are mocked; no real GP/CH instance required.
psycopg2.extras.execute_values is imported locally inside the function, so we
patch it at the psycopg2.extras level before the function runs.
"""
from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import MagicMock, call, patch

import pytest

from manual_excel_loader.db_loader import load_to_db
from manual_excel_loader.enums import DatabaseType

GP = DatabaseType.GREENPLUM
CH = DatabaseType.CLICKHOUSE

SCHEME = "my_schema"
TABLE = "my_table"
HEADERS = ["id", "name", "amount"]
BATCH_SIZE = 2


# ── psycopg2 stub ─────────────────────────────────────────────────────────────
# psycopg2 may not be installed in the test environment.  We inject a minimal
# stub so that the local `from psycopg2.extras import execute_values` inside
# _load_gp() resolves to our mock without requiring the real C extension.

def _ensure_psycopg2_stub():
    try:
        import psycopg2  # noqa: F401 — use real package if available
    except ImportError:
        psycopg2_mod = ModuleType("psycopg2")
        extras_mod = ModuleType("psycopg2.extras")
        extras_mod.execute_values = MagicMock()
        psycopg2_mod.extras = extras_mod
        psycopg2_mod.connect = MagicMock()
        sys.modules["psycopg2"] = psycopg2_mod
        sys.modules["psycopg2.extras"] = extras_mod

_ensure_psycopg2_stub()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_gp_conn():
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


def _rows(n: int):
    return iter((i, f"name_{i}", float(i)) for i in range(n))


# ── GP — own connection (gp_conn=None) ────────────────────────────────────────

class TestGPOwnConnection:

    def test_returns_correct_row_count(self):
        conn, cur = _make_gp_conn()
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn), \
             patch("psycopg2.extras.execute_values"):
            result = load_to_db(HEADERS, _rows(5), SCHEME, TABLE, GP, batch_size=BATCH_SIZE)
        assert result == 5

    def test_empty_rows_returns_zero(self):
        conn, cur = _make_gp_conn()
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn), \
             patch("psycopg2.extras.execute_values"):
            result = load_to_db(HEADERS, iter([]), SCHEME, TABLE, GP, batch_size=BATCH_SIZE)
        assert result == 0

    def test_execute_values_called_with_correct_sql(self):
        conn, cur = _make_gp_conn()
        mock_ev = MagicMock()
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn), \
             patch("psycopg2.extras.execute_values", mock_ev):
            load_to_db(HEADERS, _rows(1), SCHEME, TABLE, GP, batch_size=BATCH_SIZE)
        sql_used = mock_ev.call_args_list[0].args[1]
        assert f'"{SCHEME}"' in sql_used
        assert f'"{TABLE}"' in sql_used
        for h in HEADERS:
            assert f'"{h}"' in sql_used
        assert "VALUES %s" in sql_used

    def test_rows_batched_correctly(self):
        """5 rows with batch_size=2 → 3 execute_values calls (2+2+1)."""
        conn, cur = _make_gp_conn()
        mock_ev = MagicMock()
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn), \
             patch("psycopg2.extras.execute_values", mock_ev):
            load_to_db(HEADERS, _rows(5), SCHEME, TABLE, GP, batch_size=2)
        assert mock_ev.call_count == 3
        # batch sizes: 2, 2, 1
        batch_sizes = [len(c.args[2]) for c in mock_ev.call_args_list]
        assert batch_sizes == [2, 2, 1]

    def test_rows_exactly_batch_size_one_call(self):
        conn, cur = _make_gp_conn()
        mock_ev = MagicMock()
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn), \
             patch("psycopg2.extras.execute_values", mock_ev):
            load_to_db(HEADERS, _rows(BATCH_SIZE), SCHEME, TABLE, GP, batch_size=BATCH_SIZE)
        assert mock_ev.call_count == 1

    def test_rows_batch_size_plus_one_two_calls(self):
        conn, cur = _make_gp_conn()
        mock_ev = MagicMock()
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn), \
             patch("psycopg2.extras.execute_values", mock_ev):
            load_to_db(HEADERS, _rows(BATCH_SIZE + 1), SCHEME, TABLE, GP, batch_size=BATCH_SIZE)
        assert mock_ev.call_count == 2

    def test_commit_called_on_success(self):
        conn, cur = _make_gp_conn()
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn), \
             patch("psycopg2.extras.execute_values"):
            load_to_db(HEADERS, _rows(3), SCHEME, TABLE, GP, batch_size=BATCH_SIZE)
        conn.commit.assert_called_once()

    def test_rollback_called_on_exception(self):
        conn, cur = _make_gp_conn()
        mock_ev = MagicMock(side_effect=RuntimeError("DB error"))
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn), \
             patch("psycopg2.extras.execute_values", mock_ev):
            with pytest.raises(RuntimeError, match="DB error"):
                load_to_db(HEADERS, _rows(3), SCHEME, TABLE, GP, batch_size=BATCH_SIZE)
        conn.rollback.assert_called_once()

    def test_commit_not_called_on_exception(self):
        conn, cur = _make_gp_conn()
        mock_ev = MagicMock(side_effect=RuntimeError("DB error"))
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn), \
             patch("psycopg2.extras.execute_values", mock_ev):
            with pytest.raises(RuntimeError):
                load_to_db(HEADERS, _rows(3), SCHEME, TABLE, GP, batch_size=BATCH_SIZE)
        conn.commit.assert_not_called()

    def test_conn_closed_after_success(self):
        conn, cur = _make_gp_conn()
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn), \
             patch("psycopg2.extras.execute_values"):
            load_to_db(HEADERS, _rows(3), SCHEME, TABLE, GP, batch_size=BATCH_SIZE)
        conn.close.assert_called_once()

    def test_conn_closed_after_exception(self):
        conn, cur = _make_gp_conn()
        mock_ev = MagicMock(side_effect=RuntimeError("DB error"))
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn), \
             patch("psycopg2.extras.execute_values", mock_ev):
            with pytest.raises(RuntimeError):
                load_to_db(HEADERS, _rows(3), SCHEME, TABLE, GP, batch_size=BATCH_SIZE)
        conn.close.assert_called_once()

    def test_autocommit_set_false_for_own_conn(self):
        conn, cur = _make_gp_conn()
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn), \
             patch("psycopg2.extras.execute_values"):
            load_to_db(HEADERS, _rows(1), SCHEME, TABLE, GP, batch_size=BATCH_SIZE)
        assert conn.autocommit is False


# ── GP — provided connection (gp_conn=mock) ───────────────────────────────────

class TestGPProvidedConnection:

    def test_does_not_create_new_connection(self):
        provided_conn, cur = _make_gp_conn()
        mock_factory = MagicMock()
        with patch("manual_excel_loader._connections.get_gp_conn", mock_factory), \
             patch("psycopg2.extras.execute_values"):
            load_to_db(
                HEADERS, _rows(3), SCHEME, TABLE, GP,
                batch_size=BATCH_SIZE, gp_conn=provided_conn,
            )
        mock_factory.assert_not_called()

    def test_does_not_call_commit(self):
        provided_conn, cur = _make_gp_conn()
        with patch("manual_excel_loader._connections.get_gp_conn"), \
             patch("psycopg2.extras.execute_values"):
            load_to_db(
                HEADERS, _rows(3), SCHEME, TABLE, GP,
                batch_size=BATCH_SIZE, gp_conn=provided_conn,
            )
        provided_conn.commit.assert_not_called()

    def test_does_not_call_rollback_on_exception(self):
        provided_conn, cur = _make_gp_conn()
        mock_ev = MagicMock(side_effect=RuntimeError("err"))
        with patch("manual_excel_loader._connections.get_gp_conn"), \
             patch("psycopg2.extras.execute_values", mock_ev):
            with pytest.raises(RuntimeError):
                load_to_db(
                    HEADERS, _rows(3), SCHEME, TABLE, GP,
                    batch_size=BATCH_SIZE, gp_conn=provided_conn,
                )
        provided_conn.rollback.assert_not_called()

    def test_does_not_close_provided_conn(self):
        provided_conn, cur = _make_gp_conn()
        with patch("manual_excel_loader._connections.get_gp_conn"), \
             patch("psycopg2.extras.execute_values"):
            load_to_db(
                HEADERS, _rows(3), SCHEME, TABLE, GP,
                batch_size=BATCH_SIZE, gp_conn=provided_conn,
            )
        provided_conn.close.assert_not_called()

    def test_returns_correct_row_count_with_provided_conn(self):
        provided_conn, cur = _make_gp_conn()
        with patch("manual_excel_loader._connections.get_gp_conn"), \
             patch("psycopg2.extras.execute_values"):
            result = load_to_db(
                HEADERS, _rows(4), SCHEME, TABLE, GP,
                batch_size=BATCH_SIZE, gp_conn=provided_conn,
            )
        assert result == 4


# ── CH — own client (ch_client=None) ─────────────────────────────────────────

class TestCHOwnClient:

    def test_returns_correct_row_count(self):
        client = MagicMock()
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            result = load_to_db(HEADERS, _rows(5), SCHEME, TABLE, CH, batch_size=BATCH_SIZE)
        assert result == 5

    def test_empty_rows_returns_zero(self):
        client = MagicMock()
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            result = load_to_db(HEADERS, iter([]), SCHEME, TABLE, CH, batch_size=BATCH_SIZE)
        assert result == 0

    def test_execute_called_with_correct_sql(self):
        client = MagicMock()
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            load_to_db(HEADERS, _rows(1), SCHEME, TABLE, CH, batch_size=BATCH_SIZE)
        sql_used = client.execute.call_args_list[0].args[0]
        assert f"`{SCHEME}`" in sql_used
        assert f"`{TABLE}`" in sql_used
        for h in HEADERS:
            assert f"`{h}`" in sql_used
        assert "VALUES" in sql_used

    def test_rows_batched_correctly(self):
        """5 rows with batch_size=2 → 3 execute calls (2+2+1)."""
        client = MagicMock()
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            load_to_db(HEADERS, _rows(5), SCHEME, TABLE, CH, batch_size=2)
        assert client.execute.call_count == 3
        batch_sizes = [len(c.args[1]) for c in client.execute.call_args_list]
        assert batch_sizes == [2, 2, 1]

    def test_rows_exactly_batch_size_one_call(self):
        client = MagicMock()
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            load_to_db(HEADERS, _rows(BATCH_SIZE), SCHEME, TABLE, CH, batch_size=BATCH_SIZE)
        assert client.execute.call_count == 1

    def test_rows_batch_size_plus_one_two_calls(self):
        client = MagicMock()
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            load_to_db(HEADERS, _rows(BATCH_SIZE + 1), SCHEME, TABLE, CH, batch_size=BATCH_SIZE)
        assert client.execute.call_count == 2

    def test_does_not_call_get_ch_client_when_provided(self):
        provided_client = MagicMock()
        mock_factory = MagicMock()
        with patch("manual_excel_loader._connections.get_ch_client", mock_factory):
            load_to_db(
                HEADERS, _rows(3), SCHEME, TABLE, CH,
                batch_size=BATCH_SIZE, ch_client=provided_client,
            )
        mock_factory.assert_not_called()


# ── CH — provided client ──────────────────────────────────────────────────────

class TestCHProvidedClient:

    def test_uses_provided_client(self):
        provided_client = MagicMock()
        with patch("manual_excel_loader._connections.get_ch_client"):
            result = load_to_db(
                HEADERS, _rows(3), SCHEME, TABLE, CH,
                batch_size=BATCH_SIZE, ch_client=provided_client,
            )
        provided_client.execute.assert_called()

    def test_returns_correct_row_count_with_provided_client(self):
        provided_client = MagicMock()
        with patch("manual_excel_loader._connections.get_ch_client"):
            result = load_to_db(
                HEADERS, _rows(4), SCHEME, TABLE, CH,
                batch_size=BATCH_SIZE, ch_client=provided_client,
            )
        assert result == 4

    def test_empty_rows_provided_client(self):
        provided_client = MagicMock()
        with patch("manual_excel_loader._connections.get_ch_client"):
            result = load_to_db(
                HEADERS, iter([]), SCHEME, TABLE, CH,
                batch_size=BATCH_SIZE, ch_client=provided_client,
            )
        assert result == 0
        provided_client.execute.assert_not_called()
