"""
tests/test_table_manager.py
============================
Tests for manual_excel_loader.table_manager — prepare() and finalize().

All database connections are mocked; no real GP/CH instance required.
"""
from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

from manual_excel_loader.enums import DatabaseType
from manual_excel_loader.table_manager import finalize, prepare

GP = DatabaseType.GREENPLUM
CH = DatabaseType.CLICKHOUSE

SCHEME = "my_schema"
TABLE = "my_table"
DDL = f"CREATE TABLE {SCHEME}.{TABLE} (id bigint NULL) DISTRIBUTED RANDOMLY;"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_gp_conn(table_exists: bool = True):
    """Return a mock psycopg2 connection."""
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    # fetchone() controls table existence check
    cur.fetchone.return_value = (1,) if table_exists else None
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


def _make_ch_client(table_exists: bool = True):
    """Return a mock clickhouse_driver.Client."""
    client = MagicMock()
    # First call: system.tables check; subsequent calls: DDL/DML
    client.execute.return_value = [(1,)] if table_exists else []
    return client


# ── GP append ─────────────────────────────────────────────────────────────────

class TestGPAppend:

    def test_table_exists_no_ddl_executed(self):
        conn, cur = _make_gp_conn(table_exists=True)
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            ctx = prepare(SCHEME, TABLE, GP, "append", create_ddl=DDL)
        # fetchone returns truthy → table exists → DDL not executed
        executed_sqls = [c.args[0] for c in cur.execute.call_args_list]
        assert not any("CREATE TABLE" in sql for sql in executed_sqls)

    def test_table_not_exists_ddl_executed(self):
        conn, cur = _make_gp_conn(table_exists=False)
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            ctx = prepare(SCHEME, TABLE, GP, "append", create_ddl=DDL)
        executed_sqls = [c.args[0] for c in cur.execute.call_args_list]
        assert any("CREATE TABLE" in sql for sql in executed_sqls)

    def test_table_not_exists_commit_called(self):
        conn, cur = _make_gp_conn(table_exists=False)
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            prepare(SCHEME, TABLE, GP, "append", create_ddl=DDL)
        conn.commit.assert_called()

    def test_no_ddl_provided_no_execute(self):
        conn, cur = _make_gp_conn(table_exists=True)
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            ctx = prepare(SCHEME, TABLE, GP, "append", create_ddl=None)
        cur.execute.assert_not_called()

    def test_autocommit_set_true(self):
        conn, _ = _make_gp_conn(table_exists=True)
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            prepare(SCHEME, TABLE, GP, "append", create_ddl=None)
        assert conn.autocommit is True

    def test_context_contains_conn(self):
        conn, _ = _make_gp_conn(table_exists=True)
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            ctx = prepare(SCHEME, TABLE, GP, "append", create_ddl=None)
        assert ctx["conn"] is conn


# ── GP truncate_load ──────────────────────────────────────────────────────────

class TestGPTruncateLoad:

    def test_truncate_executed(self):
        conn, cur = _make_gp_conn(table_exists=True)
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            ctx = prepare(SCHEME, TABLE, GP, "truncate_load", create_ddl=None)
        executed_sqls = [c.args[0] for c in cur.execute.call_args_list]
        assert any("TRUNCATE" in sql for sql in executed_sqls)

    def test_finalize_success_commits(self):
        conn, cur = _make_gp_conn(table_exists=True)
        conn.autocommit = False
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            ctx = prepare(SCHEME, TABLE, GP, "truncate_load", create_ddl=None)
        conn.commit.reset_mock()
        finalize(ctx, success=True)
        conn.commit.assert_called_once()

    def test_finalize_failure_rollbacks(self):
        conn, cur = _make_gp_conn(table_exists=True)
        conn.autocommit = False
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            ctx = prepare(SCHEME, TABLE, GP, "truncate_load", create_ddl=None)
        finalize(ctx, success=False)
        conn.rollback.assert_called_once()

    def test_finalize_failure_does_not_commit(self):
        conn, cur = _make_gp_conn(table_exists=True)
        conn.autocommit = False
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            ctx = prepare(SCHEME, TABLE, GP, "truncate_load", create_ddl=None)
        conn.commit.reset_mock()
        finalize(ctx, success=False)
        conn.commit.assert_not_called()

    def test_conn_closed_after_finalize(self):
        conn, cur = _make_gp_conn(table_exists=True)
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            ctx = prepare(SCHEME, TABLE, GP, "truncate_load", create_ddl=None)
        finalize(ctx, success=True)
        conn.close.assert_called_once()


# ── GP via_backup ─────────────────────────────────────────────────────────────

class TestGPViaBackup:

    def test_rename_executed_when_table_exists(self):
        conn, cur = _make_gp_conn(table_exists=True)
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            ctx = prepare(SCHEME, TABLE, GP, "via_backup", create_ddl=DDL)
        executed_sqls = [c.args[0] for c in cur.execute.call_args_list]
        assert any("RENAME" in sql for sql in executed_sqls)

    def test_ddl_executed_after_rename(self):
        conn, cur = _make_gp_conn(table_exists=True)
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            ctx = prepare(SCHEME, TABLE, GP, "via_backup", create_ddl=DDL)
        executed_sqls = [c.args[0] for c in cur.execute.call_args_list]
        assert any("CREATE TABLE" in sql for sql in executed_sqls)

    def test_commit_called_after_setup(self):
        conn, cur = _make_gp_conn(table_exists=True)
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            prepare(SCHEME, TABLE, GP, "via_backup", create_ddl=DDL)
        conn.commit.assert_called()

    def test_no_rename_when_table_not_exists(self):
        conn, cur = _make_gp_conn(table_exists=False)
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            ctx = prepare(SCHEME, TABLE, GP, "via_backup", create_ddl=DDL)
        executed_sqls = [c.args[0] for c in cur.execute.call_args_list]
        assert not any("RENAME" in sql for sql in executed_sqls)
        assert "backup_table" not in ctx

    def test_ddl_executed_even_without_rename(self):
        conn, cur = _make_gp_conn(table_exists=False)
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            prepare(SCHEME, TABLE, GP, "via_backup", create_ddl=DDL)
        executed_sqls = [c.args[0] for c in cur.execute.call_args_list]
        assert any("CREATE TABLE" in sql for sql in executed_sqls)

    def test_no_ddl_raises_value_error(self):
        conn, _ = _make_gp_conn(table_exists=True)
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            with pytest.raises(ValueError, match="via_backup"):
                prepare(SCHEME, TABLE, GP, "via_backup", create_ddl=None)

    def test_finalize_failure_drops_and_renames_back(self):
        conn, cur = _make_gp_conn(table_exists=True)
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            ctx = prepare(SCHEME, TABLE, GP, "via_backup", create_ddl=DDL)
        # Reset execute history for finalize checks
        cur.execute.reset_mock()
        conn.commit.reset_mock()
        finalize(ctx, success=False)
        executed_sqls = [c.args[0] for c in cur.execute.call_args_list]
        assert any("DROP TABLE" in sql for sql in executed_sqls)
        assert any("RENAME" in sql for sql in executed_sqls)
        conn.commit.assert_called()

    def test_finalize_success_does_not_drop_or_rename(self):
        conn, cur = _make_gp_conn(table_exists=True)
        with patch("manual_excel_loader._connections.get_gp_conn", return_value=conn):
            ctx = prepare(SCHEME, TABLE, GP, "via_backup", create_ddl=DDL)
        cur.execute.reset_mock()
        finalize(ctx, success=True)
        executed_sqls = [c.args[0] for c in cur.execute.call_args_list]
        assert not any("DROP TABLE" in sql for sql in executed_sqls)
        assert not any("RENAME" in sql for sql in executed_sqls)


# ── CH append ─────────────────────────────────────────────────────────────────

class TestCHAppend:

    def test_table_not_exists_ddl_executed(self):
        client = _make_ch_client(table_exists=False)
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            prepare(SCHEME, TABLE, CH, "append", create_ddl=DDL)
        calls_sql = [c.args[0] for c in client.execute.call_args_list]
        assert any("CREATE TABLE" in sql for sql in calls_sql)

    def test_table_exists_no_ddl_executed(self):
        client = _make_ch_client(table_exists=True)
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            prepare(SCHEME, TABLE, CH, "append", create_ddl=DDL)
        calls_sql = [c.args[0] for c in client.execute.call_args_list]
        assert not any("CREATE TABLE" in sql for sql in calls_sql)

    def test_context_contains_client(self):
        client = _make_ch_client(table_exists=True)
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            ctx = prepare(SCHEME, TABLE, CH, "append", create_ddl=None)
        assert ctx["client"] is client


# ── CH truncate_load ──────────────────────────────────────────────────────────

class TestCHTruncateLoad:

    def _prepare_ctx(self, table_exists: bool = True):
        client = MagicMock()
        # First call to execute: system.tables check (when create_ddl provided)
        # Then: CREATE TABLE temp, INSERT INTO temp, TRUNCATE
        side_effects = []
        if True:  # create_ddl is None so no existence check; calls directly
            pass
        client.execute.return_value = [(1,)] if table_exists else []
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            ctx = prepare(SCHEME, TABLE, CH, "truncate_load", create_ddl=None)
        return ctx, client

    def test_create_temp_table_executed(self):
        ctx, client = self._prepare_ctx()
        calls_sql = [c.args[0] for c in client.execute.call_args_list]
        assert any("_temp" in sql for sql in calls_sql)

    def test_insert_into_temp_executed(self):
        ctx, client = self._prepare_ctx()
        calls_sql = [c.args[0] for c in client.execute.call_args_list]
        assert any("INSERT INTO" in sql and "_temp" in sql for sql in calls_sql)

    def test_truncate_executed(self):
        ctx, client = self._prepare_ctx()
        calls_sql = [c.args[0] for c in client.execute.call_args_list]
        assert any("TRUNCATE" in sql for sql in calls_sql)

    def test_temp_table_name_stored_in_context(self):
        ctx, client = self._prepare_ctx()
        assert "temp_table" in ctx
        assert "_temp" in ctx["temp_table"]

    def test_finalize_success_drops_temp(self):
        ctx, client = self._prepare_ctx()
        client.execute.reset_mock()
        finalize(ctx, success=True)
        calls_sql = [c.args[0] for c in client.execute.call_args_list]
        assert any("DROP TABLE" in sql and "_temp" in sql for sql in calls_sql)

    def test_finalize_failure_restores_from_temp(self):
        ctx, client = self._prepare_ctx()
        client.execute.reset_mock()
        finalize(ctx, success=False)
        calls_sql = [c.args[0] for c in client.execute.call_args_list]
        # Must INSERT data back from temp into original
        assert any(
            "INSERT INTO" in sql and TABLE in sql and "_temp" in sql
            for sql in calls_sql
        )

    def test_finalize_failure_drops_temp_after_restore(self):
        ctx, client = self._prepare_ctx()
        client.execute.reset_mock()
        finalize(ctx, success=False)
        calls_sql = [c.args[0] for c in client.execute.call_args_list]
        assert any("DROP TABLE" in sql and "_temp" in sql for sql in calls_sql)


# ── CH via_backup ─────────────────────────────────────────────────────────────

class TestCHViaBackup:

    def test_rename_executed_when_table_exists(self):
        client = MagicMock()
        client.execute.side_effect = [
            [(1,)],  # system.tables check → table exists
            None,    # RENAME TABLE
            None,    # CREATE new table from DDL
        ]
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            ctx = prepare(SCHEME, TABLE, CH, "via_backup", create_ddl=DDL)
        calls_sql = [c.args[0] for c in client.execute.call_args_list]
        assert any("RENAME TABLE" in sql for sql in calls_sql)

    def test_ddl_executed_after_rename(self):
        client = MagicMock()
        client.execute.side_effect = [
            [(1,)],
            None,
            None,
        ]
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            prepare(SCHEME, TABLE, CH, "via_backup", create_ddl=DDL)
        calls_sql = [c.args[0] for c in client.execute.call_args_list]
        assert any("CREATE TABLE" in sql for sql in calls_sql)

    def test_no_rename_when_table_not_exists(self):
        client = MagicMock()
        client.execute.side_effect = [
            [],    # system.tables check → table not found
            None,  # CREATE new table
        ]
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            ctx = prepare(SCHEME, TABLE, CH, "via_backup", create_ddl=DDL)
        calls_sql = [c.args[0] for c in client.execute.call_args_list]
        assert not any("RENAME TABLE" in sql for sql in calls_sql)
        assert "backup_table" not in ctx

    def test_no_ddl_raises_value_error(self):
        client = MagicMock()
        client.execute.return_value = []
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            with pytest.raises(ValueError, match="via_backup"):
                prepare(SCHEME, TABLE, CH, "via_backup", create_ddl=None)

    def test_finalize_failure_drops_and_renames_back(self):
        client = MagicMock()
        client.execute.side_effect = [
            [(1,)],  # system.tables check
            None,    # RENAME TABLE original → backup
            None,    # CREATE new table
        ]
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            ctx = prepare(SCHEME, TABLE, CH, "via_backup", create_ddl=DDL)
        client.execute.reset_mock()
        client.execute.side_effect = None
        client.execute.return_value = None
        finalize(ctx, success=False)
        calls_sql = [c.args[0] for c in client.execute.call_args_list]
        assert any("DROP TABLE" in sql for sql in calls_sql)
        assert any("RENAME TABLE" in sql for sql in calls_sql)

    def test_finalize_success_does_nothing(self):
        client = MagicMock()
        client.execute.side_effect = [
            [(1,)],
            None,
            None,
        ]
        with patch("manual_excel_loader._connections.get_ch_client", return_value=client):
            ctx = prepare(SCHEME, TABLE, CH, "via_backup", create_ddl=DDL)
        client.execute.reset_mock()
        finalize(ctx, success=True)
        # No further execute calls expected on success
        client.execute.assert_not_called()
