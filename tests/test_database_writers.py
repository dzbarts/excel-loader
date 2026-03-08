"""
tests/test_database_writers.py
================================
Тесты PostgresWriter и ClickHouseWriter — мокаем сетевые соединения.

Новые тесты vs предыдущей версии:
  - test_rollback_on_executemany_error: проверяет что conn.rollback() вызывается
    при ошибке батча (PostgresWriter).
  - test_disconnect_called_on_success: проверяет что client.disconnect() вызывается
    после успешной записи (ClickHouseWriter).
  - test_disconnect_called_on_error: проверяет что client.disconnect() вызывается
    даже при ошибке execute (ClickHouseWriter) — нет утечки соединений.
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch, call

from manual_excel_loader.writers.database import PostgresWriter, ClickHouseWriter, _batched
from manual_excel_loader.writers.base import DbWriterConfig


# ── Fixtures ─────────────────────────────────────────────────────────────────────
@pytest.fixture
def pg_config():
    return DbWriterConfig(
        host="localhost",
        port=5432,
        database="testdb",
        user="user",
        password="pass",
        table_name="test_table",
        scheme_name="test_schema",
        batch_size=2,
    )


@pytest.fixture
def ch_config():
    return DbWriterConfig(
        host="localhost",
        port=9000,
        database="testdb",
        user="user",
        password="pass",
        table_name="test_table",
        scheme_name="test_db",
        batch_size=2,
    )


HEADERS = ["id", "name", "age"]
ROWS = [(1, "Alice", 30), (2, "Bob", 25), (3, "Charlie", 35)]


# ── _batched helper ───────────────────────────────────────────────────────────────
class TestBatched:
    def test_even_batches(self):
        result = list(_batched([1, 2, 3, 4], 2))
        assert result == [[1, 2], [3, 4]]

    def test_uneven_batches(self):
        result = list(_batched([1, 2, 3], 2))
        assert result == [[1, 2], [3]]

    def test_empty_input(self):
        assert list(_batched([], 10)) == []

    def test_batch_larger_than_input(self):
        result = list(_batched([1, 2], 100))
        assert result == [[1, 2]]


# ── PostgresWriter ────────────────────────────────────────────────────────────────
class TestPostgresWriter:
    def _make_mock_conn(self):
        """Создаёт mock-объект соединения psycopg2 с context manager."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return mock_conn, mock_cursor

    def test_write_calls_executemany(self, pg_config):
        """executemany вызывается для каждого батча."""
        mock_conn, mock_cursor = self._make_mock_conn()
        with patch("psycopg2.connect", return_value=mock_conn):
            writer = PostgresWriter(pg_config)
            count = writer.write(HEADERS, ROWS)
        # 3 строки, batch_size=2 → 2 вызова executemany
        assert mock_cursor.executemany.call_count == 2
        assert count == 3

    def test_commit_called(self, pg_config):
        """conn.commit() вызывается после всех батчей."""
        mock_conn, mock_cursor = self._make_mock_conn()
        with patch("psycopg2.connect", return_value=mock_conn):
            PostgresWriter(pg_config).write(HEADERS, ROWS)
        mock_conn.commit.assert_called_once()

    def test_rollback_on_executemany_error(self, pg_config):
        """При ошибке батча — conn.rollback() вызывается, исключение пробрасывается.

        Это ключевой тест надёжности: частичные данные не должны оставаться в БД.
        Без rollback в таблице могут оказаться первые N батчей при падении на N+1.
        """
        mock_conn, mock_cursor = self._make_mock_conn()
        mock_cursor.executemany.side_effect = [None, RuntimeError("DB error")]

        with patch("psycopg2.connect", return_value=mock_conn):
            writer = PostgresWriter(pg_config)
            with pytest.raises(RuntimeError, match="DB error"):
                writer.write(HEADERS, ROWS)

        mock_conn.rollback.assert_called_once()
        mock_conn.commit.assert_not_called()

    def test_sql_uses_identifiers(self, pg_config):
        """SQL строится через psycopg2.sql.Identifier — имена экранированы.

        Проверяем что executemany получает Composed-объект (не plain str),
        что означает использование psycopg2.sql API вместо f-строки.
        """
        from psycopg2 import sql as pgsql

        mock_conn, mock_cursor = self._make_mock_conn()
        with patch("psycopg2.connect", return_value=mock_conn):
            PostgresWriter(pg_config).write(HEADERS, ROWS)

        sql_used = mock_cursor.executemany.call_args_list[0][0][0]
        # psycopg2.sql.Composed — это объект, а не строка
        assert isinstance(sql_used, pgsql.Composed), (
            "SQL должен строиться через psycopg2.sql, а не f-строкой — "
            "иначе заголовки Excel могут содержать SQL-инъекцию."
        )

    def test_psycopg2_not_installed_raises(self, pg_config):
        """Если psycopg2 нет — ImportError с понятным сообщением."""
        import builtins
        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "psycopg2":
                raise ImportError("No module named 'psycopg2'")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            writer = PostgresWriter(pg_config)
            with pytest.raises(ImportError, match="psycopg2"):
                writer.write(HEADERS, ROWS)

    def test_empty_rows(self, pg_config):
        """Пустой список строк → 0 записей, commit всё равно вызывается."""
        mock_conn, mock_cursor = self._make_mock_conn()
        with patch("psycopg2.connect", return_value=mock_conn):
            count = PostgresWriter(pg_config).write(HEADERS, [])
        assert count == 0
        mock_cursor.executemany.assert_not_called()
        mock_conn.commit.assert_called_once()


# ── ClickHouseWriter ──────────────────────────────────────────────────────────────
class TestClickHouseWriter:
    def test_write_calls_execute(self, ch_config):
        """client.execute вызывается для каждого батча."""
        mock_client = MagicMock()
        with patch("clickhouse_driver.Client", return_value=mock_client):
            writer = ClickHouseWriter(ch_config)
            count = writer.write(HEADERS, ROWS)
        # 3 строки, batch_size=2 → 2 вызова execute
        assert mock_client.execute.call_count == 2
        assert count == 3

    def test_rows_passed_as_dicts(self, ch_config):
        """clickhouse-driver получает список словарей."""
        mock_client = MagicMock()
        with patch("clickhouse_driver.Client", return_value=mock_client):
            ClickHouseWriter(ch_config).write(HEADERS, ROWS[:1])
        passed_data = mock_client.execute.call_args_list[0][0][1]
        assert isinstance(passed_data, list)
        assert isinstance(passed_data[0], dict)
        assert passed_data[0]["id"] == 1
        assert passed_data[0]["name"] == "Alice"

    def test_disconnect_called_on_success(self, ch_config):
        """client.disconnect() вызывается после успешной записи.

        Без disconnect соединения накапливаются при долгой работе Airflow-воркера.
        """
        mock_client = MagicMock()
        with patch("clickhouse_driver.Client", return_value=mock_client):
            ClickHouseWriter(ch_config).write(HEADERS, ROWS)
        mock_client.disconnect.assert_called_once()

    def test_disconnect_called_on_error(self, ch_config):
        """client.disconnect() вызывается даже если execute упало.

        Это гарантирует отсутствие утечки соединений при ошибках.
        """
        mock_client = MagicMock()
        mock_client.execute.side_effect = RuntimeError("CH error")
        with patch("clickhouse_driver.Client", return_value=mock_client):
            with pytest.raises(RuntimeError, match="CH error"):
                ClickHouseWriter(ch_config).write(HEADERS, ROWS)
        mock_client.disconnect.assert_called_once()

    def test_clickhouse_driver_not_installed_raises(self, ch_config):
        """Если clickhouse_driver нет — ImportError с понятным сообщением."""
        import builtins
        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "clickhouse_driver":
                raise ImportError("No module named 'clickhouse_driver'")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            writer = ClickHouseWriter(ch_config)
            with pytest.raises(ImportError, match="clickhouse"):
                writer.write(HEADERS, ROWS)

    def test_empty_rows(self, ch_config):
        """Пустой список → 0 записей, execute не вызывается."""
        mock_client = MagicMock()
        with patch("clickhouse_driver.Client", return_value=mock_client):
            count = ClickHouseWriter(ch_config).write(HEADERS, [])
        assert count == 0
        mock_client.execute.assert_not_called()
        # disconnect должен вызываться даже для пустого ввода
        mock_client.disconnect.assert_called_once()
