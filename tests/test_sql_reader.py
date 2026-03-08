"""
tests/test_sql_reader.py
========================
Тесты для readers/sql_reader.py
"""
from __future__ import annotations

import pytest
from pathlib import Path

from manual_excel_loader.readers.sql_reader import SqlReadConfig, read_sql, iter_sql


def _write_sql(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


class TestReadSql:

    def test_basic_single_insert(self, tmp_path):
        f = tmp_path / "data.sql"
        _write_sql(f, "INSERT INTO myschema.users (id, name) VALUES (1, 'Alice');")

        result = read_sql(SqlReadConfig(path=f))

        assert result.headers == ["id", "name"]
        assert result.rows == [(1, "Alice")]
        assert result.table_name == "users"

    def test_batch_insert(self, tmp_path):
        f = tmp_path / "data.sql"
        _write_sql(f, """
            INSERT INTO users (id, name) VALUES
                (1, 'Alice'),
                (2, 'Bob'),
                (3, 'Charlie');
        """)

        result = read_sql(SqlReadConfig(path=f))

        assert len(result.rows) == 3
        assert result.rows[1] == (2, "Bob")

    def test_multiple_inserts_same_table(self, tmp_path):
        f = tmp_path / "data.sql"
        _write_sql(f, """
            INSERT INTO users (id, name) VALUES (1, 'Alice');
            INSERT INTO users (id, name) VALUES (2, 'Bob');
        """)

        result = read_sql(SqlReadConfig(path=f))

        assert len(result.rows) == 2

    def test_null_values(self, tmp_path):
        f = tmp_path / "data.sql"
        _write_sql(f, "INSERT INTO t (a, b, c) VALUES (1, NULL, 'x');")

        result = read_sql(SqlReadConfig(path=f))

        assert result.rows[0] == (1, None, "x")

    def test_boolean_values(self, tmp_path):
        f = tmp_path / "data.sql"
        _write_sql(f, "INSERT INTO t (a, b) VALUES (TRUE, FALSE);")

        result = read_sql(SqlReadConfig(path=f))

        assert result.rows[0] == (True, False)

    def test_target_table_selection(self, tmp_path):
        f = tmp_path / "data.sql"
        _write_sql(f, """
            INSERT INTO schema1.users (id) VALUES (1);
            INSERT INTO schema2.orders (order_id) VALUES (100);
        """)

        result = read_sql(SqlReadConfig(path=f, target_table="orders"))

        assert result.headers == ["order_id"]
        assert result.rows[0] == (100,)

    def test_target_table_not_found_raises(self, tmp_path):
        f = tmp_path / "data.sql"
        _write_sql(f, "INSERT INTO users (id) VALUES (1);")

        with pytest.raises(ValueError, match="orders"):
            read_sql(SqlReadConfig(path=f, target_table="orders"))

    def test_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            read_sql(SqlReadConfig(path=tmp_path / "missing.sql"))

    def test_empty_file_raises(self, tmp_path):
        f = tmp_path / "empty.sql"
        f.write_text("-- only a comment")
        with pytest.raises(ValueError, match="INSERT"):
            read_sql(SqlReadConfig(path=f))

    def test_comments_stripped(self, tmp_path):
        f = tmp_path / "data.sql"
        _write_sql(f, """
            -- This is a comment
            /* block comment */
            INSERT INTO t (x) VALUES (42);
        """)

        result = read_sql(SqlReadConfig(path=f))

        assert result.rows == [(42,)]

    def test_integer_and_float_coercion(self, tmp_path):
        f = tmp_path / "data.sql"
        _write_sql(f, "INSERT INTO t (a, b, c) VALUES (10, 3.14, 'text');")

        result = read_sql(SqlReadConfig(path=f))

        assert result.rows[0] == (10, 3.14, "text")
        assert isinstance(result.rows[0][0], int)
        assert isinstance(result.rows[0][1], float)


class TestIterSql:

    def test_iter_yields_rows(self, tmp_path):
        f = tmp_path / "data.sql"
        _write_sql(f, """
            INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y');
        """)

        rows = list(iter_sql(SqlReadConfig(path=f)))

        assert len(rows) == 2
        assert rows[0] == (1, "x")