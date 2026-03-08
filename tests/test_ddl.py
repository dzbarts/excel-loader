"""
tests/test_ddl.py
=================
Тесты парсера DDL (src/manual_excel_loader/ddl.py).

ddl.py не имел ни одного теста — это исправляется здесь.
Покрываем: GP и CH диалекты, составные типы, Nullable, комментарии,
граничные случаи (пустой DDL, несбалансированные скобки).
"""
from __future__ import annotations

import pytest

from manual_excel_loader.ddl import parse_ddl
from manual_excel_loader.enums import DatabaseType
from manual_excel_loader.exceptions import ConfigurationError


GP = DatabaseType.GREENPLUM
CH = DatabaseType.CLICKHOUSE


# ── GreenPlum ─────────────────────────────────────────────────────────────────
class TestParseGP:
    def test_basic_types(self):
        ddl = """
        CREATE TABLE hr.employees (
            id        integer NOT NULL,
            full_name text,
            hired_at  date
        ) DISTRIBUTED BY (id);
        """
        result = parse_ddl(ddl, GP)
        assert result == {
            "id": "integer",
            "full_name": "text",
            "hired_at": "date",
        }

    def test_decimal_with_precision(self):
        ddl = "CREATE TABLE t (salary decimal(12, 2) NOT NULL);"
        result = parse_ddl(ddl, GP)
        assert result["salary"] == "decimal(12, 2)"

    def test_varchar_with_length(self):
        ddl = "CREATE TABLE t (code character varying(50));"
        result = parse_ddl(ddl, GP)
        assert result["code"] == "character varying(50)"

    def test_timestamp_types(self):
        ddl = """
        CREATE TABLE t (
            created_at  timestamp without time zone,
            updated_at  timestamp with time zone
        );
        """
        result = parse_ddl(ddl, GP)
        assert result["created_at"] == "timestamp without time zone"
        assert result["updated_at"] == "timestamp with time zone"

    def test_double_precision(self):
        ddl = "CREATE TABLE t (ratio double precision);"
        result = parse_ddl(ddl, GP)
        assert result["ratio"] == "double precision"

    def test_boolean_type(self):
        ddl = "CREATE TABLE t (is_active boolean DEFAULT true);"
        result = parse_ddl(ddl, GP)
        assert result["is_active"] == "boolean"

    def test_skips_constraints(self):
        ddl = """
        CREATE TABLE t (
            id    integer,
            name  text,
            CONSTRAINT pk_t PRIMARY KEY (id)
        );
        """
        result = parse_ddl(ddl, GP)
        assert set(result.keys()) == {"id", "name"}

    def test_strips_line_comments(self):
        ddl = """
        CREATE TABLE t (
            -- this is the primary key
            id integer NOT NULL, -- inline comment
            name text
        );
        """
        result = parse_ddl(ddl, GP)
        assert set(result.keys()) == {"id", "name"}

    def test_strips_block_comments(self):
        ddl = """
        /* Table for employees */
        CREATE TABLE t (
            id   integer,
            name /* strange comment */ text
        );
        """
        result = parse_ddl(ddl, GP)
        assert "id" in result

    def test_column_names_lowercased(self):
        ddl = 'CREATE TABLE t ("MyCol" integer, "UPPER" text);'
        result = parse_ddl(ddl, GP)
        assert "mycol" in result
        assert "upper" in result


# ── ClickHouse ────────────────────────────────────────────────────────────────
class TestParseCH:
    def test_basic_types(self):
        ddl = """
        CREATE TABLE default.events (
            `id`         Int32,
            `event_name` String,
            `created_at` Date
        ) ENGINE = MergeTree ORDER BY id;
        """
        result = parse_ddl(ddl, CH)
        assert result["id"] == "Int32"
        assert result["event_name"] == "String"
        assert result["created_at"] == "Date"

    def test_nullable_unwrapped(self):
        """Nullable(X) должен разворачиваться в X для валидатора."""
        ddl = """
        CREATE TABLE t (
            `amount` Nullable(Decimal(18, 2)),
            `note`   Nullable(String)
        );
        """
        result = parse_ddl(ddl, CH)
        assert result["amount"] == "Decimal(18, 2)"
        assert result["note"] == "String"

    def test_decimal_type(self):
        ddl = "CREATE TABLE t (`price` Decimal(10, 4)) ENGINE = MergeTree ORDER BY price;"
        result = parse_ddl(ddl, CH)
        assert result["price"] == "Decimal(10, 4)"

    def test_datetime64_type(self):
        ddl = "CREATE TABLE t (`ts` DateTime64(6)) ENGINE = MergeTree ORDER BY ts;"
        result = parse_ddl(ddl, CH)
        assert result["ts"] == "DateTime64(6)"

    def test_skips_index_lines(self):
        ddl = """
        CREATE TABLE t (
            `id`   Int32,
            `name` String,
            INDEX idx_name name TYPE minmax GRANULARITY 4
        ) ENGINE = MergeTree ORDER BY id;
        """
        result = parse_ddl(ddl, CH)
        assert set(result.keys()) == {"id", "name"}

    def test_column_names_lowercased(self):
        ddl = "CREATE TABLE t (`MyCol` Int32, `UPPER` String);"
        result = parse_ddl(ddl, CH)
        assert "mycol" in result
        assert "upper" in result


# ── Error cases ───────────────────────────────────────────────────────────────
class TestParseErrors:
    def test_empty_ddl_raises(self):
        with pytest.raises(ConfigurationError, match="empty"):
            parse_ddl("", GP)

    def test_whitespace_only_raises(self):
        with pytest.raises(ConfigurationError, match="empty"):
            parse_ddl("   \n\t  ", GP)

    def test_no_parentheses_raises(self):
        with pytest.raises(ConfigurationError, match="parentheses"):
            parse_ddl("CREATE TABLE t", GP)

    def test_unbalanced_parens_raises(self):
        with pytest.raises(ConfigurationError, match="unbalanced"):
            parse_ddl("CREATE TABLE t (id integer, name text", GP)

    def test_no_columns_found_raises(self):
        # Синтаксически валидный DDL, но без распознаваемых колонок
        with pytest.raises(ConfigurationError, match="no column definitions"):
            parse_ddl("CREATE TABLE t (CONSTRAINT pk PRIMARY KEY (id));", GP)