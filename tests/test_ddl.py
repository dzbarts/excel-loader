"""
Tests for ddl.parse_ddl().

Covers:
- Basic GP / CH round-trips
- Multi-word types (double precision, character varying, timestamp without time zone)
- Parameterised types (decimal(12,2), varchar(255), FixedString(10))
- Nullable(X) unwrapping for CH
- Comments in DDL (-- and /* */)
- Quoted identifiers ("col", `col`)
- Constraint / index lines are skipped
- Column order mismatch between DDL and Excel (dict, not list)
- Error cases: empty DDL, no parens, unbalanced parens
"""
from __future__ import annotations

import pytest
from src.manual_excel_loader.ddl import parse_ddl
from src.manual_excel_loader.enums import DatabaseType
from src.manual_excel_loader.exceptions import ConfigurationError

GP = DatabaseType.GREENPLUM
CH = DatabaseType.CLICKHOUSE

# ── GreenPlum ────────────────────────────────────────────────────────────────

class TestParseDdlGP:
    def test_basic_types(self):
        ddl = """
        CREATE TABLE s.t (
            id          integer         NOT NULL,
            full_name   text,
            hired_at    date
        ) DISTRIBUTED BY (id);
        """
        result = parse_ddl(ddl, GP)
        assert result["id"] == "integer"
        assert result["full_name"] == "text"
        assert result["hired_at"] == "date"

    def test_decimal_parameterised(self):
        ddl = "CREATE TABLE t (salary decimal(12, 2));"
        result = parse_ddl(ddl, GP)
        assert "decimal" in result["salary"]
        assert "12" in result["salary"]
        assert "2" in result["salary"]

    def test_numeric_alias(self):
        ddl = "CREATE TABLE t (price numeric(10,4));"
        result = parse_ddl(ddl, GP)
        assert "numeric" in result["price"]

    def test_character_varying(self):
        ddl = "CREATE TABLE t (code character varying(50));"
        result = parse_ddl(ddl, GP)
        assert "character varying" in result["code"] or "varchar" in result["code"]

    def test_double_precision(self):
        ddl = "CREATE TABLE t (score double precision);"
        result = parse_ddl(ddl, GP)
        assert "double precision" in result["score"]

    def test_timestamp_without_time_zone(self):
        ddl = "CREATE TABLE t (created_at timestamp without time zone);"
        result = parse_ddl(ddl, GP)
        assert "timestamp" in result["created_at"]

    def test_bigint_serial(self):
        ddl = "CREATE TABLE t (id bigserial NOT NULL, seq serial);"
        result = parse_ddl(ddl, GP)
        assert result["id"] == "bigserial"
        assert result["seq"] == "serial"

    def test_boolean(self):
        ddl = "CREATE TABLE t (is_active boolean DEFAULT true);"
        result = parse_ddl(ddl, GP)
        assert result["is_active"] == "boolean"

    def test_interval(self):
        ddl = "CREATE TABLE t (duration interval);"
        result = parse_ddl(ddl, GP)
        assert result["duration"] == "interval"

    def test_uuid(self):
        ddl = "CREATE TABLE t (uid uuid);"
        result = parse_ddl(ddl, GP)
        assert result["uid"] == "uuid"

    def test_quoted_column_names(self):
        ddl = 'CREATE TABLE t ("user_id" integer, "full_name" text);'
        result = parse_ddl(ddl, GP)
        assert "user_id" in result
        assert "full_name" in result

    def test_constraint_line_skipped(self):
        ddl = """
        CREATE TABLE t (
            id integer NOT NULL,
            name text,
            CONSTRAINT pk_t PRIMARY KEY (id)
        );
        """
        result = parse_ddl(ddl, GP)
        assert "id" in result
        assert "name" in result
        # constraint must not appear as a column
        assert not any("constraint" in k for k in result)

    def test_line_comments_ignored(self):
        ddl = """
        CREATE TABLE t (
            -- this is the primary key
            id integer NOT NULL,
            name text  -- человеческое имя
        );
        """
        result = parse_ddl(ddl, GP)
        assert result["id"] == "integer"
        assert result["name"] == "text"

    def test_block_comments_ignored(self):
        ddl = """
        /* drop first: DROP TABLE s.t; */
        CREATE TABLE s.t (
            id integer,
            val text
        );
        """
        result = parse_ddl(ddl, GP)
        assert result["id"] == "integer"

    def test_column_names_lowercase(self):
        ddl = "CREATE TABLE T (ID INTEGER, Name TEXT);"
        result = parse_ddl(ddl, GP)
        assert "id" in result
        assert "name" in result

    def test_returns_dict_not_list(self):
        ddl = "CREATE TABLE t (a integer, b text);"
        result = parse_ddl(ddl, GP)
        assert isinstance(result, dict)

    def test_order_independent(self):
        """Columns in DDL and Excel can be in different order — dict handles it."""
        ddl = "CREATE TABLE t (z text, a integer, m date);"
        result = parse_ddl(ddl, GP)
        # All three present regardless of order
        assert set(result.keys()) == {"z", "a", "m"}

    def test_multiple_columns_count(self):
        ddl = """
        CREATE TABLE t (
            id integer,
            name text,
            age smallint,
            score real,
            created date
        );
        """
        result = parse_ddl(ddl, GP)
        assert len(result) == 5

    def test_distributed_clause_not_parsed_as_column(self):
        ddl = "CREATE TABLE t (id integer) DISTRIBUTED BY (id);"
        result = parse_ddl(ddl, GP)
        assert list(result.keys()) == ["id"]

    def test_null_not_null_stripped_from_type(self):
        ddl = "CREATE TABLE t (id integer NOT NULL, val text NULL);"
        result = parse_ddl(ddl, GP)
        assert result["id"] == "integer"
        assert result["val"] == "text"


# ── ClickHouse ───────────────────────────────────────────────────────────────

class TestParseDdlCH:
    def test_basic_types(self):
        ddl = """
        CREATE TABLE db.t
        (
            `id`        Int32,
            `name`      String,
            `load_dttm` DateTime
        )
        ENGINE = MergeTree
        ORDER BY id;
        """
        result = parse_ddl(ddl, CH)
        assert result["id"] == "Int32"
        assert result["name"] == "String"
        assert result["load_dttm"] == "DateTime"

    def test_nullable_unwrapped(self):
        ddl = "CREATE TABLE t (`val` Nullable(Int64)) ENGINE = MergeTree ORDER BY val;"
        result = parse_ddl(ddl, CH)
        # Nullable wrapper should be stripped
        assert result["val"] == "Int64"

    def test_nullable_decimal_unwrapped(self):
        ddl = "CREATE TABLE t (`price` Nullable(Decimal(18, 4))) ENGINE = MergeTree ORDER BY price;"
        result = parse_ddl(ddl, CH)
        assert "Decimal" in result["price"]
        assert "Nullable" not in result["price"]

    def test_date32(self):
        ddl = "CREATE TABLE t (`d` Date32) ENGINE = MergeTree ORDER BY d;"
        result = parse_ddl(ddl, CH)
        assert result["d"] == "Date32"

    def test_datetime64(self):
        ddl = "CREATE TABLE t (`ts` DateTime64(6)) ENGINE = MergeTree ORDER BY ts;"
        result = parse_ddl(ddl, CH)
        assert "DateTime64" in result["ts"]

    def test_uint_types(self):
        ddl = "CREATE TABLE t (`a` UInt32, `b` UInt64) ENGINE = MergeTree ORDER BY a;"
        result = parse_ddl(ddl, CH)
        assert result["a"] == "UInt32"
        assert result["b"] == "UInt64"

    def test_bool(self):
        ddl = "CREATE TABLE t (`flag` Bool) ENGINE = MergeTree ORDER BY flag;"
        result = parse_ddl(ddl, CH)
        assert result["flag"] == "Bool"

    def test_fixed_string(self):
        ddl = "CREATE TABLE t (`code` FixedString(10)) ENGINE = MergeTree ORDER BY code;"
        result = parse_ddl(ddl, CH)
        assert "FixedString" in result["code"]
        assert "10" in result["code"]

    def test_default_clause_stripped(self):
        ddl = "CREATE TABLE t (`load_dttm` Nullable(DateTime) DEFAULT NULL) ENGINE = MergeTree ORDER BY load_dttm;"
        result = parse_ddl(ddl, CH)
        assert result["load_dttm"] == "DateTime"

    def test_backtick_names_normalised(self):
        ddl = "CREATE TABLE t (`MyColumn` String) ENGINE = MergeTree ORDER BY MyColumn;"
        result = parse_ddl(ddl, CH)
        assert "mycolumn" in result

    def test_engine_clause_not_a_column(self):
        ddl = "CREATE TABLE t (`id` Int32) ENGINE = MergeTree ORDER BY id;"
        result = parse_ddl(ddl, CH)
        assert list(result.keys()) == ["id"]

    def test_line_comments_ignored(self):
        ddl = """
        CREATE TABLE t (
            `id`   Int32,  -- primary
            `name` String  -- human name
        ) ENGINE = MergeTree ORDER BY id;
        """
        result = parse_ddl(ddl, CH)
        assert result["id"] == "Int32"

    def test_multiple_columns(self):
        ddl = """
        CREATE TABLE t (
            `id`       Int32,
            `name`     Nullable(String),
            `score`    Nullable(Float64),
            `hired_at` Nullable(Date32),
            `flag`     Bool
        ) ENGINE = MergeTree ORDER BY id;
        """
        result = parse_ddl(ddl, CH)
        assert len(result) == 5
        assert result["name"] == "String"
        assert result["score"] == "Float64"


# ── Error cases ───────────────────────────────────────────────────────────────

class TestParseDdlErrors:
    def test_empty_string(self):
        with pytest.raises(ConfigurationError, match="empty"):
            parse_ddl("", GP)

    def test_whitespace_only(self):
        with pytest.raises(ConfigurationError):
            parse_ddl("   \n\t  ", GP)

    def test_no_parentheses(self):
        with pytest.raises(ConfigurationError, match="parenthes"):
            parse_ddl("CREATE TABLE t;", GP)

    def test_unbalanced_parens(self):
        with pytest.raises(ConfigurationError, match="unbalanced"):
            parse_ddl("CREATE TABLE t (id integer", GP)