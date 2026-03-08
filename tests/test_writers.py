"""
Tests for writers: escape functions, SqlFileWriter, CsvFileWriter.
"""
from __future__ import annotations

import csv
from pathlib import Path

import pytest

# We test the private escape functions directly — they are the most critical
# correctness boundary and easiest to break silently.
from manual_excel_loader.writers.sql_file import _escape_gp, _escape_ch, _format_insert
from manual_excel_loader.writers.sql_file import SqlFileWriter
from manual_excel_loader.writers.csv_file import CsvFileWriter
from manual_excel_loader.writers.base import FileWriterConfig
from manual_excel_loader.enums import DatabaseType


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def gp_config(tmp_path: Path) -> FileWriterConfig:
    return FileWriterConfig(
        output_path=tmp_path / "out.sql",
        db_type=DatabaseType.GREENPLUM,
        table_name="employees",
        scheme_name="hr",
    )


@pytest.fixture()
def ch_config(tmp_path: Path) -> FileWriterConfig:
    return FileWriterConfig(
        output_path=tmp_path / "out.sql",
        db_type=DatabaseType.CLICKHOUSE,
        table_name="employees",
        scheme_name="hr",
    )


@pytest.fixture()
def csv_config(tmp_path: Path) -> FileWriterConfig:
    return FileWriterConfig(
        output_path=tmp_path / "out.csv",
        db_type=DatabaseType.GREENPLUM,
        table_name="employees",
        scheme_name="hr",
    )


# ── _escape_gp ────────────────────────────────────────────────────────────────

class TestEscapeGP:
    def test_none_is_null(self):
        assert _escape_gp(None) == "NULL"

    def test_true(self):
        assert _escape_gp(True) == "TRUE"

    def test_false(self):
        assert _escape_gp(False) == "FALSE"

    def test_integer(self):
        assert _escape_gp(42) == "42"

    def test_negative_integer(self):
        assert _escape_gp(-7) == "-7"

    def test_float(self):
        assert _escape_gp(3.14) == "3.14"

    def test_plain_string(self):
        assert _escape_gp("hello") == "'hello'"

    def test_single_quote_doubled(self):
        # Standard SQL: O'Brien → 'O''Brien'
        assert _escape_gp("O'Brien") == "'O''Brien'"

    def test_backslash_not_escaped(self):
        # GP with standard_conforming_strings=on: backslash is literal
        assert _escape_gp("C:\\path") == "'C:\\path'"

    def test_newline_not_escaped(self):
        # GP does not escape \n in standard mode
        assert _escape_gp("line1\nline2") == "'line1\nline2'"

    def test_bool_before_int(self):
        # bool is subclass of int — must NOT render as 1/0
        assert _escape_gp(True) != "1"
        assert _escape_gp(False) != "0"


# ── _escape_ch ────────────────────────────────────────────────────────────────

class TestEscapeCH:
    def test_none_is_null(self):
        assert _escape_ch(None) == "NULL"

    def test_true_is_1(self):
        # CH Bool: safest cross-version form is integer
        assert _escape_ch(True) == "1"

    def test_false_is_0(self):
        assert _escape_ch(False) == "0"

    def test_integer(self):
        assert _escape_ch(42) == "42"

    def test_float(self):
        assert _escape_ch(3.14) == "3.14"

    def test_plain_string(self):
        assert _escape_ch("hello") == "'hello'"

    def test_single_quote_backslash_escaped(self):
        # CH: O'Brien → 'O\'Brien'  (NOT doubled like GP)
        assert _escape_ch("O'Brien") == r"'O\'Brien'"

    def test_backslash_doubled(self):
        # CH: backslash must be doubled
        assert _escape_ch("C:\\path") == r"'C:\\path'"

    def test_newline_escaped(self):
        assert _escape_ch("line1\nline2") == r"'line1\nline2'"

    def test_tab_escaped(self):
        assert _escape_ch("col1\tcol2") == r"'col1\tcol2'"

    def test_null_byte_escaped(self):
        assert _escape_ch("a\0b") == r"'a\0b'"

    def test_bool_before_int(self):
        # bool is subclass of int — must render as 1/0, not as "True"/"False"
        assert _escape_ch(True) == "1"
        assert _escape_ch(False) == "0"


# ── GP vs CH differ on the critical cases ────────────────────────────────────

class TestEscapeDifferences:
    """Explicitly document where GP and CH diverge."""

    def test_single_quote_differs(self):
        gp = _escape_gp("O'Brien")
        ch = _escape_ch("O'Brien")
        assert gp == "'O''Brien'"     # GP: double
        assert ch == r"'O\'Brien'"    # CH: backslash
        assert gp != ch

    def test_backslash_differs(self):
        gp = _escape_gp("a\\b")
        ch = _escape_ch("a\\b")
        assert gp == "'a\\b'"         # GP: literal
        assert ch == r"'a\\b'"        # CH: doubled
        assert gp != ch

    def test_bool_differs(self):
        assert _escape_gp(True) == "TRUE"
        assert _escape_ch(True) == "1"


# ── SqlFileWriter ─────────────────────────────────────────────────────────────

class TestSqlFileWriterGP:
    def test_creates_file(self, gp_config):
        SqlFileWriter(gp_config).write(["id", "name"], [(1, "Alice")])
        assert gp_config.output_path.exists()

    def test_insert_structure(self, gp_config):
        SqlFileWriter(gp_config).write(["id", "name"], [(1, "Alice")])
        content = gp_config.output_path.read_text()
        assert "INSERT INTO hr.employees (id, name)" in content
        assert "VALUES" in content
        assert "(1, 'Alice')" in content

    def test_null_value(self, gp_config):
        SqlFileWriter(gp_config).write(["id", "val"], [(1, None)])
        assert "NULL" in gp_config.output_path.read_text()

    def test_batching_produces_multiple_inserts(self, tmp_path):
        config = FileWriterConfig(
            output_path=tmp_path / "out.sql",
            db_type=DatabaseType.GREENPLUM,
            table_name="t", scheme_name="s",
            batch_size=2,
        )
        rows = [(i, f"name{i}") for i in range(5)]
        SqlFileWriter(config).write(["id", "name"], rows)
        content = config.output_path.read_text()
        # 5 rows with batch_size=2 → 3 INSERT statements
        assert content.count("INSERT INTO") == 3

    def test_single_quote_in_string_escaped(self, gp_config):
        SqlFileWriter(gp_config).write(["name"], [("O'Brien",)])
        assert "O''Brien" in gp_config.output_path.read_text()

    def test_generator_rows_consumed_once(self, gp_config):
        rows = ((i,) for i in range(3))
        SqlFileWriter(gp_config).write(["id"], rows)
        content = gp_config.output_path.read_text()
        assert "0" in content and "1" in content and "2" in content

    def test_empty_rows_creates_empty_file(self, gp_config):
        SqlFileWriter(gp_config).write(["id"], [])
        assert gp_config.output_path.read_text() == ""


class TestSqlFileWriterCH:
    def test_ch_escape_used_for_quotes(self, ch_config):
        SqlFileWriter(ch_config).write(["name"], [("O'Brien",)])
        content = ch_config.output_path.read_text()
        # CH: backslash escape, not doubling
        assert r"O\'Brien" in content
        assert "O''Brien" not in content

    def test_bool_rendered_as_integer(self, ch_config):
        SqlFileWriter(ch_config).write(["flag"], [(True,)])
        content = ch_config.output_path.read_text()
        assert "(1)" in content


# ── CsvFileWriter ─────────────────────────────────────────────────────────────

class TestCsvFileWriter:
    def test_creates_file(self, csv_config):
        CsvFileWriter(csv_config).write(["id", "name"], [(1, "Alice")])
        assert csv_config.output_path.exists()

    def test_header_row_present(self, csv_config):
        CsvFileWriter(csv_config).write(["id", "name"], [(1, "Alice")])
        rows = csv_config.output_path.read_text().splitlines()
        assert rows[0] == "id,name"

    def test_data_row(self, csv_config):
        CsvFileWriter(csv_config).write(["id", "name"], [(1, "Alice")])
        rows = csv_config.output_path.read_text().splitlines()
        assert rows[1] == "1,Alice"

    def test_semicolon_delimiter(self, tmp_path):
        config = FileWriterConfig(
            output_path=tmp_path / "out.csv",
            db_type=DatabaseType.GREENPLUM,
            table_name="t", scheme_name="s",
            delimiter=";",
        )
        CsvFileWriter(config).write(["a", "b"], [(1, 2)])
        content = config.output_path.read_text()
        assert "a;b" in content
        assert "1;2" in content

    def test_tab_delimiter(self, tmp_path):
        config = FileWriterConfig(
            output_path=tmp_path / "out.csv",
            db_type=DatabaseType.GREENPLUM,
            table_name="t", scheme_name="s",
            delimiter="\t",
        )
        CsvFileWriter(config).write(["x", "y"], [("hello", "world")])
        content = config.output_path.read_text()
        assert "x\ty" in content

    def test_none_written_as_empty(self, csv_config):
        CsvFileWriter(csv_config).write(["id", "val"], [(1, None)])
        rows = csv_config.output_path.read_text().splitlines()
        assert rows[1] == "1,"

    def test_string_with_comma_quoted(self, csv_config):
        CsvFileWriter(csv_config).write(["name"], [("Smith, John",)])
        content = csv_config.output_path.read_text()
        assert '"Smith, John"' in content

    def test_empty_rows_writes_only_header(self, csv_config):
        CsvFileWriter(csv_config).write(["id", "name"], [])
        rows = csv_config.output_path.read_text().splitlines()
        assert len(rows) == 1
        assert rows[0] == "id,name"

    def test_encoding_respected(self, tmp_path):
        config = FileWriterConfig(
            output_path=tmp_path / "out.csv",
            db_type=DatabaseType.GREENPLUM,
            table_name="t", scheme_name="s",
            encoding="cp1251",
        )
        CsvFileWriter(config).write(["name"], [("Иванов",)])
        content = config.output_path.read_text(encoding="cp1251")
        assert "Иванов" in content