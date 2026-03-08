"""
tests/test_csv_reader.py
========================
Тесты для readers/csv_reader.py
"""
from __future__ import annotations

import csv
import pytest
from pathlib import Path

from manual_excel_loader.readers.csv_reader import CsvReadConfig, read_csv, iter_csv


def _write_csv(path: Path, rows: list[list], delimiter: str = ",") -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=delimiter)
        writer.writerows(rows)


class TestReadCsv:

    def test_basic_read(self, tmp_path):
        f = tmp_path / "data.csv"
        _write_csv(f, [["id", "name", "age"], ["1", "Alice", "30"], ["2", "Bob", ""]])

        result = read_csv(CsvReadConfig(path=f))

        assert result.headers == ["id", "name", "age"]
        assert len(result.rows) == 2
        assert result.rows[0] == ("1", "Alice", "30")
        assert result.rows[1] == ("2", "Bob", None)   # пустая строка → None

    def test_skip_rows(self, tmp_path):
        f = tmp_path / "data.csv"
        _write_csv(f, [["meta1"], ["meta2"], ["id", "name"], ["1", "Alice"]])

        result = read_csv(CsvReadConfig(path=f, skip_rows=2))

        assert result.headers == ["id", "name"]
        assert result.rows[0] == ("1", "Alice")

    def test_skip_cols(self, tmp_path):
        f = tmp_path / "data.csv"
        _write_csv(f, [["junk", "id", "name"], ["x", "1", "Alice"]])

        result = read_csv(CsvReadConfig(path=f, skip_cols=1))

        assert result.headers == ["id", "name"]
        assert result.rows[0] == ("1", "Alice")

    def test_max_row(self, tmp_path):
        f = tmp_path / "data.csv"
        _write_csv(f, [["id"]] + [[str(i)] for i in range(100)])

        result = read_csv(CsvReadConfig(path=f, max_row=10))

        assert len(result.rows) == 10

    def test_tab_delimiter(self, tmp_path):
        f = tmp_path / "data.tsv"
        _write_csv(f, [["a", "b"], ["1", "2"]], delimiter="\t")

        result = read_csv(CsvReadConfig(path=f, delimiter="\t"))

        assert result.headers == ["a", "b"]
        assert result.rows[0] == ("1", "2")

    def test_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            read_csv(CsvReadConfig(path=tmp_path / "missing.csv"))

    def test_empty_file_raises(self, tmp_path):
        f = tmp_path / "empty.csv"
        f.write_text("")
        with pytest.raises(ValueError, match="пустой"):
            read_csv(CsvReadConfig(path=f))

    def test_headers_lowercased_and_stripped(self, tmp_path):
        f = tmp_path / "data.csv"
        _write_csv(f, [["  ID ", " Name "], ["1", "Alice"]])

        result = read_csv(CsvReadConfig(path=f))

        assert result.headers == ["id", "name"]

    def test_short_row_padded_with_none(self, tmp_path):
        """Строка короче заголовка → дополняется None."""
        f = tmp_path / "data.csv"
        _write_csv(f, [["a", "b", "c"], ["1", "2"]])

        result = read_csv(CsvReadConfig(path=f))

        assert result.rows[0] == ("1", "2", None)


class TestIterCsv:

    def test_iter_yields_rows(self, tmp_path):
        f = tmp_path / "data.csv"
        _write_csv(f, [["x", "y"], ["1", "2"], ["3", "4"]])

        rows = list(iter_csv(CsvReadConfig(path=f)))

        assert len(rows) == 2
        assert rows[0] == ("1", "2")

    def test_iter_respects_max_row(self, tmp_path):
        f = tmp_path / "data.csv"
        _write_csv(f, [["x"]] + [[str(i)] for i in range(50)])

        rows = list(iter_csv(CsvReadConfig(path=f, max_row=5)))

        assert len(rows) == 5