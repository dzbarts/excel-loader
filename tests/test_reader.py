"""
tests/test_reader.py
====================

Тесты для readers/excel_reader.py.
Используют реальные xlsx-файлы через tmp_path — без мока openpyxl.

Запуск:
    pytest tests/test_reader.py -v
"""

from __future__ import annotations

from pathlib import Path

import openpyxl
import pytest

from manual_excel_loader.exceptions import FileReadError, HeaderValidationError
from manual_excel_loader.readers.excel_reader import ExcelReadConfig, SheetData, read_excel


# ── Helpers & fixtures ────────────────────────────────────────────────────────

def _make_xlsx(path: Path, rows: list[list], sheet_name: str = "Sheet1") -> Path:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = sheet_name
    for row in rows:
        ws.append(row)
    wb.save(path)
    return path


@pytest.fixture
def simple_xlsx(tmp_path: Path) -> Path:
    return _make_xlsx(
        tmp_path / "simple.xlsx",
        [["id", "name", "amount"], [1, "alice", 100], [2, "bob", 200]],
    )


@pytest.fixture
def skip_rows_xlsx(tmp_path: Path) -> Path:
    return _make_xlsx(
        tmp_path / "skip_rows.xlsx",
        [["meta", "info", None], ["report", "2024", None], ["id", "name", "amount"], [1, "alice", 100]],
    )


@pytest.fixture
def skip_cols_xlsx(tmp_path: Path) -> Path:
    return _make_xlsx(
        tmp_path / "skip_cols.xlsx",
        [[None, "id", "name"], [None, 1, "alice"], [None, 2, "bob"]],
    )


@pytest.fixture
def trailing_blank_rows_xlsx(tmp_path: Path) -> Path:
    return _make_xlsx(
        tmp_path / "blanks.xlsx",
        [["id", "name"], [1, "alice"], [None, None], [None, None]],
    )


# ── Happy path ────────────────────────────────────────────────────────────────

class TestReadExcelHappyPath:
    def test_returns_sheet_data(self, simple_xlsx):
        assert isinstance(read_excel(ExcelReadConfig(path=simple_xlsx)), SheetData)

    def test_headers_are_lowercase(self, simple_xlsx):
        assert read_excel(ExcelReadConfig(path=simple_xlsx)).headers == ["id", "name", "amount"]

    def test_rows_iterator_yields_tuples(self, simple_xlsx):
        rows = list(read_excel(ExcelReadConfig(path=simple_xlsx)).rows)
        assert all(isinstance(r, tuple) for r in rows)

    def test_correct_row_count(self, simple_xlsx):
        assert len(list(read_excel(ExcelReadConfig(path=simple_xlsx)).rows)) == 2

    def test_row_values_match(self, simple_xlsx):
        rows = list(read_excel(ExcelReadConfig(path=simple_xlsx)).rows)
        assert rows[0] == (1, "alice", 100)
        assert rows[1] == (2, "bob", 200)

    def test_headers_stripped_of_whitespace(self, tmp_path):
        path = _make_xlsx(tmp_path / "spaces.xlsx", [[" id ", " name "], [1, "x"]])
        assert read_excel(ExcelReadConfig(path=path)).headers == ["id", "name"]

    def test_headers_converted_to_lowercase(self, tmp_path):
        path = _make_xlsx(tmp_path / "upper.xlsx", [["ID", "Name"], [1, "x"]])
        assert read_excel(ExcelReadConfig(path=path)).headers == ["id", "name"]


# ── skip_rows / skip_cols ─────────────────────────────────────────────────────

class TestSkipOffsets:
    def test_skip_rows(self, skip_rows_xlsx):
        result = read_excel(ExcelReadConfig(path=skip_rows_xlsx, skip_rows=2))
        assert result.headers == ["id", "name", "amount"]
        assert list(result.rows)[0][0] == 1

    def test_skip_cols(self, skip_cols_xlsx):
        result = read_excel(ExcelReadConfig(path=skip_cols_xlsx, skip_cols=1))
        assert result.headers == ["id", "name"]
        assert list(result.rows)[0] == (1, "alice")


# ── max_row ───────────────────────────────────────────────────────────────────

class TestMaxRow:
    def test_max_row_limits_output(self, simple_xlsx):
        rows = list(read_excel(ExcelReadConfig(path=simple_xlsx, max_row=2)).rows)
        assert len(rows) == 1


# ── Blank rows ────────────────────────────────────────────────────────────────

class TestBlankRowFiltering:
    def test_blank_rows_are_skipped(self, trailing_blank_rows_xlsx):
        rows = list(read_excel(ExcelReadConfig(path=trailing_blank_rows_xlsx)).rows)
        assert len(rows) == 1
        assert rows[0][0] == 1


# ── Named sheet ───────────────────────────────────────────────────────────────

class TestSheetSelection:
    def test_read_named_sheet(self, tmp_path):
        path = tmp_path / "named.xlsx"
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "sales"
        ws.append(["product", "qty"])
        ws.append(["widget", 5])
        wb.save(path)
        result = read_excel(ExcelReadConfig(path=path, sheet_name="sales"))
        assert result.headers == ["product", "qty"]

    def test_unknown_sheet_raises(self, simple_xlsx):
        with pytest.raises(FileReadError, match="does_not_exist"):
            read_excel(ExcelReadConfig(path=simple_xlsx, sheet_name="does_not_exist"))


# ── Error handling ────────────────────────────────────────────────────────────

class TestErrorHandling:
    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileReadError, match="not found"):
            read_excel(ExcelReadConfig(path=tmp_path / "ghost.xlsx"))

    def test_empty_header_raises(self, tmp_path):
        path = _make_xlsx(tmp_path / "empty_header.xlsx", [[None, None], [1, 2]])
        with pytest.raises(HeaderValidationError, match="empty"):
            read_excel(ExcelReadConfig(path=path))

    def test_invalid_header_chars_raises(self, tmp_path):
        path = _make_xlsx(tmp_path / "bad_header.xlsx", [["col-1", "col2"], [1, 2]])
        with pytest.raises(HeaderValidationError, match="invalid characters"):
            read_excel(ExcelReadConfig(path=path))

    def test_cyrillic_header_raises(self, tmp_path):
        path = _make_xlsx(tmp_path / "cyrillic.xlsx", [["имя", "сумма"], [1, 2]])
        with pytest.raises(HeaderValidationError, match="invalid characters"):
            read_excel(ExcelReadConfig(path=path))

    def test_duplicate_headers_raises(self, tmp_path):
        path = _make_xlsx(tmp_path / "dupes.xlsx", [["id", "id", "name"], [1, 2, "x"]])
        with pytest.raises(HeaderValidationError, match="duplicate"):
            read_excel(ExcelReadConfig(path=path))


# ── Workbook lifecycle ────────────────────────────────────────────────────────

class TestWorkbookLifecycle:
    def test_workbook_closed_after_full_iteration(self, simple_xlsx):
        result = read_excel(ExcelReadConfig(path=simple_xlsx))
        list(result.rows)
        _make_xlsx(simple_xlsx, [["id"], [99]])  # упадёт на Windows если файл ещё открыт

    def test_workbook_closed_on_early_break(self, simple_xlsx):
        result = read_excel(ExcelReadConfig(path=simple_xlsx))
        for _ in result.rows:
            break
        result.rows.close()  # type: ignore[union-attr]
        _make_xlsx(simple_xlsx, [["id"], [99]])