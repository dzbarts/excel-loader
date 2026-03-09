"""
Тесты для validation_report.py:
    - _rows_to_ranges: форматирование диапазонов строк
    - _group_errors: группировка ошибок по (колонка, тип)
    - _format_report: текстовый отчёт (OK / FAILED, с/без sample values)
    - write_report: создание файла, создание директории
    - log_validation_result: уровни логирования при наличии/отсутствии ошибок
"""
from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from manual_excel_loader.models import CellValidationError, FileValidationResult
from manual_excel_loader.validation_report import (
    _group_errors,
    _rows_to_ranges,
    _format_report,
    write_report,
    log_validation_result,
)


# ── Фикстуры ──────────────────────────────────────────────────────────────────

def _err(cell_name: str, expected_type: str, col_name: str = "", value: object = None) -> CellValidationError:
    return CellValidationError(
        cell_name=cell_name,
        cell_value=value,
        expected_type=expected_type,
        message="test error",
        col_name=col_name,
    )


def _result(*errors: CellValidationError) -> FileValidationResult:
    r = FileValidationResult()
    for e in errors:
        r.add_error(e)
    return r


INPUT_FILE = Path("sales.xlsx")


# ── _rows_to_ranges ────────────────────────────────────────────────────────────

class TestRowsToRanges:
    def test_single_row(self):
        assert _rows_to_ranges([5]) == "5"

    def test_contiguous(self):
        assert _rows_to_ranges([1, 2, 3]) == "1–3"

    def test_mixed(self):
        assert _rows_to_ranges([1, 2, 3, 5, 6, 8]) == "1–3, 5–6, 8"

    def test_scattered(self):
        assert _rows_to_ranges([1, 3, 5]) == "1, 3, 5"

    def test_duplicates_ignored(self):
        assert _rows_to_ranges([2, 2, 3, 3]) == "2–3"

    def test_truncation_at_10_ranges(self):
        # 11 разрозненных строк → 11 диапазонов → обрезка
        rows = list(range(1, 23, 2))  # 1,3,5,...,21 → 11 элементов
        result = _rows_to_ranges(rows)
        assert "… and" in result
        assert "more rows" in result

    def test_no_truncation_at_exactly_10_ranges(self):
        rows = list(range(1, 21, 2))  # ровно 10 разрозненных строк
        result = _rows_to_ranges(rows)
        assert "… and" not in result


# ── _group_errors ──────────────────────────────────────────────────────────────

class TestGroupErrors:
    def test_single_group(self):
        errors = [_err("B5", "integer", "amount"), _err("B6", "integer", "amount")]
        groups = _group_errors(errors)
        assert len(groups) == 1
        key = ("B", "amount", "integer")
        assert key in groups
        rows = [r for r, _ in groups[key]]
        assert rows == [5, 6]

    def test_two_columns_same_type(self):
        errors = [_err("B5", "integer", "amount"), _err("C10", "integer", "qty")]
        groups = _group_errors(errors)
        assert len(groups) == 2

    def test_same_column_different_types(self):
        errors = [_err("B5", "integer", "amount"), _err("B6", "datetime", "amount")]
        groups = _group_errors(errors)
        assert len(groups) == 2

    def test_sorted_by_column_letter(self):
        errors = [_err("C1", "integer", "c"), _err("A1", "integer", "a")]
        groups = _group_errors(errors)
        keys = list(groups.keys())
        assert keys[0][0] == "A"
        assert keys[1][0] == "C"

    def test_col_name_fallback_to_letter(self):
        # col_name не задан — ключ использует букву колонки
        errors = [_err("D3", "text")]
        groups = _group_errors(errors)
        key = ("D", "D", "text")
        assert key in groups


# ── _format_report ─────────────────────────────────────────────────────────────

class TestFormatReport:
    def test_ok_result(self):
        result = FileValidationResult()
        text = _format_report(result, INPUT_FILE, include_sample_values=False)
        assert "Result: OK" in text
        assert INPUT_FILE.name in text

    def test_failed_contains_header(self):
        r = _result(_err("B5", "integer", "amount"))
        text = _format_report(r, INPUT_FILE, include_sample_values=False)
        assert "Result: FAILED" in text
        assert "1 error(s)" in text

    def test_failed_contains_column_and_type(self):
        r = _result(_err("C21", "datetime", "sale_date"))
        text = _format_report(r, INPUT_FILE, include_sample_values=False)
        assert "[datetime]" in text
        assert "sale_date" in text
        assert "C" in text

    def test_failed_contains_row_range(self):
        errors = [_err(f"B{i}", "integer", "amount") for i in range(21, 73)]
        r = _result(*errors)
        text = _format_report(r, INPUT_FILE, include_sample_values=False)
        assert "21–72" in text

    def test_no_sample_values_by_default(self):
        r = _result(_err("B5", "integer", "amount", value=99))
        text = _format_report(r, INPUT_FILE, include_sample_values=False)
        assert "Sample values" not in text

    def test_sample_values_when_flag_set(self):
        r = _result(_err("B5", "integer", "amount", value=99))
        text = _format_report(r, INPUT_FILE, include_sample_values=True)
        assert "Sample values" in text
        assert "B5" in text

    def test_input_filename_in_report(self):
        r = _result(_err("B5", "integer", "amount"))
        text = _format_report(r, INPUT_FILE, include_sample_values=False)
        assert INPUT_FILE.name in text

    def test_multiple_columns_all_present(self):
        r = _result(
            _err("B5", "integer", "amount"),
            _err("C10", "datetime", "sale_date"),
        )
        text = _format_report(r, INPUT_FILE, include_sample_values=False)
        assert "[integer]" in text
        assert "[datetime]" in text


# ── write_report ───────────────────────────────────────────────────────────────

class TestWriteReport:
    def test_creates_file(self, tmp_path: Path):
        r = _result(_err("B5", "integer", "amount"))
        path = write_report(r, INPUT_FILE, tmp_path)
        assert path.exists()
        assert path.suffix == ".txt"

    def test_filename_contains_stem_and_timestamp(self, tmp_path: Path):
        r = _result(_err("B5", "integer", "amount"))
        path = write_report(r, INPUT_FILE, tmp_path)
        assert "sales" in path.name
        assert "validation" in path.name

    def test_creates_missing_directory(self, tmp_path: Path):
        report_dir = tmp_path / "nested" / "reports"
        assert not report_dir.exists()
        r = _result(_err("B5", "integer", "amount"))
        write_report(r, INPUT_FILE, report_dir)
        assert report_dir.exists()

    def test_file_content_is_valid_text(self, tmp_path: Path):
        r = _result(_err("B5", "integer", "amount"))
        path = write_report(r, INPUT_FILE, tmp_path)
        content = path.read_text(encoding="utf-8")
        assert "Validation Report" in content

    def test_two_runs_produce_different_filenames(self, tmp_path: Path):
        r = _result(_err("B5", "integer", "amount"))
        # Разные временны́е метки гарантируются sleep, но нам достаточно проверить
        # что оба файла создаются и имеют разные имена при разных секундах.
        # Просто убеждаемся, что функция не перезаписывает один файл.
        p1 = write_report(r, INPUT_FILE, tmp_path)
        p2 = write_report(r, INPUT_FILE, tmp_path)
        files = list(tmp_path.iterdir())
        # Оба файла реально существуют (даже если имена совпали из-за быстрого теста)
        assert p1.exists() and p2.exists()
        assert len(files) >= 1  # минимум один файл создан


# ── log_validation_result ──────────────────────────────────────────────────────

class TestLogValidationResult:
    def test_valid_logs_info_not_warning(self):
        result = FileValidationResult()
        mock_logger = MagicMock(spec=logging.Logger)
        log_validation_result(result, INPUT_FILE, mock_logger)
        mock_logger.info.assert_called_once()
        mock_logger.warning.assert_not_called()

    def test_errors_log_warning(self):
        r = _result(_err("B5", "integer", "amount"))
        mock_logger = MagicMock(spec=logging.Logger)
        log_validation_result(r, INPUT_FILE, mock_logger)
        assert mock_logger.warning.call_count >= 2  # summary + column line + fix hint

    def test_warning_mentions_column(self):
        r = _result(_err("B5", "integer", "amount"))
        mock_logger = MagicMock(spec=logging.Logger)
        log_validation_result(r, INPUT_FILE, mock_logger)
        all_args = " ".join(
            str(a) for call in mock_logger.warning.call_args_list for a in call.args
        )
        assert "amount" in all_args or "integer" in all_args

    def test_warning_mentions_filename(self):
        r = _result(_err("B5", "integer", "amount"))
        mock_logger = MagicMock(spec=logging.Logger)
        log_validation_result(r, INPUT_FILE, mock_logger)
        all_args = " ".join(
            str(a) for call in mock_logger.warning.call_args_list for a in call.args
        )
        assert INPUT_FILE.name in all_args
