"""
validation_report.py
====================
Форматирование и вывод результатов валидации.

Публичный API:
    log_validation_result  — всегда вызывается из load(); пишет в logger
    write_report           — опционально; пишет TXT-файл в указанную директорию
"""
from __future__ import annotations

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import CellValidationError, FileValidationResult


# ── Внутренние хелперы ────────────────────────────────────────────────────────

def _parse_cell(cell_name: str) -> tuple[str, int]:
    """'B5' → ('B', 5);  'AA123' → ('AA', 123)."""
    m = re.match(r"([A-Z]+)(\d+)", cell_name)
    if not m:
        return cell_name, 0
    return m.group(1), int(m.group(2))


def _rows_to_ranges(rows: list[int], max_ranges: int = 10) -> str:
    """Преобразует список строк в компактное строковое представление диапазонов.

    Пример: [1,2,3,5,6,8] → '1–3, 5–6, 8'
    Если диапазонов больше max_ranges — обрезает и добавляет '… and N more rows'.
    """
    sorted_rows = sorted(set(rows))
    if not sorted_rows:
        return ""

    # Строим список диапазонов (start, end)
    ranges: list[tuple[int, int]] = []
    start = end = sorted_rows[0]
    for r in sorted_rows[1:]:
        if r == end + 1:
            end = r
        else:
            ranges.append((start, end))
            start = end = r
    ranges.append((start, end))

    visible = ranges[:max_ranges]
    parts = [str(s) if s == e else f"{s}–{e}" for s, e in visible]
    result = ", ".join(parts)

    if len(ranges) > max_ranges:
        extra_count = sum(e - s + 1 for s, e in ranges[max_ranges:])
        result += f" … and {extra_count} more rows"

    return result


def _group_errors(
    errors: list[CellValidationError],
) -> dict[tuple[str, str, str], list[tuple[int, CellValidationError]]]:
    """Группирует ошибки по (col_letter, col_name, expected_type).

    Возвращает упорядоченный (по col_letter) словарь:
        (col_letter, col_name, expected_type) → [(row, error), ...]
    """
    groups: dict[tuple[str, str, str], list[tuple[int, CellValidationError]]] = {}
    for err in errors:
        col_letter, row = _parse_cell(err.cell_name)
        col_display = err.col_name if err.col_name else col_letter
        key = (col_letter, col_display, err.expected_type)
        groups.setdefault(key, []).append((row, err))
    return dict(sorted(groups.items()))


def _col_label(col_letter: str, col_name: str) -> str:
    """'sale_date (C)' или просто 'C' если имя не задано."""
    if col_name and col_name != col_letter:
        return f"{col_name} ({col_letter})"
    return col_letter


# ── Публичный API ─────────────────────────────────────────────────────────────

def log_validation_result(
    result: FileValidationResult,
    input_file: Path,
    logger: logging.Logger,
) -> None:
    """Логирует итог валидации. Вызывается всегда после валидации."""
    if result.is_valid:
        logger.info("Validation passed: 0 errors in %s", input_file.name)
        return

    groups = _group_errors(result.errors)
    logger.warning(
        "Validation: %d error(s) in %s (%d column(s) affected)",
        len(result.errors),
        input_file.name,
        len(groups),
    )
    for (col_letter, col_name, expected_type), errs in groups.items():
        rows = [row for row, _ in errs]
        logger.warning(
            "  [%s] column %s — %d cell(s), rows: %s",
            expected_type,
            _col_label(col_letter, col_name),
            len(rows),
            _rows_to_ranges(rows),
        )
    logger.warning("Fix: open %s and correct the column(s) listed above", input_file.name)


def _format_report(
    result: FileValidationResult,
    input_file: Path,
    include_sample_values: bool,
) -> str:
    """Форматирует полный текстовый отчёт."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        f"=== Validation Report: {input_file.name} ===",
        f"Generated: {ts}",
        "",
    ]

    if result.is_valid:
        lines.append("Result: OK — no errors found.")
        return "\n".join(lines)

    groups = _group_errors(result.errors)
    lines.append(
        f"Result: FAILED — {len(result.errors)} error(s) in {len(groups)} column(s)"
    )
    lines.append("")

    for (col_letter, col_name, expected_type), errs in groups.items():
        rows = [row for row, _ in errs]
        label = _col_label(col_letter, col_name)
        lines.append(f"[{expected_type}]  column {label}  ({len(rows)} error(s))")
        lines.append(f"  Rows: {_rows_to_ranges(rows)}")
        if include_sample_values:
            samples = errs[:3]
            sample_str = ",  ".join(
                f'"{e.cell_value!r}" ({e.cell_name})' for _, e in samples
            )
            lines.append(f"  Sample values: {sample_str}")
        lines.append("")

    lines.append(f"Fix hint: open {input_file.name} and correct the cell ranges listed above.")
    return "\n".join(lines)


def write_report(
    result: FileValidationResult,
    input_file: Path,
    report_dir: Path,
    include_sample_values: bool = False,
) -> Path:
    """Записывает TXT-отчёт в report_dir и возвращает путь к файлу."""
    report_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{input_file.stem}_validation_{ts}.txt"
    path = report_dir / filename
    text = _format_report(result, input_file, include_sample_values=include_sample_values)
    path.write_text(text, encoding="utf-8")
    return path
