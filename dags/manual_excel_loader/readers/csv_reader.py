"""
readers/csv_reader.py
=====================
Читает CSV/TSV-файлы и возвращает SheetData — тот же контракт, что ExcelReader.
Это позволяет loader.py работать с CSV без изменений в логике валидации.
"""
from __future__ import annotations

import csv
import dataclasses
import logging
from pathlib import Path
from typing import Iterator

from ..exceptions import FileReadError

log = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class CsvReadConfig:
    path: Path
    delimiter: str = ","
    encoding: str = "utf-8"
    skip_rows: int = 0
    skip_cols: int = 0
    max_row: int | None = None


@dataclasses.dataclass
class CsvSheetData:
    """Аналог SheetData из reader.py — унифицированный формат для loader."""
    headers: list[str]
    rows: list[tuple]
    source_path: Path


def read_csv(config: CsvReadConfig) -> CsvSheetData:
    """
    Читает CSV-файл согласно конфигу.

    Логика:
      - skip_rows: пропустить N строк в начале (до заголовка)
      - skip_cols: пропустить N столбцов слева
      - max_row: ограничить количество строк данных (не считая заголовка)
    """
    path = config.path
    if not path.exists():
        raise FileReadError(f"CSV-файл не найден: {path}")

    log.info("Читаем CSV: %s (разделитель=%r, кодировка=%s)", path, config.delimiter, config.encoding)

    with open(path, encoding=config.encoding, newline="") as fh:
        reader = csv.reader(fh, delimiter=config.delimiter)

        # Пропускаем skip_rows строк до заголовка
        for _ in range(config.skip_rows):
            try:
                next(reader)
            except StopIteration:
                break

        # Читаем заголовок
        try:
            raw_headers = next(reader)
        except StopIteration:
            raise ValueError(f"CSV-файл пустой или содержит только пропускаемые строки: {path}")

        headers = [h.strip().lower() for h in raw_headers[config.skip_cols:]]
        if not headers:
            raise ValueError(f"После skip_cols={config.skip_cols} не осталось столбцов в заголовке.")

        # Читаем строки данных
        rows: list[tuple] = []
        for row_idx, raw_row in enumerate(reader):
            if config.max_row is not None and row_idx >= config.max_row:
                break

            row = raw_row[config.skip_cols:]
            # Выравниваем длину строки по заголовку
            row = _align_row(row, len(headers))
            # Преобразуем пустые строки в None
            normalized = tuple(cell if cell != "" else None for cell in row)
            rows.append(normalized)

    log.info("CSV прочитан: %d строк, %d столбцов", len(rows), len(headers))
    return CsvSheetData(headers=headers, rows=rows, source_path=path)


def _align_row(row: list[str], expected_len: int) -> list[str | None]:
    """Дополняет строку None или обрезает до нужной длины."""
    if len(row) < expected_len:
        return list(row) + [None] * (expected_len - len(row))
    return list(row[:expected_len])


# ── Итератор строк (для больших файлов) ──────────────────────────────────────

def iter_csv(config: CsvReadConfig) -> Iterator[tuple]:
    """
    Генератор строк CSV — не держит весь файл в памяти.
    Используется в loader при обработке больших файлов.
    """
    with open(config.path, encoding=config.encoding, newline="") as fh:
        reader = csv.reader(fh, delimiter=config.delimiter)

        for _ in range(config.skip_rows):
            try:
                next(reader)
            except StopIteration:
                return

        try:
            raw_headers = next(reader)
        except StopIteration:
            return

        n_cols = len(raw_headers) - config.skip_cols

        for row_idx, raw_row in enumerate(reader):
            if config.max_row is not None and row_idx >= config.max_row:
                break
            row = raw_row[config.skip_cols:]
            row = _align_row(row, n_cols)
            yield tuple(cell if cell != "" else None for cell in row)