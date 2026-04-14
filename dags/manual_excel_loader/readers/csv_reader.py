"""
readers/csv_reader.py
=====================
Читает CSV/TSV-файлы и возвращает SheetData — тот же контракт, что ExcelReader.
Это позволяет loader.py работать с CSV без изменений в логике валидации.
"""
from __future__ import annotations

import contextlib
import csv
import dataclasses
import io
import logging
from pathlib import Path
from typing import Iterator

from ..exceptions import FileReadError, HeaderValidationError
from .headers import read_headers_raw, validate_headers

log = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class CsvReadConfig:
    path: Path
    stream: bytes | None = None
    delimiter: str = ","
    encoding: str = "utf-8"
    skip_rows: int = 0
    skip_cols: int = 0
    max_row: int | None = None
    skip_header_validation: bool = False



def _align_row(row: list[str], expected_len: int) -> list[str | None]:
    """Дополняет строку None или обрезает до нужной длины."""
    if len(row) < expected_len:
        return list(row) + [None] * (expected_len - len(row))
    return list(row[:expected_len])


# ── Потоковое чтение (для больших файлов) ────────────────────────────────────

def stream_csv(config: CsvReadConfig) -> tuple[list[str], Iterator[tuple]]:
    """
    Потоковое чтение CSV — не держит весь файл в памяти.
    Возвращает (headers, итератор строк). Файл остаётся открытым
    до исчерпания итератора или сборки мусора.
    """
    gen = _csv_gen(config)
    try:
        headers = next(gen)
    except StopIteration:
        raise ValueError(
            f"CSV-файл пустой или содержит только пропускаемые строки: {config.path}"
        )
    return headers, gen


def _csv_gen(config: CsvReadConfig) -> Iterator:
    """Внутренний генератор: первый yield — заголовки, далее — строки данных."""
    if config.stream is not None:
        ctx = contextlib.nullcontext(
            io.TextIOWrapper(io.BytesIO(config.stream), encoding=config.encoding, newline="")
        )
    else:
        ctx = open(config.path, encoding=config.encoding, newline="")

    with ctx as fh:
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

        raw = [h if h.strip() else None for h in raw_headers[config.skip_cols:]]
        if config.skip_header_validation:
            headers = read_headers_raw(raw)
        else:
            headers = validate_headers(raw)
        yield headers

        n_cols = len(headers)
        for row_idx, raw_row in enumerate(reader):
            if config.max_row is not None and row_idx >= config.max_row:
                break
            row = raw_row[config.skip_cols:]
            row = _align_row(row, n_cols)
            yield tuple(cell if cell != "" else None for cell in row)