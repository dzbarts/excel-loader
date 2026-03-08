"""
readers/sql_reader.py
=====================
Читает SQL-файлы с INSERT-выражениями и возвращает унифицированные данные.

Поддерживает:
  - одиночные INSERT: INSERT INTO t (c1, c2) VALUES (v1, v2);
  - батчевые INSERT:  INSERT INTO t (c1, c2) VALUES (...), (...), (...);
  - несколько таблиц в одном файле (возвращает данные первой или указанной)
"""
from __future__ import annotations

import dataclasses
import logging
import re
from pathlib import Path
from typing import Iterator

log = logging.getLogger(__name__)

# ── Паттерны ──────────────────────────────────────────────────────────────────

_INSERT_PATTERN = re.compile(
    r"INSERT\s+INTO\s+"
    r"(?P<table>[^\s(]+)"            # имя таблицы (schema.table или просто table)
    r"\s*\((?P<cols>[^)]+)\)"        # список колонок
    r"\s*VALUES\s*"
    r"(?P<values>.+?)(?=;|$)",       # VALUES до первой точки с запятой
    re.IGNORECASE | re.DOTALL,
)

_VALUES_ROW_PATTERN = re.compile(r"\(([^)]+)\)")


@dataclasses.dataclass(frozen=True)
class SqlReadConfig:
    path: Path
    encoding: str = "utf-8"
    target_table: str | None = None   # если None — читаем первую таблицу


@dataclasses.dataclass
class SqlSheetData:
    """Унифицированный формат для loader — аналог SheetData."""
    headers: list[str]
    rows: list[tuple]
    source_path: Path
    table_name: str


def read_sql(config: SqlReadConfig) -> SqlSheetData:
    """
    Парсит SQL-файл и возвращает данные первой (или указанной) таблицы.

    Raises:
        FileNotFoundError: если файл не найден.
        ValueError: если в файле нет подходящих INSERT-выражений.
    """
    path = config.path
    if not path.exists():
        raise FileNotFoundError(f"SQL-файл не найден: {path}")

    log.info("Читаем SQL: %s", path)
    content = path.read_text(encoding=config.encoding)

    tables = _parse_inserts(content)
    if not tables:
        raise ValueError(f"В файле {path} не найдено INSERT-выражений.")

    if config.target_table:
        key = _normalize_table_name(config.target_table)
        if key not in tables:
            available = list(tables.keys())
            raise ValueError(
                f"Таблица '{config.target_table}' не найдена в файле. "
                f"Доступны: {available}"
            )
        table_name, data = config.target_table, tables[key]
    else:
        # берём первую таблицу
        first_key = next(iter(tables))
        table_name = first_key
        data = tables[first_key]

    log.info(
        "SQL прочитан: таблица=%s, строк=%d, столбцов=%d",
        table_name,
        len(data["rows"]),
        len(data["headers"]),
    )
    return SqlSheetData(
        headers=data["headers"],
        rows=data["rows"],
        source_path=path,
        table_name=table_name,
    )


def iter_sql(config: SqlReadConfig) -> Iterator[tuple]:
    """Генератор строк — для ленивой обработки больших SQL-файлов."""
    sheet = read_sql(config)
    yield from sheet.rows


# ── Внутренние функции ────────────────────────────────────────────────────────

def _parse_inserts(sql: str) -> dict[str, dict]:
    """
    Находит все INSERT-блоки и группирует строки по таблицам.

    Returns:
        {normalized_table_name: {"headers": [...], "rows": [(...), ...]}}
    """
    # Убираем комментарии
    sql = re.sub(r"--[^\n]*", "", sql)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)

    tables: dict[str, dict] = {}

    for match in _INSERT_PATTERN.finditer(sql):
        raw_table = match.group("table").strip().strip("`\"[]")
        key = _normalize_table_name(raw_table)

        raw_cols = match.group("cols")
        headers = [c.strip().strip("`\"[] ").lower() for c in raw_cols.split(",")]

        values_block = match.group("values")
        rows = _parse_values_block(values_block, len(headers))

        if key not in tables:
            tables[key] = {"headers": headers, "rows": []}
        tables[key]["rows"].extend(rows)

    return tables


def _parse_values_block(values_block: str, n_cols: int) -> list[tuple]:
    """Разбирает блок VALUES (...), (...) на список кортежей."""
    rows = []
    for row_match in _VALUES_ROW_PATTERN.finditer(values_block):
        raw = row_match.group(1)
        row = _split_values_row(raw)
        # Выравниваем по числу колонок
        if len(row) < n_cols:
            row.extend([None] * (n_cols - len(row)))
        else:
            row = row[:n_cols]
        rows.append(tuple(row))
    return rows


def _split_values_row(raw: str) -> list:
    """
    Разбирает одну строку VALUES с учётом кавычек.
    Обрабатывает NULL, числа, строки в одинарных кавычках.
    """
    values = []
    current: list[str] = []
    in_quotes = False
    escape = False

    for ch in raw:
        if escape:
            current.append(ch)
            escape = False
            continue
        if ch == "\\" and in_quotes:
            escape = True
            continue
        if ch == "'" and not in_quotes:
            in_quotes = True
            continue
        if ch == "'" and in_quotes:
            in_quotes = False
            continue
        if ch == "," and not in_quotes:
            values.append(_coerce_value("".join(current).strip()))
            current = []
            continue
        current.append(ch)

    if current or raw.endswith(","):
        values.append(_coerce_value("".join(current).strip()))

    return values


def _coerce_value(raw: str):
    """Конвертирует строковое представление SQL-значения в Python-тип."""
    if raw.upper() == "NULL":
        return None
    if raw.upper() == "TRUE":
        return True
    if raw.upper() == "FALSE":
        return False
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    return raw if raw else None


def _normalize_table_name(name: str) -> str:
    """schema.table → просто table (нижний регистр)."""
    if "." in name:
        return name.split(".", 1)[1].lower()
    return name.lower()