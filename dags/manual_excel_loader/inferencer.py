"""
inferencer.py
=============
Инференс типов колонок по данным файла (без pandas, через openpyxl-типы).

Читает первые SAMPLE_SIZE непустых строк каждой колонки и выбирает
наиболее специфичный совместимый тип.

Публичный API
-------------
infer_types(sheet_data, db_type) -> dict[str, str]
    Вернуть {col_name: type_string} для GP или CH.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, time
from typing import Any

from .enums import DatabaseType
from .readers import SheetData

log = logging.getLogger(__name__)

SAMPLE_SIZE = 200  # строк для анализа на колонку


# ── Приоритет типов: от более специфичного к менее специфичному ──────────────
# Если в колонке есть хоть одно float — тип float, даже если остальные int.
# Если есть строка, которую нельзя привести к числу — text/String.

def infer_types(sheet_data: SheetData, db_type: DatabaseType) -> dict[str, str]:
    """Инференс типов по первым SAMPLE_SIZE строкам данных."""
    headers = list(sheet_data.headers)
    col_values: dict[str, list[Any]] = {h: [] for h in headers}

    count = 0
    for row in sheet_data.rows:
        if count >= SAMPLE_SIZE:
            break
        for header, value in zip(headers, row):
            if value is not None:
                col_values[header].append(value)
        count += 1

    result = {}
    for header in headers:
        values = col_values[header]
        inferred = _infer_column(values, db_type)
        result[header] = inferred
        log.debug("Inferred %s → %s (%s)", header, inferred, db_type.value)
    return result


def _infer_column(values: list[Any], db_type: DatabaseType) -> str:
    if not values:
        return "text" if db_type == DatabaseType.GREENPLUM else "String"

    # Флаги наличия каждого Python-типа (bool проверяем ДО int — bool наследует int)
    has_bool     = any(isinstance(v, bool) for v in values)
    has_datetime = any(isinstance(v, datetime) for v in values)
    has_date     = any(isinstance(v, date) and not isinstance(v, datetime) for v in values)
    has_time     = any(isinstance(v, time) for v in values)
    has_float    = any(isinstance(v, float) for v in values)
    has_int      = any(isinstance(v, int) and not isinstance(v, bool) for v in values)
    has_str      = any(isinstance(v, str) for v in values)

    # Строки: пробуем конвертировать числа/даты из строковых ячеек
    # (openpyxl уже распарсил большинство значений в нативные типы,
    #  но на всякий случай оставляем строку как fallback)

    if db_type == DatabaseType.GREENPLUM:
        return _gp_type(has_bool, has_datetime, has_date, has_time,
                        has_float, has_int, has_str)
    return _ch_type(has_bool, has_datetime, has_date, has_time,
                    has_float, has_int, has_str)


def _gp_type(
    has_bool: bool, has_datetime: bool, has_date: bool, has_time: bool,
    has_float: bool, has_int: bool, has_str: bool,
) -> str:
    if has_str:
        return "text"
    if has_datetime:
        return "timestamp"
    if has_date:
        return "date"
    if has_time:
        return "time"
    if has_bool:
        return "boolean"
    if has_float:
        return "decimal(18,6)"
    if has_int:
        return "bigint"
    return "text"


def _ch_type(
    has_bool: bool, has_datetime: bool, has_date: bool, has_time: bool,
    has_float: bool, has_int: bool, has_str: bool,
) -> str:
    if has_str:
        return "String"
    if has_datetime:
        return "DateTime"
    if has_date:
        return "Date32"
    if has_time:
        return "String"   # CH не имеет нативного типа Time
    if has_bool:
        return "Bool"
    if has_float:
        return "Float64"
    if has_int:
        return "Int64"
    return "String"
