"""
inferencer.py
=============
Инференс типов колонок по данным файла (без pandas, через openpyxl-типы).

Читает первые SAMPLE_SIZE непустых строк каждой колонки и выбирает
наиболее специфичный совместимый тип.

Публичный API
-------------
infer_types(sheet_data, db_type) -> dict[str, str]
    Вернуть {col_name: type_string} для GP, PG или CH.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime, time
from typing import Any

from .enums import DatabaseType
from .readers import SheetData
from .type_mapping import gp_to_ch

_UUID_RE = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    re.IGNORECASE,
)

log = logging.getLogger(__name__)

# ── Приоритет типов: от более специфичного к менее специфичному ──────────────
# Если в колонке есть хоть одно float — тип float, даже если остальные int.
# Если есть строка, которую нельзя привести к числу — text/String.

def infer_types(
    sheet_data: SheetData,
    db_type: DatabaseType,
    sample_size: int | None = None,
) -> dict[str, str]:
    """Инференс типов по данным файла.

    sample_size — максимальное число строк для анализа; None означает весь документ.
    """
    headers = list(sheet_data.headers)
    col_values: dict[str, list[Any]] = {h: [] for h in headers}

    count = 0
    for row in sheet_data.rows:
        if sample_size is not None and count >= sample_size:
            break
        for header, value in zip(headers, row):
            if value is not None:
                col_values[header].append(value)
        count += 1

    log.info("inference: прочитано %d строк (sample_size=%s)", count, sample_size)
    result = {}
    for header in headers:
        values = col_values[header]
        inferred = _infer_column(values, db_type)
        result[header] = inferred
        log.debug("Inferred %s → %s (%s)", header, inferred, db_type.value)
    return result


def _classify_str_numbers(str_values: list[str]) -> tuple[bool, bool]:
    """Проверить, являются ли все непустые строки числами.

    Возвращает (all_int, all_float):
      (True,  False) — все парсятся как целые
      (False, True)  — все парсятся как float (хотя бы одно с точкой)
      (False, False) — есть строки, которые не число
    Пустые строки игнорируются (считаются отсутствующим значением).
    """
    nonempty = [v for v in str_values if v.strip()]
    if not nonempty:
        return False, False
    has_dot = False
    for v in nonempty:
        try:
            float(v)
        except ValueError:
            return False, False
        if '.' in v:
            has_dot = True
    if has_dot:
        return False, True
    return True, False


def _infer_column(values: list[Any], db_type: DatabaseType) -> str:
    if not values:
        gp = "text"
    else:
        # bool проверяем ДО int — bool наследует int
        has_bool     = any(isinstance(v, bool) for v in values)
        has_datetime = any(isinstance(v, datetime) for v in values)
        has_date     = any(isinstance(v, date) and not isinstance(v, datetime) for v in values)
        has_time     = any(isinstance(v, time) for v in values)
        has_float    = any(isinstance(v, float) for v in values)
        has_int      = any(isinstance(v, int) and not isinstance(v, bool) for v in values)
        str_values   = [v for v in values if isinstance(v, str)]
        has_uuid     = bool(str_values) and all(_UUID_RE.match(v) for v in str_values)

        # Если строки выглядят как числа (весь файл выгружен текстом) —
        # повышаем has_int/has_float и снимаем has_str.
        str_int, str_float = _classify_str_numbers(str_values)
        has_int   = has_int   or str_int
        has_float = has_float or str_float
        has_str   = bool(str_values) and not has_uuid and not str_int and not str_float

        gp = _gp_type(has_bool, has_datetime, has_date, has_time,
                      has_float, has_int, has_str, has_uuid)

    if db_type in (DatabaseType.GREENPLUM, DatabaseType.POSTGRES):
        return gp
    return gp_to_ch(gp)


def _gp_type(
    has_bool: bool, has_datetime: bool, has_date: bool, has_time: bool,
    has_float: bool, has_int: bool, has_str: bool, has_uuid: bool,
) -> str:
    if has_str:
        return "text"
    if has_uuid:
        return "uuid"
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
