"""
date_clipper.py
===============
Обрезка дат до допустимого диапазона целевой БД.

Публичный API
-------------
build_date_clipper(db_type, dtypes, headers) -> callable | None
    Вернуть функцию (row: tuple) -> tuple, которая обрезает значения
    date/datetime-колонок до границ целевой БД.
    Вернёт None, если клиппинг не нужен (GP/PG, или нет дата-колонок).
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Callable

from .enums import DatabaseType

log = logging.getLogger(__name__)

# ── Границы типов ClickHouse ──────────────────────────────────────────────────

_CH_DATE_BOUNDS: dict[str, tuple[date, date]] = {
    "date":   (date(1970, 1, 1),  date(2149, 6, 6)),
    "date32": (date(1925, 1, 1),  date(2283, 11, 11)),
}

_CH_DATETIME_BOUNDS: dict[str, tuple[datetime, datetime]] = {
    "datetime":   (datetime(1970, 1, 1), datetime(2106, 2, 7, 6, 28, 15)),
    "datetime64": (datetime(1925, 1, 1), datetime(2283, 11, 11)),
}


def _ch_type_key(type_str: str) -> str | None:
    """Извлечь ключ из строки CH-типа: 'DateTime64(3, UTC)' → 'datetime64'."""
    base = re.split(r"[\s(]", type_str.lower().strip())[0]
    if base == "date32":
        return "date32"
    if base == "date":
        return "date"
    if base == "datetime64":
        return "datetime64"
    if base in ("datetime", "timestamp"):
        return "datetime"
    return None


# ── Публичный API ─────────────────────────────────────────────────────────────

def build_date_clipper(
    db_type: DatabaseType,
    dtypes: dict[str, str] | None,
    headers: list[str],
) -> Callable[[tuple], tuple] | None:
    """Построить функцию-клиппер строк для целевой БД.

    Args:
        db_type: целевая БД.
        dtypes:  словарь {col_name: type_str} из DDL / инференса.
        headers: финальный список колонок (после exclude), в том порядке,
                 в котором они идут в строках итератора.

    Returns:
        Callable (row) -> row  — если есть что обрезать.
        None                   — если клиппинг не применим.
    """
    if db_type != DatabaseType.CLICKHOUSE:
        return None
    if not dtypes:
        return None

    # Индекс колонки → (lo, hi)
    clip_spec: dict[int, tuple] = {}
    for i, col in enumerate(headers):
        type_str = dtypes.get(col)
        if not type_str:
            continue
        key = _ch_type_key(type_str)
        if key is None:
            continue
        bounds = _CH_DATE_BOUNDS.get(key) or _CH_DATETIME_BOUNDS.get(key)
        if bounds:
            clip_spec[i] = bounds

    if not clip_spec:
        return None

    clipped_cols = [headers[i] for i in clip_spec]
    log.info("date_clipper: активен для %d колонок: %s", len(clip_spec), clipped_cols)

    def _clip(row: tuple) -> tuple:
        lst = list(row)
        for idx, (lo, hi) in clip_spec.items():
            if idx >= len(lst):
                continue
            v = lst[idx]
            if v is None:
                continue
            try:
                # Привести типы для корректного сравнения
                if isinstance(lo, datetime):
                    # Нужен datetime — если пришла date, поднять до datetime
                    if isinstance(v, date) and not isinstance(v, datetime):
                        v = datetime(v.year, v.month, v.day)
                else:
                    # Нужна date — если пришёл datetime, взять .date()
                    if isinstance(v, datetime):
                        v = v.date()

                clipped = max(lo, min(hi, v))
                if clipped is not v:
                    log.debug(
                        "date_clipper: col[%d] %r → %r", idx, lst[idx], clipped
                    )
                lst[idx] = clipped
            except (TypeError, ValueError):
                pass
        return tuple(lst)

    return _clip
