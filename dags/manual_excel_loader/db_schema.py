"""
db_schema.py
============
Запрос типов колонок из целевой таблицы GP или CH.

Публичный API
-------------
get_table_columns(scheme, table, db_type) -> dict[str, str] | None
    Вернуть {col_name: type_string} или None если таблица не существует.

table_exists(scheme, table, db_type) -> bool
"""
from __future__ import annotations

import logging
import re

from .enums import DatabaseType

log = logging.getLogger(__name__)


def get_table_columns(
    scheme: str, table: str, db_type: DatabaseType
) -> dict[str, str] | None:
    """
    Запросить имена и типы колонок из целевой таблицы.

    Возвращает dict {col_name: type_string} в порядке ordinal_position,
    или None если таблица не найдена.
    Типы нормализованы для совместимости с get_validator():
      - GP: 'character varying(N)', 'numeric(P,S)', 'integer', 'text', …
      - CH: Nullable(X) раскрывается до X.
    """
    if db_type == DatabaseType.GREENPLUM:
        return _gp_columns(scheme, table)
    return _ch_columns(scheme, table)


def table_exists(scheme: str, table: str, db_type: DatabaseType) -> bool:
    return get_table_columns(scheme, table, db_type) is not None


# ── GreenPlum ─────────────────────────────────────────────────────────────────

def _gp_columns(scheme: str, table: str) -> dict[str, str] | None:
    from ._connections import get_gp_conn

    conn = get_gp_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    column_name,
                    CASE
                        WHEN data_type = 'character varying'
                            THEN 'character varying(' || character_maximum_length || ')'
                        WHEN data_type IN ('character', 'char')
                            THEN 'character(' || character_maximum_length || ')'
                        WHEN data_type IN ('numeric', 'decimal')
                            THEN data_type || '(' || numeric_precision || ',' || numeric_scale || ')'
                        ELSE data_type
                    END AS full_type
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name   = %s
                ORDER BY ordinal_position
                """,
                (scheme, table),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        log.debug("GP: table %s.%s not found in information_schema", scheme, table)
        return None
    return {col: type_str for col, type_str in rows}


# ── ClickHouse ────────────────────────────────────────────────────────────────

_NULLABLE_RE = re.compile(r"^Nullable\((.+)\)$")


def _ch_columns(scheme: str, table: str) -> dict[str, str] | None:
    from ._connections import get_ch_client

    client = get_ch_client()
    rows = client.execute(
        "SELECT name, type FROM system.columns "
        "WHERE database = %(db)s AND table = %(tbl)s "
        "ORDER BY position",
        {"db": scheme, "tbl": table},
    )
    if not rows:
        log.debug("CH: table %s.%s not found in system.columns", scheme, table)
        return None

    result: dict[str, str] = {}
    for name, type_str in rows:
        m = _NULLABLE_RE.match(type_str)
        result[name] = m.group(1) if m else type_str
    return result
