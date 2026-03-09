"""
ddl_generator.py
================
Генерация CREATE TABLE DDL из словаря {col_name: type_string}.

Публичный API
-------------
generate_ddl(columns, scheme, table, db_type, timestamp_col=None) -> str
"""
from __future__ import annotations

from .enums import DatabaseType, TimestampField


def generate_ddl(
    columns: dict[str, str],
    scheme: str,
    table: str,
    db_type: DatabaseType,
    timestamp_col: TimestampField | str | None = None,
) -> str:
    """
    Сгенерировать CREATE TABLE DDL.

    Args:
        columns:       {col_name: type_string} — порядок сохраняется (Python 3.7+).
        scheme:        схема / база данных.
        table:         имя таблицы.
        db_type:       целевая БД.
        timestamp_col: если задан и отсутствует в columns — добавить как последнюю колонку.

    Returns:
        Строка DDL готовая к исполнению.
    """
    ts_name = timestamp_col.value if isinstance(timestamp_col, TimestampField) else timestamp_col

    if db_type == DatabaseType.GREENPLUM:
        return _gp_ddl(columns, scheme, table, ts_name)
    return _ch_ddl(columns, scheme, table, ts_name)


# ── GreenPlum ─────────────────────────────────────────────────────────────────

def _gp_ddl(
    columns: dict[str, str],
    scheme: str,
    table: str,
    ts_name: str | None,
) -> str:
    lines = [f"\t{col}\t{dtype}\tNULL" for col, dtype in columns.items()]
    if ts_name and ts_name not in columns:
        lines.append(f"\t{ts_name}\ttimestamp\tNULL")

    cols_str = ",\n".join(lines)
    return (
        f"-- {scheme}.{table} definition\n"
        f"-- DROP TABLE {scheme}.{table};\n"
        f"CREATE TABLE {scheme}.{table} (\n{cols_str}\n)\n"
        f"DISTRIBUTED RANDOMLY;\n"
        f"-- Рекомендуется заменить RANDOMLY на явный ключ распределения"
    )


# ── ClickHouse ────────────────────────────────────────────────────────────────

def _ch_ddl(
    columns: dict[str, str],
    scheme: str,
    table: str,
    ts_name: str | None,
) -> str:
    lines = [
        f"    `{col}` Nullable({dtype}) DEFAULT NULL"
        for col, dtype in columns.items()
    ]
    if ts_name and ts_name not in columns:
        lines.append(f"    `{ts_name}` Nullable(DateTime()) DEFAULT NULL")

    cols_str = ",\n".join(lines)
    return (
        f"-- DROP TABLE IF EXISTS {scheme}.{table};\n"
        f"CREATE TABLE {scheme}.{table}\n"
        f"(\n{cols_str}\n)\n"
        f"ENGINE = MergeTree\n"
        f"ORDER BY tuple()\n"
        f"SETTINGS allow_nullable_key = 1,\n"
        f"         index_granularity = 8192;"
    )
