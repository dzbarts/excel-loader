"""
db_loader.py
============
Потоковая вставка строк в GP или CH.

Публичный API
-------------
load_to_db(headers, rows, scheme, table, db_type, batch_size, gp_conn, ch_client) -> int
    Вставить строки в таблицу. Возвращает количество вставленных строк.

    gp_conn  — передавать открытое соединение из table_manager.prepare()
               при truncate_load (сохранение транзакции). Для append/via_backup
               можно передать None — будет открыто новое.
    ch_client — аналогично для CH.
"""
from __future__ import annotations

import logging
from typing import Iterator

from .enums import DatabaseType

log = logging.getLogger(__name__)


def load_to_db(
    headers: list[str],
    rows: Iterator[tuple],
    scheme: str,
    table: str,
    db_type: DatabaseType,
    batch_size: int = 500,
    gp_conn=None,
    ch_client=None,
) -> int:
    if db_type == DatabaseType.GREENPLUM:
        return _load_gp(headers, rows, scheme, table, batch_size, gp_conn)
    return _load_ch(headers, rows, scheme, table, batch_size, ch_client)


# ── GreenPlum ─────────────────────────────────────────────────────────────────

def _load_gp(
    headers: list[str],
    rows: Iterator[tuple],
    scheme: str,
    table: str,
    batch_size: int,
    conn,
) -> int:
    from psycopg2.extras import execute_values

    own_conn = conn is None
    if own_conn:
        from ._connections import get_gp_conn
        conn = get_gp_conn()
        conn.autocommit = False

    cols = ", ".join(f'"{h}"' for h in headers)
    sql = f'INSERT INTO "{scheme}"."{table}" ({cols}) VALUES %s'

    total = 0
    batch: list[tuple] = []

    try:
        with conn.cursor() as cur:
            for row in rows:
                batch.append(tuple(row))
                if len(batch) >= batch_size:
                    execute_values(cur, sql, batch)
                    total += len(batch)
                    batch = []
            if batch:
                execute_values(cur, sql, batch)
                total += len(batch)

        if own_conn:
            conn.commit()
    except Exception:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()

    log.info("GP: вставлено %d строк → %s.%s", total, scheme, table)
    return total


# ── ClickHouse ────────────────────────────────────────────────────────────────

def _load_ch(
    headers: list[str],
    rows: Iterator[tuple],
    scheme: str,
    table: str,
    batch_size: int,
    client,
) -> int:
    own_client = client is None
    if own_client:
        from ._connections import get_ch_client
        client = get_ch_client()

    cols = ", ".join(f"`{h}`" for h in headers)
    sql = f"INSERT INTO `{scheme}`.`{table}` ({cols}) VALUES"

    total = 0
    batch: list[tuple] = []

    for row in rows:
        batch.append(tuple(row))
        if len(batch) >= batch_size:
            client.execute(sql, batch)
            total += len(batch)
            batch = []
    if batch:
        client.execute(sql, batch)
        total += len(batch)

    log.info("CH: вставлено %d строк → %s.%s", total, scheme, table)
    return total
