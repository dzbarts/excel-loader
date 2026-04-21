"""
writers/db_writer.py
====================
Потоковая вставка строк в GreenPlum/PostgreSQL (через COPY FROM STDIN)
или ClickHouse (нативный протокол).

Публичный API
-------------
DbWriter(config) → BaseWriter
    write(headers, rows) -> int  — вставить строки, вернуть количество.

DbWriterConfig
    db_type, scheme_name, table_name, batch_size — параметры записи.
    conn   — открытое psycopg2-соединение из table_manager.prepare()
             при truncate_load (сохранение транзакции). None — откроется новое.
    client — аналогично для ClickHouse.
"""
from __future__ import annotations

import csv
import io
import logging
from dataclasses import dataclass, field
from typing import Iterable, Iterator

from ..enums import DatabaseType
from .base import BaseWriter

log = logging.getLogger(__name__)


# ── Конфиг ───────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class DbWriterConfig:
    """Параметры записи в БД для DbWriter."""

    db_type: DatabaseType
    scheme_name: str
    table_name: str
    batch_size: int = 10000
    conn: object = field(default=None, hash=False)    # GP/PG: соединение из table_manager
    client: object = field(default=None, hash=False)  # CH: клиент из table_manager


# ── Writer ────────────────────────────────────────────────────────────────────

class DbWriter(BaseWriter):
    """Пишет строки напрямую в GP/PG (COPY FROM STDIN) или CH (нативный протокол).

    Конфигурация передаётся один раз в конструктор.
    Если conn/client не переданы — соединение открывается из Airflow-коннектора
    и управляется самостоятельно (commit/rollback/close).
    """

    def __init__(self, config: DbWriterConfig) -> None:
        self._config = config

    def write(self, headers: list[str], rows: Iterable[tuple]) -> int:
        cfg = self._config
        if cfg.db_type == DatabaseType.GREENPLUM:
            return _load_copy(headers, rows, cfg.scheme_name, cfg.table_name,
                              cfg.conn, tag="GP", get_conn=_gp_conn)
        if cfg.db_type == DatabaseType.POSTGRES:
            return _load_copy(headers, rows, cfg.scheme_name, cfg.table_name,
                              cfg.conn, tag="PG", get_conn=_pg_conn)
        return _load_ch(headers, rows, cfg.scheme_name, cfg.table_name,
                        cfg.batch_size, cfg.client)


# ── GreenPlum / PostgreSQL — COPY FROM STDIN ─────────────────────────────────

def _gp_conn():
    from .._connections import get_gp_conn
    return get_gp_conn()


def _pg_conn():
    from .._connections import get_pg_conn
    return get_pg_conn()


def _row_to_csv(row: tuple) -> str:
    """Сериализовать одну строку в CSV-формат для COPY."""
    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    writer.writerow("" if v is None else v for v in row)
    return buf.getvalue()


def _load_copy(
    headers: list[str],
    rows: Iterator[tuple],
    scheme: str,
    table: str,
    conn,
    tag: str,
    get_conn,
) -> int:
    own_conn = conn is None
    if own_conn:
        conn = get_conn()
        conn.autocommit = False

    cols = ", ".join(f'"{h}"' for h in headers)
    copy_sql = f"COPY \"{scheme}\".\"{table}\" ({cols}) FROM STDIN WITH (FORMAT CSV, NULL '')"

    total = 0

    def _gen_csv():
        nonlocal total
        for row in rows:
            yield _row_to_csv(tuple(row))
            total += 1

    try:
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, _CsvStream(_gen_csv()))
        if own_conn:
            conn.commit()
    except Exception:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()

    log.info("%s: вставлено %d строк → %s.%s", tag, total, scheme, table)
    return total


class _CsvStream:
    """File-like объект для copy_expert: отдаёт строки генератора как байты."""

    def __init__(self, gen):
        self._gen = gen
        self._buf = b""

    def read(self, size: int) -> bytes:
        while len(self._buf) < size:
            try:
                chunk = next(self._gen).encode("utf-8")
                self._buf += chunk
            except StopIteration:
                break
        data, self._buf = self._buf[:size], self._buf[size:]
        return data


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
        from .._connections import get_ch_client
        client = get_ch_client()

    cols = ", ".join(f"`{h}`" for h in headers)
    sql = f"INSERT INTO `{scheme}`.`{table}` ({cols}) VALUES"

    # Определяем типы колонок по схеме CH и строим таблицу приведений.
    # clickhouse-driver не приводит типы автоматически, поэтому делаем сами.
    ch_col_types = {
        name: type_str
        for name, type_str in client.execute(
            "SELECT name, type FROM system.columns "
            "WHERE database = %(db)s AND table = %(tbl)s",
            {"db": scheme, "tbl": table},
        )
    }

    string_col_indices: set[int] = set()
    int_col_indices:    set[int] = set()
    float_col_indices:  set[int] = set()

    for i, h in enumerate(headers):
        ch_type = ch_col_types.get(h, "")
        # убираем Nullable(...) обёртку для сравнения
        inner = ch_type.replace("Nullable(", "").rstrip(")")
        if "String" in inner or "UUID" in inner:
            string_col_indices.add(i)
        elif "Int" in inner or inner == "Bool":
            int_col_indices.add(i)
        elif "Float" in inner or "Decimal" in inner:
            float_col_indices.add(i)

    def _coerce(row: tuple) -> tuple:
        if not (string_col_indices or int_col_indices or float_col_indices):
            return row
        lst = list(row)
        for i in string_col_indices:
            if lst[i] is not None and not isinstance(lst[i], str):
                lst[i] = str(lst[i])
        for i in int_col_indices:
            if lst[i] is not None and not isinstance(lst[i], int):
                lst[i] = int(float(lst[i]))
        for i in float_col_indices:
            if lst[i] is not None and not isinstance(lst[i], float):
                lst[i] = float(lst[i])
        return tuple(lst)

    total = 0
    batch: list[tuple] = []

    for row in rows:
        batch.append(_coerce(row))
        if len(batch) >= batch_size:
            client.execute(sql, batch)
            total += len(batch)
            batch = []
    if batch:
        client.execute(sql, batch)
        total += len(batch)

    log.info("CH: вставлено %d строк → %s.%s", total, scheme, table)
    return total
