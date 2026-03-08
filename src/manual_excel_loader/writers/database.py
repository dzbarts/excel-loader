"""
writers/database.py
===================
Прямая запись строк в GreenPlum (psycopg2) и ClickHouse (clickhouse-driver).

Обе зависимости уже есть в Airflow-окружении (см. список пакетов).
"""
from __future__ import annotations

import logging
from typing import Iterable, Iterator

from .base import BaseWriter, DbWriterConfig

log = logging.getLogger(__name__)

# ── Batch helper ──────────────────────────────────────────────────────────────

def _batched(iterable: Iterable, size: int) -> Iterator[list]:
    """Разбивает итерируемое на чанки заданного размера."""
    batch: list = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


# ── PostgreSQL / GreenPlum ────────────────────────────────────────────────────

class PostgresWriter(BaseWriter):
    """
    Пишет строки напрямую в GreenPlum / PostgreSQL через psycopg2.

    Использует executemany с параметризованными запросами —
    безопасно от SQL-инъекций и быстрее конкатенации строк.

    Args:
        config: DbWriterConfig с dsn, table_name, scheme_name, batch_size.
    """

    def __init__(self, config: DbWriterConfig) -> None:
        self._config = config

    def write(self, headers: list[str], rows: Iterable[tuple]) -> int:
        """
        Записывает строки в таблицу.

        Returns:
            Количество записанных строк.
        """
        try:
            import psycopg2
        except ImportError as exc:
            raise ImportError(
                "psycopg2-binary не установлен. "
                "Добавьте его в зависимости окружения."
            ) from exc

        qualified = f"{self._config.scheme_name}.{self._config.table_name}"
        cols = ", ".join(headers)
        placeholders = ", ".join(["%s"] * len(headers))
        sql = f"INSERT INTO {qualified} ({cols}) VALUES ({placeholders})"

        total_written = 0

        cfg = self._config
        conn_str = (
            f"host={cfg.host} port={cfg.port} dbname={cfg.database} "
            f"user={cfg.user} password={cfg.password}"
        )
        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cur:
                for batch in _batched(rows, self._config.batch_size):
                    cur.executemany(sql, batch)
                    total_written += len(batch)
                    log.debug("GP: вставлено %d строк (всего %d)", len(batch), total_written)
            conn.commit()

        log.info("PostgresWriter: записано %d строк в %s", total_written, qualified)
        return total_written


# ── ClickHouse ────────────────────────────────────────────────────────────────

class ClickHouseWriter(BaseWriter):
    """
    Пишет строки напрямую в ClickHouse через clickhouse-driver.

    clickhouse-driver уже доступен в Airflow-окружении.

    Args:
        config: DbWriterConfig с host/port/database/user/password,
                table_name, scheme_name (= database в CH), batch_size.
    """

    def __init__(self, config: DbWriterConfig) -> None:
        self._config = config

    def write(self, headers: list[str], rows: Iterable[tuple]) -> int:
        """
        Записывает строки в таблицу.

        Returns:
            Количество записанных строк.
        """
        try:
            from clickhouse_driver import Client
        except ImportError as exc:
            raise ImportError(
                "clickhouse-driver не установлен. "
                "Добавьте его в зависимости окружения."
            ) from exc

        # scheme_name в CH трактуется как database
        qualified = f"{self._config.scheme_name}.{self._config.table_name}"

        cfg = self._config
        client = Client(
            host=cfg.host, port=cfg.port,
            database=cfg.database, user=cfg.user, password=cfg.password,
        )
        sql = f"INSERT INTO {qualified} ({', '.join(headers)}) VALUES"

        total_written = 0

        for batch in _batched(rows, self._config.batch_size):
            # clickhouse-driver принимает список dict или список tuple
            dicts = [dict(zip(headers, row)) for row in batch]
            client.execute(sql, dicts)
            total_written += len(batch)
            log.debug("CH: вставлено %d строк (всего %d)", len(batch), total_written)

        log.info("ClickHouseWriter: записано %d строк в %s", total_written, qualified)
        return total_written