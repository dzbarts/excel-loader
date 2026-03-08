"""
writers/database.py
===================
Прямая запись строк в GreenPlum (psycopg2) и ClickHouse (clickhouse-driver).

Обе зависимости уже есть в Airflow-окружении (см. список пакетов).

Изменения vs предыдущей версии:
  - PostgresWriter: имена таблиц/схем/колонок экранируются через psycopg2.sql.Identifier
    (защита от SQL-инъекции через заголовки Excel).
  - PostgresWriter: rollback при ошибке батча — частичные данные не остаются в БД.
  - ClickHouseWriter: client.disconnect() в finally — нет утечки соединений.
"""
from __future__ import annotations

import logging
from typing import Iterable, Iterator

from .base import BaseWriter, DbWriterConfig

log = logging.getLogger(__name__)


# ── Batch helper ────────────────────────────────────────────────────────────────
# Python 3.12+ имеет itertools.batched; держим свою реализацию для совместимости
# с Python 3.10/3.11 (requires-python = ">=3.10" в pyproject.toml).
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


# ── PostgreSQL / GreenPlum ──────────────────────────────────────────────────────
class PostgresWriter(BaseWriter):
    """
    Пишет строки напрямую в GreenPlum / PostgreSQL через psycopg2.

    Использует psycopg2.sql.Identifier для экранирования имён схемы, таблицы
    и колонок — защита от SQL-инъекции через заголовки Excel-файла.

    При ошибке любого батча транзакция откатывается целиком (ROLLBACK),
    частичные данные в БД не остаются.

    Args:
        config: DbWriterConfig с host/port/database/user/password,
                table_name, scheme_name, batch_size.
    """

    def __init__(self, config: DbWriterConfig) -> None:
        self._config = config

    def write(self, headers: list[str], rows: Iterable[tuple]) -> int:
        """
        Записывает строки в таблицу.

        Returns:
            Количество записанных строк.

        Raises:
            ImportError: если psycopg2-binary не установлен.
            Exception:  при ошибке БД — после автоматического ROLLBACK.
        """
        try:
            import psycopg2
            from psycopg2 import sql as pgsql
        except ImportError as exc:
            raise ImportError(
                "psycopg2-binary не установлен. "
                "Добавьте его в зависимости окружения."
            ) from exc

        cfg = self._config
        conn_str = (
            f"host={cfg.host} port={cfg.port} dbname={cfg.database} "
            f"user={cfg.user} password={cfg.password}"
        )

        # Экранируем все идентификаторы через psycopg2.sql.Identifier —
        # это единственный безопасный способ подставлять имена таблиц/колонок.
        qualified = pgsql.SQL("{}.{}").format(
            pgsql.Identifier(cfg.scheme_name),
            pgsql.Identifier(cfg.table_name),
        )
        col_identifiers = pgsql.SQL(", ").join(
            pgsql.Identifier(h) for h in headers
        )
        placeholders = pgsql.SQL(", ").join(pgsql.Placeholder() * len(headers))
        insert_sql = pgsql.SQL("INSERT INTO {table} ({cols}) VALUES ({vals})").format(
            table=qualified,
            cols=col_identifiers,
            vals=placeholders,
        )

        total_written = 0
        with psycopg2.connect(conn_str) as conn:
            try:
                with conn.cursor() as cur:
                    for batch in _batched(rows, cfg.batch_size):
                        cur.executemany(insert_sql, batch)
                        total_written += len(batch)
                        log.debug(
                            "GP: вставлено %d строк (всего %d)",
                            len(batch),
                            total_written,
                        )
                conn.commit()
            except Exception:
                conn.rollback()
                log.exception(
                    "GP: ошибка при записи в %s.%s — выполнен ROLLBACK.",
                    cfg.scheme_name,
                    cfg.table_name,
                )
                raise

        log.info(
            "PostgresWriter: записано %d строк в %s.%s",
            total_written,
            cfg.scheme_name,
            cfg.table_name,
        )
        return total_written


# ── ClickHouse ──────────────────────────────────────────────────────────────────
class ClickHouseWriter(BaseWriter):
    """
    Пишет строки напрямую в ClickHouse через clickhouse-driver.

    clickhouse-driver уже доступен в Airflow-окружении.

    Соединение закрывается в блоке finally — нет утечки соединений
    при долгой работе Airflow-воркера.

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

        Raises:
            ImportError: если clickhouse-driver не установлен.
        """
        try:
            from clickhouse_driver import Client
        except ImportError as exc:
            raise ImportError(
                "clickhouse-driver не установлен. "
                "Добавьте его в зависимости окружения."
            ) from exc

        cfg = self._config
        # scheme_name в CH трактуется как database
        qualified = f"{cfg.scheme_name}.{cfg.table_name}"
        sql = f"INSERT INTO {qualified} ({', '.join(headers)}) VALUES"

        client = Client(
            host=cfg.host,
            port=cfg.port,
            database=cfg.database,
            user=cfg.user,
            password=cfg.password,
        )
        total_written = 0
        try:
            for batch in _batched(rows, cfg.batch_size):
                # clickhouse-driver принимает список dict или список tuple
                dicts = [dict(zip(headers, row)) for row in batch]
                client.execute(sql, dicts)
                total_written += len(batch)
                log.debug(
                    "CH: вставлено %d строк (всего %d)", len(batch), total_written
                )
        finally:
            client.disconnect()

        log.info(
            "ClickHouseWriter: записано %d строк в %s", total_written, qualified
        )
        return total_written