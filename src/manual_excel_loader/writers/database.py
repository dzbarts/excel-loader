from __future__ import annotations

from typing import Iterable

from .base import BaseWriter, DbWriterConfig


class PostgresWriter(BaseWriter):
    """
    Writes rows directly to GreenPlum / PostgreSQL via psycopg2.

    Not yet implemented — will be added in the Airflow integration stage.
    Requires: psycopg2-binary (already available in the Airflow environment).
    """

    def __init__(self, config: DbWriterConfig) -> None:
        self._config = config

    def write(self, headers: list[str], rows: Iterable[tuple]) -> None:
        raise NotImplementedError("PostgresWriter will be implemented in Stage 5.")


class ClickHouseWriter(BaseWriter):
    """
    Writes rows directly to ClickHouse via clickhouse-driver.

    Not yet implemented — will be added in the Airflow integration stage.
    Requires: clickhouse-driver (already available in the Airflow environment).
    """

    def __init__(self, config: DbWriterConfig) -> None:
        self._config = config

    def write(self, headers: list[str], rows: Iterable[tuple]) -> None:
        raise NotImplementedError("ClickHouseWriter will be implemented in Stage 5.")