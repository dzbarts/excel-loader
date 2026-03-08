"""
writers/base.py
===============
Базовые абстракции для всех writer-ов.
"""
from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from typing import Iterable


@dataclasses.dataclass(frozen=True)
class FileWriterConfig:
    """Конфиг для файловых writer-ов (SQL, CSV)."""
    output_path: str
    table_name: str = "table_name"
    scheme_name: str = "scheme_name"
    batch_size: int = 500
    encoding: str = "utf-8"
    delimiter: str = ","


@dataclasses.dataclass(frozen=True)
class DbWriterConfig:
    """Конфиг для database writer-ов (Postgres/GP, ClickHouse)."""
    dsn: str                          # connection string
    table_name: str = "table_name"
    scheme_name: str = "scheme_name"
    batch_size: int = 1000


class BaseWriter(ABC):
    """Общий интерфейс для всех writer-ов."""

    @abstractmethod
    def write(self, headers: list[str], rows: Iterable[tuple]) -> int:
        """
        Записывает строки.

        Args:
            headers: список имён колонок.
            rows: итерируемый объект с кортежами значений.

        Returns:
            Количество записанных строк.
        """
        ...