from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from ..enums import DatabaseType


class BaseWriter(ABC):
    """Общий интерфейс для всех врайтеров.

    Конфигурация передаётся один раз в конструктор через frozen-датакласс.
    Единственный публичный метод — write(), который принимает заголовки
    и итерируемый набор уже провалидированных строк.
    """

    @abstractmethod
    def write(self, headers: list[str], rows: Iterable[tuple]) -> None: ...


# ── Конфиг файловых врайтеров ─────────────────────────────────────────────────

@dataclass(frozen=True)
class FileWriterConfig:
    """Общий конфиг для SQL и CSV врайтеров."""

    output_path: Path
    db_type: DatabaseType       # нужен SqlFileWriter для выбора стратегии экранирования
    table_name: str
    scheme_name: str
    encoding: str = "utf-8"
    batch_size: int = 10        # только для SQL, CsvFileWriter игнорирует
    delimiter: str = ","        # только для CSV, SqlFileWriter игнорирует


# ── Конфиг DB-врайтеров ───────────────────────────────────────────────────────

@dataclass(frozen=True)
class DbWriterConfig:
    """Параметры подключения и целевой таблицы для прямой записи в БД."""

    host: str
    port: int
    database: str
    user: str
    password: str
    table_name: str
    scheme_name: str
    batch_size: int = 1000
