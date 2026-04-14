from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import IO, Iterable

from ..enums import DatabaseType


class BaseWriter(ABC):
    """Общий интерфейс для всех врайтеров.

    Конфигурация передаётся один раз в конструктор через frozen-датакласс.
    Единственный публичный метод — write(), который принимает заголовки
    и итерируемый набор уже провалидированных строк.
    """

    @abstractmethod
    def write(self, headers: list[str], rows: Iterable[tuple]) -> int: ...


# ── Конфиг файловых врайтеров ─────────────────────────────────────────────────

@dataclass(frozen=True)
class FileWriterConfig:
    """Общий конфиг для SQL и CSV врайтеров.

    output_stream — открытый текстовый поток для записи.
    Управление его жизнью (открытие, закрытие) лежит на вызывающем коде (loader.py).
    """

    output_stream: IO[str] = field(hash=False)
    db_type: DatabaseType       # нужен SqlFileWriter для выбора стратегии экранирования
    table_name: str
    scheme_name: str
    encoding: str = "utf-8"
    batch_size: int = 10        # только для SQL, CsvFileWriter игнорирует
    delimiter: str = ","        # только для CSV, SqlFileWriter игнорирует
