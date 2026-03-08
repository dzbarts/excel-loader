from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from ..enums import DatabaseType


class BaseWriter(ABC):
    """
    Common interface for all output sinks.

    Each concrete writer receives its configuration once at construction time
    (via a frozen dataclass) and exposes a single write() method that
    accepts column headers and an iterable of already-validated rows.
    """

    @abstractmethod
    def write(self, headers: list[str], rows: Iterable[tuple]) -> None:
        """Write rows to the configured destination."""


# ── File-writer config ────────────────────────────────────────────────────────

@dataclass(frozen=True)
class FileWriterConfig:
    """Configuration shared by SQL and CSV file writers."""

    output_path: Path
    db_type: DatabaseType       # needed by SqlFileWriter to pick escape strategy
    table_name: str
    scheme_name: str
    encoding: str = "utf-8"
    batch_size: int = 10        # SQL only — ignored by CsvFileWriter
    delimiter: str = ","        # CSV only — ignored by SqlFileWriter


# ── Database-writer config ────────────────────────────────────────────────────

@dataclass(frozen=True)
class DbWriterConfig:
    """Connection + target-table configuration for direct database writers."""

    host: str
    port: int
    database: str
    user: str
    password: str
    table_name: str
    scheme_name: str
    batch_size: int = 1000