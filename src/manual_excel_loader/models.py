from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .enums import DatabaseType, ErrorMode, DumpType, TimestampField


@dataclass
class LoaderConfig:
    """Конфигурация одного запуска загрузчика."""

    input_file: Path
    db_type: DatabaseType

    sheet_name: Optional[str] = None
    skip_rows: int = 0
    skip_cols: int = 0
    table_name: str = "table_name"
    scheme_name: str = "scheme_name"
    dump_type: DumpType = DumpType.SQL
    error_mode: ErrorMode = ErrorMode.IGNORE
    encoding_input: str = "utf-8"
    encoding_output: str = "utf-8"
    batch_size: int = 10
    delimiter: str = ","
    timestamp: Optional[TimestampField] = None
    max_row: Optional[int] = None
    wf_load_idn: Optional[str] = None
    is_strip: bool = False
    set_empty_str_to_null: bool = True

    # KEY CHANGE: dict[col_name, type_str] instead of list[str].
    # This allows column order in DDL to differ from column order in Excel.
    # Obtain from parse_ddl() or pass manually: {"id": "integer", "name": "text"}
    # Columns in Excel but absent from dtypes are passed through without validation.
    dtypes: Optional[dict[str, str]] = None


@dataclass(frozen=True)
class CellValidationError:
    """Одна ошибка валидации в конкретной ячейке."""

    cell_name: str    # "B5"
    cell_value: object
    expected_type: str
    message: str


@dataclass
class FileValidationResult:
    """Итог валидации всего файла."""

    is_valid: bool = True
    errors: list[CellValidationError] = field(default_factory=list)

    def add_error(self, error: CellValidationError) -> None:
        self.errors.append(error)
        self.is_valid = False


@dataclass
class LoadResult:
    """Minimal summary returned by load(). Not statistics — just facts."""

    rows_written: int
    output_path: Path