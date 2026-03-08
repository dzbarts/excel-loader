from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .enums import DatabaseType, ErrorMode, DumpType, TimestampField


@dataclass
class LoaderConfig:
    """Конфигурация одного запуска загрузчика.

    Поля кодировок:
        encoding_input  — кодировка входящего CSV/TSV/SQL файла.
                          Для Excel игнорируется: openpyxl читает бинарный XLSX
                          и не принимает кодировку как параметр.
        encoding_output — кодировка исходящего SQL/CSV файла.
    """

    input_file: Path
    db_type: DatabaseType

    sheet_name: Optional[str] = None
    skip_rows: int = 0
    skip_cols: int = 0

    table_name: str = "table_name"
    scheme_name: str = "scheme_name"

    dump_type: DumpType = DumpType.SQL
    error_mode: ErrorMode = ErrorMode.IGNORE

    # encoding_input: используется только для CSV/TSV/SQL.
    # Excel (.xlsx) кодировку не принимает — openpyxl читает бинарный формат.
    encoding_input: str = "utf-8"
    # encoding_output: кодировка создаваемого SQL/CSV файла.
    encoding_output: str = "utf-8"

    batch_size: int = 500
    delimiter: str = ","

    timestamp: Optional[TimestampField] = None
    max_row: Optional[int] = None
    wf_load_idn: Optional[str] = None
    is_strip: bool = False
    set_empty_str_to_null: bool = True

    # dict[col_name, type_str] — порядок столбцов в DDL может не совпадать
    # с порядком в Excel. Получить через parse_ddl() или передать вручную:
    # {"id": "integer", "name": "text"}.
    # Столбцы из Excel, отсутствующие в dtypes, проходят без валидации.
    dtypes: Optional[dict[str, str]] = None


@dataclass(frozen=True)
class CellValidationError:
    """Одна ошибка валидации в конкретной ячейке."""

    cell_name: str    # например "B5"
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
    """Итог выполнения load().

    Поля:
        rows_written  — количество строк, записанных в выходной файл.
        rows_skipped  — количество строк, пропущенных (пустые строки в источнике).
        output_file   — путь к созданному SQL/CSV файлу (None при error_mode=VERIFY).
        error_file    — путь к файлу с ошибками (None если ошибок не было).
        has_errors    — True если в данных обнаружены ошибки валидации.
        validation_result — полный список ошибок; None если валидация не запускалась.
    """

    rows_written: int
    rows_skipped: int = 0
    output_file: Optional[Path] = None
    error_file: Optional[Path] = None
    has_errors: bool = False
    validation_result: Optional[FileValidationResult] = None

    # Алиас для обратной совместимости с кодом, который использовал output_path.
    @property
    def output_path(self) -> Optional[Path]:
        return self.output_file