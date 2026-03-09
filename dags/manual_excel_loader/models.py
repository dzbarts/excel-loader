from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .enums import DatabaseType, ErrorMode, DumpType, TimestampField

# Единый список поддерживаемых кодировок.
# encoding_input — применяется при чтении CSV/TSV/SQL-файлов.
# encoding_output — применяется при записи SQL/CSV-файлов.
# Для Excel (.xlsx) кодировка не нужна: openpyxl читает бинарный формат.
SUPPORTED_ENCODINGS: frozenset[str] = frozenset({
    "utf-8", "utf-16", "utf-16-le", "utf-16-be",
    "ascii", "latin1", "cp1252", "cp1251", "cp866",
    "koi8-r", "koi8-u", "iso-8859-5",
    "gbk", "big5", "shift_jis", "euc-jp", "euc-kr",
})


@dataclass
class LoaderConfig:
    """Конфигурация одного запуска загрузчика.

    Поля кодировок:
        encoding_input  — кодировка входящего CSV/TSV/SQL файла.
            Для Excel игнорируется: openpyxl читает бинарный XLSX.
        encoding_output — кодировка исходящего SQL/CSV файла.

    Поля прогресса:
        show_progress — показывать tqdm прогресс-бар при ручном запуске.
            Оставьте False (по умолчанию) при запуске через Airflow.
    """

    input_file: Path
    db_type: DatabaseType
    sheet_name: str | None = None
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
    timestamp: TimestampField | None = None
    max_row: int | None = None
    wf_load_idn: str | None = None
    is_strip: bool = False
    set_empty_str_to_null: bool = True

    # dict[col_name, type_str] — порядок столбцов в DDL может не совпадать
    # с порядком в Excel. Получить через parse_ddl() или передать вручную:
    # {"id": "integer", "name": "text"}.
    # Столбцы из Excel, отсутствующие в dtypes, проходят без валидации.
    dtypes: dict[str, str] | None = None

    # Показывать tqdm прогресс-бар (только для ручного запуска).
    # При запуске через Airflow оставьте False.
    show_progress: bool = False

    # Директория для TXT-отчёта валидации.
    # None (по умолчанию) — файл не создаётся, ошибки только в логах.
    # Передайте Path, чтобы записать отчёт; удобный дефолт: input_file.parent.
    validation_report_dir: Path | None = None
    # Включить примеры значений в TXT-отчёт (может содержать чувствительные данные).
    validation_report_include_values: bool = False

    def __post_init__(self) -> None:
        """Базовая валидация конфига при создании объекта.

        Позволяет поймать очевидные ошибки конфигурации до запуска pipeline,
        а не в середине обработки большого файла.

        Кодировки проверяются всегда при создании объекта: encoding_output
        используется при любом запуске; encoding_input — только для текстовых
        форматов, но ошибку лучше поймать до чтения файла.
        """
        if self.batch_size <= 0:
            raise ValueError(
                f"batch_size must be a positive integer, got {self.batch_size}."
            )
        if self.skip_rows < 0:
            raise ValueError(f"skip_rows must be >= 0, got {self.skip_rows}.")
        if self.skip_cols < 0:
            raise ValueError(f"skip_cols must be >= 0, got {self.skip_cols}.")
        if self.max_row is not None and self.max_row <= 0:
            raise ValueError(f"max_row must be a positive integer, got {self.max_row}.")
        if self.encoding_input.lower() not in SUPPORTED_ENCODINGS:
            raise ValueError(
                f"Unsupported encoding_input '{self.encoding_input}'. "
                f"Supported: {sorted(SUPPORTED_ENCODINGS)}"
            )
        if self.encoding_output.lower() not in SUPPORTED_ENCODINGS:
            raise ValueError(
                f"Unsupported encoding_output '{self.encoding_output}'. "
                f"Supported: {sorted(SUPPORTED_ENCODINGS)}"
            )


@dataclass(frozen=True)
class CellValidationError:
    """Одна ошибка валидации в конкретной ячейке."""

    cell_name: str   # например "B5"
    cell_value: Any
    expected_type: str
    message: str
    col_name: str = ""  # имя колонки из заголовка (например "sale_date")


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
        rows_written      — количество строк, записанных в выходной файл.
        rows_skipped      — количество строк, пропущенных (пустые строки в источнике).
        output_file       — путь к созданному SQL/CSV файлу (None при error_mode=VERIFY).
        error_file        — путь к файлу с ошибками (None если ошибок не было).
        has_errors        — True если в данных обнаружены ошибки валидации.
        validation_result — полный список ошибок; None если валидация не запускалась.
    """

    rows_written: int
    rows_skipped: int = 0
    output_file: Path | None = None
    error_file: Path | None = None
    has_errors: bool = False
    validation_result: FileValidationResult | None = None

    # Алиас для обратной совместимости с кодом, который использовал output_path.
    @property
    def output_path(self) -> Path | None:
        return self.output_file