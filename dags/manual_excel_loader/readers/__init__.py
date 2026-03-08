"""
readers/
========

Все ридеры пакета собраны здесь. Каждый читает свой формат и возвращает
унифицированный SheetData — loader работает только с этим типом и не знает
о деталях конкретного формата.

Форматы:
    .xlsx / .xls / .xlsm  →  excel_reader.read_excel()
    .csv / .tsv            →  csv_reader.read_csv()
    .sql / .txt            →  sql_reader.read_sql()

Публичный API этого пакета
--------------------------
SheetData          — унифицированный результат любого ридера
read_file(path)    — автоматически выбрать нужный ридер по расширению файла
ExcelReadConfig    — конфиг для Excel
CsvReadConfig      — конфиг для CSV/TSV
SqlReadConfig      — конфиг для SQL

Для тонкой настройки импортируй конкретный ридер напрямую:

    from manual_excel_loader.readers.excel_reader import read_excel, ExcelReadConfig
"""

from __future__ import annotations

from pathlib import Path

from .csv_reader import CsvReadConfig, CsvSheetData, read_csv
from .excel_reader import ExcelReadConfig, SheetData, read_excel
from .sql_reader import SqlReadConfig, SqlSheetData, read_sql

__all__ = [
    # Унифицированный тип
    "SheetData",
    # Excel
    "ExcelReadConfig",
    "read_excel",
    # CSV / TSV
    "CsvReadConfig",
    "CsvSheetData",
    "read_csv",
    # SQL
    "SqlReadConfig",
    "SqlSheetData",
    "read_sql",
    # Фабрика
    "read_file",
]

# Расширения → ридер (строки, чтобы не тянуть openpyxl при импорте пакета)
_EXCEL_SUFFIXES = frozenset({".xlsx", ".xls", ".xlsm"})
_CSV_SUFFIXES = frozenset({".csv", ".tsv"})
_SQL_SUFFIXES = frozenset({".sql", ".txt"})


def read_file(
    path: Path,
    *,
    encoding: str = "utf-8",
    delimiter: str = ",",
    sheet_name: str | None = None,
    skip_rows: int = 0,
    skip_cols: int = 0,
    max_row: int | None = None,
    skip_header_validation: bool = False,
) -> SheetData:
    """Прочитать файл любого поддерживаемого формата.

    Автоматически выбирает нужный ридер по расширению файла и возвращает
    SheetData — единый тип для всех форматов.

    Args:
        path:                   путь к файлу.
        encoding:               кодировка (только для CSV/TSV/SQL).
        delimiter:              разделитель (только для CSV).
        sheet_name:             лист (только для Excel).
        skip_rows:              строк пропустить перед заголовком.
        skip_cols:              столбцов пропустить слева.
        max_row:                максимальное число строк данных.
        skip_header_validation: не проверять символы в заголовке
                                (только для шаблонных Excel-файлов).

    Returns:
        SheetData с заголовками и итератором строк.

    Raises:
        ConfigurationError: неподдерживаемое расширение.
        FileReadError:      файл не найден или не читается.
        HeaderValidationError: некорректный заголовок.
    """
    from ..exceptions import ConfigurationError

    suffix = path.suffix.lower()

    if suffix in _EXCEL_SUFFIXES:
        cfg = ExcelReadConfig(
            path=path,
            sheet_name=sheet_name,
            skip_rows=skip_rows,
            skip_cols=skip_cols,
            max_row=max_row,
            skip_header_validation=skip_header_validation,
        )
        return read_excel(cfg)

    if suffix in _CSV_SUFFIXES:
        csv_cfg = CsvReadConfig(
            path=path,
            delimiter="\t" if suffix == ".tsv" else delimiter,
            encoding=encoding,
            skip_rows=skip_rows,
            skip_cols=skip_cols,
            max_row=max_row,
        )
        csv_data = read_csv(csv_cfg)
        return SheetData(
            headers=csv_data.headers,
            rows=iter(csv_data.rows),
            source_path=csv_data.source_path,
        )

    if suffix in _SQL_SUFFIXES:
        sql_cfg = SqlReadConfig(path=path, encoding=encoding)
        sql_data = read_sql(sql_cfg)
        return SheetData(
            headers=sql_data.headers,
            rows=iter(sql_data.rows),
            source_path=sql_data.source_path,
        )

    raise ConfigurationError(
        f"Unsupported file format: '{suffix}'. "
        f"Supported: "
        f"{', '.join(sorted(_EXCEL_SUFFIXES | _CSV_SUFFIXES | _SQL_SUFFIXES))}"
    )