"""
manual-excel-loader
~~~~~~~~~~~~~~~~~~~
Загрузчик табличных файлов (Excel / CSV / TSV / SQL) в GreenPlum и ClickHouse
с валидацией типов и генерацией SQL/CSV-дампов.

Pipeline: read_file() → (validate) → writer.write()

Публичный API:
    load(config)       — полный pipeline: чтение → валидация → запись файла.
    load_rows(config)  — только чтение и валидация; возвращает итератор строк
                         для прямой загрузки в БД через DbWriter.

Конфигурация и результат:
    LoaderConfig         — конфигурация одного запуска загрузчика.
    LoadResult           — результат выполнения load().

Enums:
    DatabaseType         — целевая БД (greenplum / postgres / clickhouse).
    DumpType             — формат дампа (sql / csv).
    ErrorMode            — режим обработки ошибок валидации (raise / coerce / ignore / verify).
    TimestampField       — имя поля с меткой времени загрузки.

Модели валидации:
    CellValidationError  — ошибка в одной ячейке (строка, колонка, значение, причина).
    FileValidationResult — сводный результат валидации файла; входит в DataValidationError.

Исключения:
    ExcelLoaderError       — базовый класс для всех ошибок пакета.
    FileReadError          — файл не найден или не читается.
    HeaderValidationError  — пустой, дублирующийся или недопустимый заголовок.
    DataValidationError    — ячейки не прошли валидацию; несёт .validation_result.
    ConfigurationError     — некорректная конфигурация LoaderConfig.
    UnsupportedDataTypeError — тип данных не поддерживается целевой БД.
    DumpCreationError      — ошибка при создании SQL/CSV-дампа.
    TemplateError          — ошибка при чтении или применении шаблона Excel.
"""

from .enums import DatabaseType, DumpType, ErrorMode, TimestampField
from .exceptions import (
    ExcelLoaderError,
    FileReadError,
    HeaderValidationError,
    DataValidationError,
    ConfigurationError,
    UnsupportedDataTypeError,
    DumpCreationError,
    TemplateError,
)
from .models import LoaderConfig, LoadResult, CellValidationError, FileValidationResult
from .loader import load, load_rows

__all__ = [
    # Main entry points
    "load",
    "load_rows",
    # Config & result
    "LoaderConfig",
    "LoadResult",
    # Enums
    "DatabaseType",
    "DumpType",
    "ErrorMode",
    "TimestampField",
    # Validation models
    "CellValidationError",
    "FileValidationResult",
    # Exceptions
    "ExcelLoaderError",
    "FileReadError",
    "HeaderValidationError",
    "DataValidationError",
    "ConfigurationError",
    "UnsupportedDataTypeError",
    "DumpCreationError",
    "TemplateError",
]