"""
manual-excel-loader
~~~~~~~~~~~~~~~~~~~
Excel → SQL/CSV loader with data validation for GreenPlum and ClickHouse.

Public API:
    load()        — run the full pipeline
    LoaderConfig  — configure a run
    LoadResult    — result returned by load()

Exceptions:
    ExcelLoaderError       — base class
    FileReadError
    HeaderValidationError
    DataValidationError    — carries .validation_result
    ConfigurationError
    UnsupportedDataTypeError
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