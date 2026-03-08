from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import FileValidationResult


class ExcelLoaderError(Exception):
    """Base exception for all excel-loader errors."""


class FileReadError(ExcelLoaderError):
    """Cannot open or read the source file."""


class HeaderValidationError(ExcelLoaderError):
    """Column headers are missing, invalid, or duplicated."""


class DataValidationError(ExcelLoaderError):
    """One or more cells failed type validation.

    Attributes:
        validation_result: Full FileValidationResult with all cell errors.
    """

    def __init__(self, message: str, validation_result: FileValidationResult) -> None:
        super().__init__(message)
        self.validation_result = validation_result


class UnsupportedDataTypeError(ExcelLoaderError):
    """The requested data type is not supported for the target database."""


class ConfigurationError(ExcelLoaderError):
    """Invalid or incomplete loader configuration."""


class DumpCreationError(ExcelLoaderError):
    """Failed to create the output SQL/CSV file."""


class TemplateError(ExcelLoaderError):
    """ODS template structure is invalid or inconsistent."""