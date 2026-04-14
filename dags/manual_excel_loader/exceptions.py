from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import FileValidationResult


class ExcelLoaderError(Exception):
    """Базовый класс для всех ошибок excel-loader."""


class FileReadError(ExcelLoaderError):
    """Файл не найден или не читается."""


class HeaderValidationError(ExcelLoaderError):
    """Заголовок пустой, содержит недопустимые символы или дублирующиеся имена."""


class DataValidationError(ExcelLoaderError):
    """Одна или несколько ячеек не прошли валидацию типов.

    Атрибут validation_result содержит полный список ошибок по ячейкам.
    """

    def __init__(self, message: str, validation_result: FileValidationResult) -> None:
        super().__init__(message)
        self.validation_result = validation_result


class UnsupportedDataTypeError(ExcelLoaderError):
    """Тип данных не поддерживается для выбранной БД."""


class ConfigurationError(ExcelLoaderError):
    """Некорректный или неполный конфиг загрузчика."""


class DumpCreationError(ExcelLoaderError):
    """Ошибка при создании выходного SQL/CSV файла."""


class TemplateError(ExcelLoaderError):
    """Структура шаблона ODS нарушена или несовместима."""
