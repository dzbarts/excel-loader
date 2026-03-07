# src/excel_loader/exceptions.py

class ExcelLoaderError(Exception):
    """Базовое исключение проекта. Лови это если хочешь поймать всё."""
    pass

class FileReadError(ExcelLoaderError):
    """Не удалось прочитать входной файл."""
    pass

class HeaderValidationError(ExcelLoaderError):
    """Проблема с заголовками таблицы."""
    pass

class DataValidationError(ExcelLoaderError):
    """Данные не соответствуют ожидаемым типам."""
    pass

class UnsupportedDataTypeError(ExcelLoaderError):
    """Передан неизвестный тип данных."""
    pass

class ConfigurationError(ExcelLoaderError):
    """Некорректная конфигурация запуска."""
    pass

class DatabaseConnectionError(ExcelLoaderError):
    """Проблема с подключением к базе данных."""
    pass

class DataInsertionError(ExcelLoaderError):
    """Ошибка при вставке данных в базу."""
    pass

class DumpCreationError(ExcelLoaderError):
    """Ошибка при создании дампа данных."""
    pass

class TemplateError(ExcelLoaderError):
    """Проблема с excel шаблоном."""
    pass
