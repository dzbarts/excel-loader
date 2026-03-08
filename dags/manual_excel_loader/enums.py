"""Перечисления для excel-loader.

Все Enum наследуют str, поэтому их значения можно сравнивать со строками
и сериализовывать в JSON без дополнительных преобразований.
"""

from enum import Enum


class DatabaseType(str, Enum):
    GREENPLUM = "greenplum"
    CLICKHOUSE = "clickhouse"


class ErrorMode(str, Enum):
    RAISE = "raise"    # проверить; при ошибках — поднять исключение
    COERCE = "coerce"  # заменить ошибочные ячейки на NULL, продолжить
    IGNORE = "ignore"  # выгрузить как есть, без валидации
    VERIFY = "verify"  # только проверить, без выгрузки


class DumpType(str, Enum):
    SQL = "sql"
    CSV = "csv"


class TimestampField(str, Enum):
    WRITE_TS = "write_ts"
    LOAD_DTTM = "load_dttm"