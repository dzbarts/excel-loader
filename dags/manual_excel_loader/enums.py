"""Перечисления для excel-loader"""

from enum import Enum


class DatabaseType(str, Enum):
    GREENPLUM = "greenplum"
    POSTGRES = "postgres"
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
    WRITE_TS = "write_ts"   # метка времени записи в БД - STG/INT
    LOAD_DTTM = "load_dttm" # метка времени записи в БД - MRT