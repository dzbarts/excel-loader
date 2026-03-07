# src/excel_loader/enums.py
from enum import Enum

class DatabaseType(str, Enum):
    GREENPLUM = "greenplum"
    CLICKHOUSE = "clickhouse"

class ErrorMode(str, Enum):
    RAISE = "raise"      # проверить и упасть
    COERCE = "coerce"    # заменить ошибки на NULL
    IGNORE = "ignore"    # выгрузить как есть
    VERIFY = "verify"    # только проверить, не выгружать

class DumpType(str, Enum):
    SQL = "sql"
    CSV = "csv"

class TimestampField(str, Enum):
    WRITE_TS = "write_ts"
    LOAD_DTTM = "load_dttm"