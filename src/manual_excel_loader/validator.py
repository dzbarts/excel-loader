from __future__ import annotations

import re
from datetime import datetime, date, time
from typing import Any, Callable
from dateutil import parser as dateutil_parser

from .enums import DatabaseType
from .exceptions import UnsupportedDataTypeError
from .result import Ok, Err, CellResult


# ── Примитивные валидаторы ────────────────────────────────────────────────────
# Чистые функции: принимают значение, возвращают Ok(преобразованное) или Err(причина)

def validate_integer(value: Any, min_val: int, max_val: int) -> CellResult[int]:
    try:
        v = int(value)
    except (ValueError, TypeError):
        return Err("не является целым числом")
    if not (min_val <= v <= max_val):
        return Err(f"значение {v} вне диапазона [{min_val}, {max_val}]")
    return Ok(v)


def validate_float(value: Any, min_val: float, max_val: float) -> CellResult[float]:
    try:
        v = float(value)
    except (ValueError, TypeError):
        return Err("не является числом с плавающей точкой")
    if not (min_val <= v <= max_val):
        return Err(f"значение вне допустимого диапазона")
    return Ok(v)


def validate_decimal(value: Any, precision: int = 32, scale: int = 8) -> CellResult[float]:
    if "," in str(value):
        return Err("используй точку вместо запятой в дробных числах")
    try:
        v = float(value)
    except (ValueError, TypeError):
        return Err("не является числом")
    s = str(v)
    int_part, _, dec_part = s.partition(".")
    int_part = int_part.lstrip("-")
    if len(int_part) + len(dec_part) > precision:
        return Err(f"превышена общая точность: {len(int_part) + len(dec_part)} > {precision}")
    return Ok(round(v, scale))


def validate_string(value: Any) -> CellResult[str]:
    return Ok(str(value))


def validate_datetime(
    value: Any,
    min_dt: datetime,
    max_dt: datetime,
    fmt: str = "%Y-%m-%d %H:%M:%S",
) -> CellResult[str]:
    try:
        if not isinstance(value, (datetime, date, time)):
            value = dateutil_parser.parse(str(value))
    except (ValueError, TypeError):
        return Err("не удалось распознать дату/время")
    if not (min_dt <= value <= max_dt):
        return Err(f"дата вне допустимого диапазона [{min_dt.date()} — {max_dt.date()}]")
    return Ok(value.strftime(fmt))


def validate_boolean_gp(value: Any) -> CellResult[str]:
    allowed = {"true", "false", "0", "1", "t", "f", "y", "n", "yes", "no", "on", "off", "null"}
    v = str(value).strip().lower()
    if v not in allowed:
        return Err(f"допустимые значения: {sorted(allowed)}")
    return Ok(v)


def validate_uuid(value: Any) -> CellResult[str]:
    pattern = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    v = str(value).strip()
    if not re.fullmatch(pattern, v):
        return Err("не соответствует формату UUID (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)")
    return Ok(v)


# ── Маппинг типов → валидаторы ────────────────────────────────────────────────
# Создаётся ОДИН РАЗ на уровне модуля, не внутри функции

_GP_VALIDATORS: dict[str, Callable[[Any], CellResult]] = {
    "smallint":         lambda v: validate_integer(v, -32768, 32767),
    "integer":          lambda v: validate_integer(v, -2147483648, 2147483647),
    "bigint":           lambda v: validate_integer(v, -9223372036854775808, 9223372036854775807),
    "smallserial":      lambda v: validate_integer(v, 1, 32767),
    "serial":           lambda v: validate_integer(v, 1, 2147483647),
    "bigserial":        lambda v: validate_integer(v, 1, 9223372036854775807),
    "real":             lambda v: validate_float(v, -3.4e38, 3.4e38),
    "double precision": lambda v: validate_float(v, -1.7e308, 1.7e308),
    "text":             lambda v: validate_string(v),
    "boolean":          lambda v: validate_boolean_gp(v),
    "uuid":             lambda v: validate_uuid(v),
    "date":             lambda v: validate_datetime(
                            v, datetime(1, 1, 1), datetime(9999, 12, 31), "%Y-%m-%d"
                        ),
    "timestamp":        lambda v: validate_datetime(
                            v, datetime(1, 1, 1), datetime(9999, 12, 31, 23, 59, 59)
                        ),
}

_CH_VALIDATORS: dict[str, Callable[[Any], CellResult]] = {
    "int8":    lambda v: validate_integer(v, -128, 127),
    "int16":   lambda v: validate_integer(v, -32768, 32767),
    "int32":   lambda v: validate_integer(v, -2147483648, 2147483647),
    "int64":   lambda v: validate_integer(v, -9223372036854775808, 9223372036854775807),
    "uint8":   lambda v: validate_integer(v, 0, 255),
    "uint16":  lambda v: validate_integer(v, 0, 65535),
    "uint32":  lambda v: validate_integer(v, 0, 4294967295),
    "uint64":  lambda v: validate_integer(v, 0, 18446744073709551615),
    "float32": lambda v: validate_float(v, -3.4e38, 3.4e38),
    "float64": lambda v: validate_float(v, -1.7e308, 1.7e308),
    "string":  lambda v: validate_string(v),
    "bool":    lambda v: validate_boolean_gp(v),
    "uuid":    lambda v: validate_uuid(v),
    "date":    lambda v: validate_datetime(
                   v, datetime(1970, 1, 1), datetime(2105, 12, 31), "%Y-%m-%d"
               ),
    "date32":  lambda v: validate_datetime(
                   v, datetime(1900, 1, 1), datetime(2299, 12, 31), "%Y-%m-%d"
               ),
    "datetime": lambda v: validate_datetime(
                    v, datetime(1970, 1, 1), datetime(2105, 12, 31, 23, 59, 59)
                ),
}


# ── Публичный интерфейс ───────────────────────────────────────────────────────

def get_validator(type_name: str, db_type: DatabaseType) -> Callable[[Any], CellResult]:
    """
    Возвращает функцию-валидатор для указанного типа данных и БД.
    Raises UnsupportedDataTypeError если тип не поддерживается.
    """
    registry = _GP_VALIDATORS if db_type == DatabaseType.GREENPLUM else _CH_VALIDATORS
    type_lower = type_name.lower().strip()

    # Точное совпадение
    if type_lower in registry:
        return registry[type_lower]

    # Decimal/Numeric с параметрами: decimal(10,2) или numeric(18,4)
    decimal_match = re.match(r"(?:decimal|numeric)\((\d+),\s*(\d+)\)", type_lower)
    if decimal_match:
        precision, scale = int(decimal_match.group(1)), int(decimal_match.group(2))
        return lambda v: validate_decimal(v, precision, scale)

    # varchar(n) и char(n)
    varchar_match = re.match(r"(?:character varying|varchar)\((\d+)\)", type_lower)
    if varchar_match:
        size = int(varchar_match.group(1))
        return lambda v: (Ok(str(v)) if len(str(v)) <= size
                         else Err(f"длина {len(str(v))} превышает максимум {size}"))

    char_match = re.match(r"char\((\d+)\)", type_lower)
    if char_match:
        size = int(char_match.group(1))
        return lambda v: (Ok(str(v)) if len(str(v)) == size
                         else Err(f"ожидается ровно {size} символов, получено {len(str(v))}"))

    raise UnsupportedDataTypeError(
        f"Тип '{type_name}' не поддерживается для {db_type.value}. "
        f"Доступные типы: {sorted(registry.keys())}"
    )