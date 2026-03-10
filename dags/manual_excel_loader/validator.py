from __future__ import annotations

import re
from datetime import datetime, date, time
from decimal import Decimal, InvalidOperation
from typing import Any, Callable
from dateutil import parser as dateutil_parser

from .enums import DatabaseType
from .exceptions import UnsupportedDataTypeError
from .result import Ok, Err, CellResult


# ── Примитивные валидаторы ───────────────────────────────────────────────────


def validate_integer(value: Any, min_val: int, max_val: int) -> CellResult[int]:
    try:
        v = int(value)
    except (ValueError, TypeError):
        return Err("not an integer")
    if not (min_val <= v <= max_val):
        return Err(f"value {v} out of range [{min_val}, {max_val}]")
    return Ok(v)


def validate_float(value: Any, min_val: float, max_val: float) -> CellResult[float]:
    try:
        v = float(value)
    except (ValueError, TypeError):
        return Err("not a floating-point number")
    if not (min_val <= v <= max_val):
        return Err(f"value out of allowed range [{min_val}, {max_val}]")
    return Ok(v)


def validate_decimal(value: Any, precision: int = 32, scale: int = 8) -> CellResult[float]:
    """Валидация DECIMAL(precision, scale).

    precision — всего значащих цифр; scale — из них после запятой.
    Использует decimal.Decimal для корректной обработки научной нотации и NaN/Inf.
    Float-значения из Excel округляются до scale знаков перед проверкой —
    это корректно, т.к. при записи в БД они всё равно будут округлены.
    """
    if isinstance(value, str) and "," in value:
        return Err("use a dot instead of a comma as decimal separator")
    try:
        d = Decimal(str(round(value, scale) if isinstance(value, float) else value))
    except InvalidOperation:
        return Err("not a number")
    sign, digits, exponent = d.as_tuple()
    if not isinstance(exponent, int):
        # Infinity или NaN
        return Err("not a finite number")
    frac_digits = max(0, -exponent)
    int_digits = max(0, len(digits) + exponent)
    if frac_digits > scale:
        return Err(f"fractional digits ({frac_digits}) exceed scale ({scale})")
    if int_digits > precision - scale:
        return Err(
            f"integer digits ({int_digits}) exceed {precision - scale} "
            f"(precision={precision}, scale={scale})"
        )
    return Ok(round(float(d), scale))


def validate_string(value: Any) -> CellResult[str]:
    return Ok(str(value))


def validate_datetime(
    value: Any,
    min_dt: datetime,
    max_dt: datetime,
    fmt: str = "%Y-%m-%d %H:%M:%S",
) -> CellResult[str]:
    try:
        if isinstance(value, datetime):
            pass  # уже datetime, оставляем как есть
        elif isinstance(value, date):
            value = datetime(value.year, value.month, value.day)
        elif isinstance(value, time):
            # только время без даты — подставляем дату-заглушку для проверки диапазона
            value = datetime(1900, 1, 1, value.hour, value.minute, value.second)
        else:
            value = dateutil_parser.parse(str(value))
    except (ValueError, TypeError):
        return Err("could not parse date/time value")
    if not (min_dt <= value <= max_dt):
        return Err(
            f"date out of allowed range [{min_dt.date()} — {max_dt.date()}]"
        )
    return Ok(value.strftime(fmt))

def validate_time(value: Any) -> CellResult[str]:
    try:
        if isinstance(value, time):
            return Ok(value.strftime("%H:%M:%S"))
        if isinstance(value, datetime):
            return Ok(value.strftime("%H:%M:%S"))
        parsed = dateutil_parser.parse(str(value))
        return Ok(parsed.strftime("%H:%M:%S"))
    except (ValueError, TypeError):
        return Err("could not parse time value")

def validate_boolean_gp(value: Any) -> CellResult[str]:
    # Полный список по документации GP: https://docs.vmware.com/en/VMware-Greenplum/7/
    allowed = {"true", "false", "0", "1", "t", "f", "y", "n", "yes", "no", "on", "off", "null"}
    v = str(value).strip().lower()
    if v not in allowed:
        return Err(f"allowed values: {sorted(allowed)}")
    return Ok(v)


def validate_uuid(value: Any) -> CellResult[str]:
    pattern = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    v = str(value).strip()
    if not re.fullmatch(pattern, v):
        return Err("does not match UUID format (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)")
    return Ok(v)


def validate_interval(value: Any) -> CellResult[str]:
    # GP interval — произвольный текст, проверяем только базовый паттерн
    pattern = re.compile(
        r"^-?\d+\s+(?:year|month|day|hour|minute|second)s?"
        r"(?:\s+-?\d+\s+(?:year|month|day|hour|minute|second)s?)*$",
        re.IGNORECASE,
    )
    v = str(value).strip()
    if not pattern.fullmatch(v):
        return Err("does not match interval format, e.g. '1 year 2 months' or '3 days'")
    return Ok(v)


# ── Маппинг тип → валидатор ───────────────────────────────────────────────────
# Строится один раз при импорте модуля.

_GP_VALIDATORS: dict[str, Callable[[Any], CellResult]] = {
    # https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/ref_guide-data_types.html
    "smallint":   lambda v: validate_integer(v, -32768, 32767),
    "integer":    lambda v: validate_integer(v, -2_147_483_648, 2_147_483_647),
    "bigint":     lambda v: validate_integer(v, -9_223_372_036_854_775_808, 9_223_372_036_854_775_807),

    # serial — автоинкремент, в Excel редкость, но поддерживаем
    "smallserial": lambda v: validate_integer(v, 1, 32_767),
    "serial":      lambda v: validate_integer(v, 1, 2_147_483_647),
    "bigserial":   lambda v: validate_integer(v, 1, 9_223_372_036_854_775_807),

    "real":             lambda v: validate_float(v, -3.4e38, 3.4e38),
    "double precision": lambda v: validate_float(v, -1.7e308, 1.7e308),

    # decimal/numeric/varchar/char — через regex в get_validator
    "text": lambda v: validate_string(v),

    # GP date range: 4713 BC – 294276 AD; ограничено пределами Python datetime
    "date": lambda v: validate_datetime(
        v, datetime(1, 1, 1), datetime(9999, 12, 31), "%Y-%m-%d"
    ),
    "time": validate_time,
    "time without time zone": validate_time,
    "time with time zone":    validate_time,
    # GP timestamp range: 4713 BC – 294276 AD
    "timestamp": lambda v: validate_datetime(  # тот же диапазон что и date
        v, datetime(1, 1, 1), datetime(9999, 12, 31, 23, 59, 59)
    ),
    "timestamp without time zone": lambda v: validate_datetime(
        v, datetime(1, 1, 1), datetime(9999, 12, 31, 23, 59, 59)
    ),
    "timestamp with time zone": lambda v: validate_datetime(
        v, datetime(1, 1, 1), datetime(9999, 12, 31, 23, 59, 59)
    ),
    "interval": lambda v: validate_interval(v),

    "boolean": lambda v: validate_boolean_gp(v),
    "uuid":    lambda v: validate_uuid(v),
    "tsrange": lambda v: validate_string(v),  # сложный range-тип, проверяем как строку
}

_CH_VALIDATORS: dict[str, Callable[[Any], CellResult]] = {
    # https://clickhouse.com/docs/sql-reference/data-types/int-uint
    "int8":   lambda v: validate_integer(v, -128, 127),
    "int16":  lambda v: validate_integer(v, -32_768, 32_767),
    "int32":  lambda v: validate_integer(v, -2_147_483_648, 2_147_483_647),
    "int64":  lambda v: validate_integer(v, -9_223_372_036_854_775_808, 9_223_372_036_854_775_807),
    "int128": lambda v: validate_integer(v, -(2**127), 2**127 - 1),
    "int256": lambda v: validate_integer(v, -(2**255), 2**255 - 1),

    "uint8":   lambda v: validate_integer(v, 0, 255),
    "uint16":  lambda v: validate_integer(v, 0, 65_535),
    "uint32":  lambda v: validate_integer(v, 0, 4_294_967_295),
    "uint64":  lambda v: validate_integer(v, 0, 18_446_744_073_709_551_615),
    "uint128": lambda v: validate_integer(v, 0, 2**128 - 1),
    "uint256": lambda v: validate_integer(v, 0, 2**256 - 1),

    # https://clickhouse.com/docs/sql-reference/data-types/float
    "float32": lambda v: validate_float(v, -3.4e38, 3.4e38),
    "float64": lambda v: validate_float(v, -1.7e308, 1.7e308),

    "string": lambda v: validate_string(v),

    # https://clickhouse.com/docs/sql-reference/data-types/boolean
    "bool": lambda v: validate_boolean_gp(v),

    "uuid": lambda v: validate_uuid(v),

    # Date:   [1970-01-01, 2149-06-06]
    # Date32: [1900-01-01, 2299-12-31]
    # https://clickhouse.com/docs/sql-reference/data-types/date
    "date": lambda v: validate_datetime(
        v, datetime(1970, 1, 1), datetime(2149, 6, 6), "%Y-%m-%d"
    ),
    "date32": lambda v: validate_datetime(
        v, datetime(1900, 1, 1), datetime(2299, 12, 31), "%Y-%m-%d"
    ),

    # DateTime:   [1970-01-01 00:00:00, 2106-02-07 06:28:15]
    # DateTime64: [1900-01-01, 2299-12-31], с суб-секундной точностью
    # https://clickhouse.com/docs/sql-reference/data-types/datetime
    "datetime": lambda v: validate_datetime(
        v, datetime(1970, 1, 1), datetime(2106, 2, 7, 6, 28, 15)
    ),
    "datetime64": lambda v: validate_datetime(
        v, datetime(1900, 1, 1), datetime(2299, 12, 31, 23, 59, 59)
    ),
}


# ── Публичный API ────────────────────────────────────────────────────────────

def get_validator(type_name: str, db_type: DatabaseType) -> Callable[[Any], CellResult]:
    """Возвращает функцию-валидатор для заданного типа и БД.

    Валидатор сохраняется один раз на колонку и вызывается для каждой строки —
    без повторного обращения к реестру.

    Raises:
        UnsupportedDataTypeError: тип не поддерживается для выбранной БД.
    """
    registry = _GP_VALIDATORS if db_type == DatabaseType.GREENPLUM else _CH_VALIDATORS
    type_lower = type_name.lower().strip()

    # 1. Точное совпадение
    if type_lower in registry:
        return registry[type_lower]

    # 2. decimal(P, S) / numeric(P, S)
    decimal_match = re.fullmatch(r"(?:decimal|numeric)\((\d+),\s*(\d+)\)", type_lower)
    if decimal_match:
        precision, scale = int(decimal_match.group(1)), int(decimal_match.group(2))
        return lambda v, p=precision, s=scale: validate_decimal(v, p, s)

    # 3. character varying(N) / varchar(N)
    varchar_match = re.fullmatch(r"(?:character varying|varchar)\((\d+)\)", type_lower)
    if varchar_match:
        size = int(varchar_match.group(1))
        return lambda v, n=size: (
            Ok(str(v)) if len(str(v)) <= n
            else Err(f"length {len(str(v))} exceeds maximum {n}")
        )

    # 4. char(N) / character(N) — фиксированная длина
    char_match = re.fullmatch(r"(?:char|character)\((\d+)\)", type_lower)
    if char_match:
        size = int(char_match.group(1))
        return lambda v, n=size: (
            Ok(str(v)) if len(str(v)) == n
            else Err(f"expected exactly {n} characters, got {len(str(v))}")
        )

    # 5. ClickHouse FixedString(N)
    if db_type == DatabaseType.CLICKHOUSE:
        fixed_match = re.fullmatch(r"fixedstring\((\d+)\)", type_lower)
        if fixed_match:
            size = int(fixed_match.group(1))
            return lambda v, n=size: (
                Ok(str(v)) if len(str(v).encode()) <= n
                else Err(f"byte length exceeds FixedString({n})")
            )

    # 6. ClickHouse DateTime64(precision) / DateTime64(precision, 'timezone')
    if db_type == DatabaseType.CLICKHOUSE:
        if re.fullmatch(r"datetime64\(\d+(?:,\s*'[^']*')?\)", type_lower):
            return lambda v: validate_datetime(
                v, datetime(1900, 1, 1), datetime(2299, 12, 31, 23, 59, 59)
            )

    raise UnsupportedDataTypeError(
        f"Type '{type_name}' is not supported for {db_type.value}. "
        f"Supported types: {sorted(registry.keys())}"
    )