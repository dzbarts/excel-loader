from __future__ import annotations

import re
from datetime import datetime, date, time
from typing import Any, Callable
from dateutil import parser as dateutil_parser

from .enums import DatabaseType
from .exceptions import UnsupportedDataTypeError
from .result import Ok, Err, CellResult


# ── Primitive validators ──────────────────────────────────────────────────────
# Pure functions: accept a value, return Ok(converted) or Err(reason).
# Created once at module level — not inside methods or loops.


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
    if "," in str(value):
        return Err("use a dot instead of a comma as decimal separator")
    try:
        v = float(value)
    except (ValueError, TypeError):
        return Err("not a number")
    s = str(v)
    int_part, _, dec_part = s.partition(".")
    int_part = int_part.lstrip("-")
    total = len(int_part) + len(dec_part)
    if total > precision:
        return Err(f"total digits {total} exceed precision {precision}")
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
        if isinstance(value, datetime):
            pass  # already datetime, use as-is
        elif isinstance(value, date):
            value = datetime(value.year, value.month, value.day)
        elif isinstance(value, time):
            # time-only: wrap with a sentinel date for range check
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
    """Parse time value and return HH:MM:SS string. No date-range check."""
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
    # Full list per GP docs: https://docs.vmware.com/en/VMware-Greenplum/7/
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
    # GP interval is free-form text; basic pattern check only
    pattern = re.compile(
        r"^-?\d+\s+(?:year|month|day|hour|minute|second)s?"
        r"(?:\s+-?\d+\s+(?:year|month|day|hour|minute|second)s?)*$",
        re.IGNORECASE,
    )
    v = str(value).strip()
    if not pattern.fullmatch(v):
        return Err("does not match interval format, e.g. '1 year 2 months' or '3 days'")
    return Ok(v)


# ── Type → validator mapping ──────────────────────────────────────────────────
# Built once at import time, never recreated per-call.

_GP_VALIDATORS: dict[str, Callable[[Any], CellResult]] = {
    # Integer types
    # https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/ref_guide-data_types.html
    "smallint":   lambda v: validate_integer(v, -32768, 32767),
    "integer":    lambda v: validate_integer(v, -2_147_483_648, 2_147_483_647),
    "bigint":     lambda v: validate_integer(v, -9_223_372_036_854_775_808, 9_223_372_036_854_775_807),

    # Serial pseudo-types (auto-increment; normally not present in Excel uploads,
    # but accepted here for completeness)
    "smallserial": lambda v: validate_integer(v, 1, 32_767),
    "serial":      lambda v: validate_integer(v, 1, 2_147_483_647),
    "bigserial":   lambda v: validate_integer(v, 1, 9_223_372_036_854_775_807),

    # Floating-point
    "real":             lambda v: validate_float(v, -3.4e38, 3.4e38),
    "double precision": lambda v: validate_float(v, -1.7e308, 1.7e308),

    # String types (decimal/numeric/varchar/char handled via regex in get_validator)
    "text": lambda v: validate_string(v),

    # Date/time
    # GP date range: 4713 BC – 294276 AD; capped at Python datetime limits here
    "date": lambda v: validate_datetime(
        v, datetime(1, 1, 1), datetime(9999, 12, 31), "%Y-%m-%d"
    ),
    "time": validate_time,
    # GP timestamp range: 4713 BC – 294276 AD
    "timestamp": lambda v: validate_datetime(
        v, datetime(1, 1, 1), datetime(9999, 12, 31, 23, 59, 59)
    ),
    "timestamp without time zone": lambda v: validate_datetime(
        v, datetime(1, 1, 1), datetime(9999, 12, 31, 23, 59, 59)
    ),
    "timestamp with time zone": lambda v: validate_datetime(
        v, datetime(1, 1, 1), datetime(9999, 12, 31, 23, 59, 59)
    ),
    # interval: free-form text with pattern validation
    "interval": lambda v: validate_interval(v),

    # Other
    "boolean": lambda v: validate_boolean_gp(v),
    "uuid":    lambda v: validate_uuid(v),
    # tsrange: complex range type; validate as non-empty string for now
    "tsrange": lambda v: validate_string(v),
}

_CH_VALIDATORS: dict[str, Callable[[Any], CellResult]] = {
    # Signed integers
    # https://clickhouse.com/docs/sql-reference/data-types/int-uint
    "int8":   lambda v: validate_integer(v, -128, 127),
    "int16":  lambda v: validate_integer(v, -32_768, 32_767),
    "int32":  lambda v: validate_integer(v, -2_147_483_648, 2_147_483_647),
    "int64":  lambda v: validate_integer(v, -9_223_372_036_854_775_808, 9_223_372_036_854_775_807),
    "int128": lambda v: validate_integer(v, -(2**127), 2**127 - 1),
    "int256": lambda v: validate_integer(v, -(2**255), 2**255 - 1),

    # Unsigned integers
    "uint8":   lambda v: validate_integer(v, 0, 255),
    "uint16":  lambda v: validate_integer(v, 0, 65_535),
    "uint32":  lambda v: validate_integer(v, 0, 4_294_967_295),
    "uint64":  lambda v: validate_integer(v, 0, 18_446_744_073_709_551_615),
    "uint128": lambda v: validate_integer(v, 0, 2**128 - 1),
    "uint256": lambda v: validate_integer(v, 0, 2**256 - 1),

    # Floating-point
    # https://clickhouse.com/docs/sql-reference/data-types/float
    "float32": lambda v: validate_float(v, -3.4e38, 3.4e38),
    "float64": lambda v: validate_float(v, -1.7e308, 1.7e308),

    # String
    "string": lambda v: validate_string(v),

    # Boolean
    # https://clickhouse.com/docs/sql-reference/data-types/boolean
    "bool": lambda v: validate_boolean_gp(v),

    # UUID
    "uuid": lambda v: validate_uuid(v),

    # Date types — ranges per official CH docs:
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
    # DateTime64: [1900-01-01, 2299-12-31] with sub-second precision
    # https://clickhouse.com/docs/sql-reference/data-types/datetime
    "datetime": lambda v: validate_datetime(
        v, datetime(1970, 1, 1), datetime(2106, 2, 7, 6, 28, 15)
    ),
    "datetime64": lambda v: validate_datetime(
        v, datetime(1900, 1, 1), datetime(2299, 12, 31, 23, 59, 59)
    ),
}


# ── Public interface ──────────────────────────────────────────────────────────

def get_validator(type_name: str, db_type: DatabaseType) -> Callable[[Any], CellResult]:
    """
    Return the validator function for a given type name and target database.

    The returned callable has signature ``(value: Any) -> CellResult`` and can
    be stored once per column, then called for every row — avoiding repeated
    registry lookups.

    Raises:
        UnsupportedDataTypeError: if the type is not recognised for the given DB.
    """
    registry = _GP_VALIDATORS if db_type == DatabaseType.GREENPLUM else _CH_VALIDATORS
    type_lower = type_name.lower().strip()

    # 1. Exact match
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

    # 4. char(N) / character(N) — fixed length
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

    raise UnsupportedDataTypeError(
        f"Type '{type_name}' is not supported for {db_type.value}. "
        f"Supported types: {sorted(registry.keys())}"
    )