"""
type_mapping.py
===============
Таблица соответствия типов данных GreenPlum → ClickHouse.

Единственное место, где задаётся это соответствие внутри пакета.
При изменении внешнего SQL CASE-WHEN — редактировать только здесь.

Публичный API
-------------
GP_TO_CH       — dict[str, str]: таблица соответствия (ключи в lower-case).
gp_to_ch(t)    — перевести GP-тип в CH-тип с поддержкой параметризованных форм.
"""
from __future__ import annotations

import logging
import re

log = logging.getLogger(__name__)

# ── Таблица соответствия ─────────────────────────────────────────────────────
# Источник: внешний SQL-файл CASE WHEN lower(data_type) = ... THEN ...
# Ключи — нижний регистр (как lower() в SQL).
# Редактировать здесь при расхождении с внешним источником.

GP_TO_CH: dict[str, str] = {
    "date":                        "Date32",
    "timestamp":                   "DateTime",
    "real":                        "Float64",
    "double":                      "Float64",
    "decimal":                     "Decimal(32,8)",
    "numeric":                     "Decimal(32,8)",
    "smallint":                    "Int16",
    "integer":                     "Int32",
    "bigint":                      "Int64",
    "serial":                      "UInt32",
    "bigserial":                   "UInt64",
    "text":                        "String",
    "char":                        "String",
    "array":                       "Array(String)",    # CH требует тип элемента; String — умолчание
    "timestamp without time zone": "DateTime64(6)",
    "boolean":                     "Bool",
    "double precision":            "Float64",
    "character varying":           "String",
    "tsrange":                     "String",
    "uuid":                        "UUID",             # источник пишет 'uuid' — приводим к регистру CH
    "unknown":                     "String",
    # ── Дополнения: GP-алиасы, которых нет в SQL-источнике ──────────────────
    "float":                       "Float64",          # GP alias для double precision
    "float4":                      "Float32",
    "float8":                      "Float64",
    "int":                         "Int32",
    "int2":                        "Int16",
    "int4":                        "Int32",
    "int8":                        "Int64",
    "smallserial":                 "UInt16",
    "interval":                    "String",
    "time":                        "String",
    "time without time zone":      "String",
    "time with time zone":         "String",
    "timestamp with time zone":    "DateTime",
}

# ── Regex для параметризованных типов ────────────────────────────────────────

_DECIMAL_RE = re.compile(
    r'^(?:decimal|numeric)\(\s*(\d+)\s*,\s*(\d+)\s*\)$',
    re.IGNORECASE,
)
_VARCHAR_RE = re.compile(
    r'^(?:character varying|varchar|char|character)\s*\(\s*\d+\s*\)$',
    re.IGNORECASE,
)


# ── Публичный API ─────────────────────────────────────────────────────────────

def gp_to_ch(gp_type: str) -> str:
    """Перевести GP/PG-тип в эквивалентный CH-тип.

    Обрабатывает:
      • точные совпадения через GP_TO_CH (регистронезависимо)
      • decimal(P,S) / numeric(P,S) → Decimal(P,S) — precision/scale сохраняются
      • varchar(N) / char(N) / character varying(N) → String

    Неизвестные типы → String + warning в лог.
    """
    t = gp_type.strip().lower()

    # 1. Точное совпадение
    if t in GP_TO_CH:
        return GP_TO_CH[t]

    # 2. decimal(P,S) / numeric(P,S) — сохраняем precision и scale
    m = _DECIMAL_RE.match(t)
    if m:
        return f"Decimal({m.group(1)},{m.group(2)})"

    # 3. varchar(N) / char(N) — теряем ограничение длины (CH String неограничен)
    if _VARCHAR_RE.match(t):
        return "String"

    # 4. Неизвестный тип — String с предупреждением
    log.warning("Unknown GP type %r — mapped to String for ClickHouse", gp_type)
    return "String"
