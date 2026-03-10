"""
Парсер DDL CREATE TABLE для GreenPlum и ClickHouse.

parse_ddl(ddl, db_type) -> dict[str, str]
    Парсит CREATE TABLE и возвращает маппинг col_name → type_str
    в нотации, которую понимает get_validator() (например "decimal(10,2)").
    Результат передаётся в LoaderConfig.dtypes.
"""
from __future__ import annotations

import re
from typing import Callable

from .enums import DatabaseType
from .exceptions import ConfigurationError


# ── Внутренние хелперы ───────────────────────────────────────────────────────

def _strip_comments(ddl: str) -> str:
    ddl = re.sub(r"/\*.*?\*/", " ", ddl, flags=re.DOTALL)
    ddl = re.sub(r"--[^\n]*", " ", ddl)
    return ddl


def _extract_columns_body(ddl: str) -> str:
    """Вытаскивает содержимое внешних скобок CREATE TABLE.

    Идём посимвольно — чтобы вложенные скобки в типах вроде Decimal(10,2)
    не сломали парсер.
    """
    start = ddl.find("(")
    if start == -1:
        raise ConfigurationError("DDL has no parentheses — is it a valid CREATE TABLE?")

    depth = 0
    for i, ch in enumerate(ddl[start:], start):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return ddl[start + 1 : i]

    raise ConfigurationError("DDL has unbalanced parentheses.")


def _split_column_defs(body: str) -> list[str]:
    """Разбивает тело CREATE TABLE на отдельные определения колонок.

    Запятые внутри Decimal(10, 2) или Array(Tuple(...)) не считаются разделителями.
    """
    parts: list[str] = []
    current: list[str] = []
    depth = 0

    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
            continue
        current.append(ch)

    if current:
        parts.append("".join(current).strip())

    return [p for p in parts if p]


def _normalize_col_name(raw: str) -> str:
    return raw.strip().strip('"`[]').lower()


def _extract_type_token(rest: str) -> str:
    """Вытаскивает токен типа после имени колонки, включая вложенные скобки.

    Останавливается на первом пробеле вне скобок — то есть Decimal(10,2)
    читается целиком, а NOT NULL уже не захватывается.
    """
    type_chars: list[str] = []
    depth = 0
    started = False

    for ch in rest.lstrip():
        if ch == "(":
            depth += 1
            type_chars.append(ch)
            started = True
        elif ch == ")":
            depth -= 1
            type_chars.append(ch)
        elif ch in (" ", "\t") and depth == 0 and started:
            break
        else:
            type_chars.append(ch)
            started = True

    return "".join(type_chars)


# ── Парсер GreenPlum ─────────────────────────────────────────────────────────

# Строки с этих ключевых слов — не определения колонок
_GP_SKIP = re.compile(
    r"^\s*(constraint|primary\s+key|foreign\s+key|unique|check)\b",
    re.IGNORECASE,
)

# Составные типы GP — порядок важен, длинные раньше
_GP_MULTIWORD_TYPES = (
    "timestamp without time zone",
    "timestamp with time zone",
    "time without time zone",
    "time with time zone",
    "double precision",
    "character varying",
    "character",  # must come after "character varying"
)

# Модификаторы, которые обрывают тип
_GP_MODIFIERS = re.compile(
    r"\b(not\s+null|null|default|encoding|storage|collate|distributed|with\s+\()\b.*$",
    re.IGNORECASE,
)


def _parse_gp_column(col_def: str) -> tuple[str, str] | None:
    """Парсит одно определение колонки GP. Возвращает (name, type) или None для constraint-строк."""
    stripped = col_def.strip()
    if not stripped or _GP_SKIP.match(stripped):
        return None

    name_match = re.match(r'^"?(?P<name>[\w]+)"?\s+', stripped)
    if not name_match:
        return None

    name = _normalize_col_name(name_match.group("name"))
    rest = stripped[name_match.end():]

    # Составные типы проверяем первыми (longest match wins)
    matched_type: str | None = None
    for mw in _GP_MULTIWORD_TYPES:
        if rest.lower().startswith(mw):
            # Grab the multi-word base plus any parenthesised suffix
            after_mw = rest[len(mw):]
            paren_part = ""
            if after_mw.lstrip().startswith("("):
                paren_part = _extract_type_token(after_mw)
            matched_type = mw + paren_part
            break

    if matched_type is None:
        matched_type = _extract_type_token(rest)

    if not matched_type:
        return None

    clean = _GP_MODIFIERS.sub("", matched_type).strip().rstrip(",")
    clean = clean.strip().lower()

    return name, clean


def _parse_ddl_gp(ddl: str) -> dict[str, str]:
    body = _extract_columns_body(_strip_comments(ddl).replace("\n", " "))
    col_defs = _split_column_defs(body)

    result: dict[str, str] = {}
    for col_def in col_defs:
        parsed = _parse_gp_column(col_def)
        if parsed:
            name, type_str = parsed
            result[name] = type_str

    return result


# ── Парсер ClickHouse ────────────────────────────────────────────────────────

_CH_SKIP = re.compile(
    r"^\s*(index\b|projection\b|constraint\b)",
    re.IGNORECASE,
)

_CH_MODIFIERS = re.compile(
    r"\s+(default|alias|materialized|codec|ttl|comment)\b.*$",
    re.IGNORECASE,
)


def _unwrap_nullable(type_str: str) -> str:
    """Nullable(X) → X, валидатор работает с внутренним типом."""
    m = re.fullmatch(r"[Nn]ullable\((.+)\)", type_str.strip())
    return m.group(1) if m else type_str


def _parse_ch_column(col_def: str) -> tuple[str, str] | None:
    """Парсит одно определение колонки CH. Возвращает (name, type) или None."""
    stripped = col_def.strip()
    if not stripped or _CH_SKIP.match(stripped):
        return None

    name_match = re.match(r"^`?(?P<name>[\w]+)`?\s+", stripped)
    if not name_match:
        return None

    name = _normalize_col_name(name_match.group("name"))
    rest = stripped[name_match.end():]

    raw_type = _extract_type_token(rest)
    if not raw_type:
        return None

    clean = _CH_MODIFIERS.sub("", raw_type).strip()
    clean = _unwrap_nullable(clean)

    return name, clean


def _parse_ddl_ch(ddl: str) -> dict[str, str]:
    body = _extract_columns_body(_strip_comments(ddl).replace("\n", " "))
    col_defs = _split_column_defs(body)

    result: dict[str, str] = {}
    for col_def in col_defs:
        parsed = _parse_ch_column(col_def)
        if parsed:
            name, type_str = parsed
            result[name] = type_str

    return result


# ── Публичный API ────────────────────────────────────────────────────────────

_PARSERS: dict[DatabaseType, Callable[[str], dict[str, str]]] = {
    DatabaseType.GREENPLUM: _parse_ddl_gp,
    DatabaseType.CLICKHOUSE: _parse_ddl_ch,
}


def parse_ddl(ddl: str, db_type: DatabaseType) -> dict[str, str]:
    """Парсит CREATE TABLE и возвращает маппинг {col_name: type_str}.

    Имена колонок приводятся к нижнему регистру.
    Колонки из DDL, которых нет в Excel — игнорируются.
    Колонки из Excel, которых нет в DDL — проходят без валидации.

    Raises:
        ConfigurationError: DDL пустой, нет скобок или структура нарушена.
    """
    if not ddl or not ddl.strip():
        raise ConfigurationError("DDL string is empty.")

    parser = _PARSERS[db_type]
    result = parser(ddl)

    if not result:
        raise ConfigurationError(
            "DDL was parsed but no column definitions were found. "
            "Check that the input is a valid CREATE TABLE statement."
        )

    return result