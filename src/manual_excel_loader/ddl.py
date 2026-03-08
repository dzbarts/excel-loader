"""
DDL parser for GreenPlum and ClickHouse CREATE TABLE statements.

Public API
----------
parse_ddl(ddl, db_type) -> dict[str, str]
    Parse a CREATE TABLE statement and return a mapping of
    column name -> type string, preserving original type notation
    (e.g. "decimal(10,2)", "character varying(255)", "Nullable(String)").

The result is intended to be passed as LoaderConfig.dtypes so that
the validator can match columns by name rather than by position.
"""
from __future__ import annotations

import re
from typing import Callable

from .enums import DatabaseType
from .exceptions import ConfigurationError


# ── Internal: text cleanup ───────────────────────────────────────────────────

def _strip_comments(ddl: str) -> str:
    """Remove SQL line comments (--) and block comments (/* */)."""
    ddl = re.sub(r"/\*.*?\*/", " ", ddl, flags=re.DOTALL)
    ddl = re.sub(r"--[^\n]*", " ", ddl)
    return ddl


def _extract_columns_body(ddl: str) -> str:
    """
    Extract the text inside the outermost parentheses of CREATE TABLE.

    Uses character-by-character scanning so nested parens in type
    definitions (Decimal(10,2), Nullable(String)) don't confuse the parser.

    Raises ConfigurationError if no matching parentheses are found.
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
    """
    Split column definitions by top-level commas only.

    A comma inside Decimal(10, 2) or Array(Tuple(Int32, String))
    must NOT be treated as a column separator.
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
    """Strip quoting characters and lowercase."""
    return raw.strip().strip('"`[]').lower()


def _extract_type_token(rest: str) -> str:
    """
    Given text after the column name, extract the full type token
    (including nested parentheses like Decimal(10,2)) stopping at the
    first whitespace that is outside any parentheses.
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


# ── GreenPlum parser ─────────────────────────────────────────────────────────

# Lines that start with these keywords are not column definitions
_GP_SKIP = re.compile(
    r"^\s*(constraint|primary\s+key|foreign\s+key|unique|check)\b",
    re.IGNORECASE,
)

# Multi-word type prefixes for GP — order matters (longest first)
_GP_MULTIWORD_TYPES = (
    "timestamp without time zone",
    "timestamp with time zone",
    "time without time zone",
    "time with time zone",
    "double precision",
    "character varying",
    "character",  # must come after "character varying"
)

# Modifiers that terminate the type string
_GP_MODIFIERS = re.compile(
    r"\b(not\s+null|null|default|encoding|storage|collate|distributed|with\s+\()\b.*$",
    re.IGNORECASE,
)


def _parse_gp_column(col_def: str) -> tuple[str, str] | None:
    """
    Parse one GP column definition and return (name, type) or None if
    this is not a column line (e.g. a table constraint).
    """
    stripped = col_def.strip()
    if not stripped or _GP_SKIP.match(stripped):
        return None

    # Extract the column name: first token, possibly quoted
    name_match = re.match(r'^"?(?P<name>[\w]+)"?\s+', stripped)
    if not name_match:
        return None

    name = _normalize_col_name(name_match.group("name"))
    rest = stripped[name_match.end():]

    # Check for multi-word types first (longest match wins)
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

    # Strip trailing modifier keywords
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


# ── ClickHouse parser ────────────────────────────────────────────────────────

_CH_SKIP = re.compile(
    r"^\s*(index\b|projection\b|constraint\b)",
    re.IGNORECASE,
)

_CH_MODIFIERS = re.compile(
    r"\s+(default|alias|materialized|codec|ttl|comment)\b.*$",
    re.IGNORECASE,
)


def _unwrap_nullable(type_str: str) -> str:
    """Nullable(X) → X, so the validator sees the inner type."""
    m = re.fullmatch(r"[Nn]ullable\((.+)\)", type_str.strip())
    return m.group(1) if m else type_str


def _parse_ch_column(col_def: str) -> tuple[str, str] | None:
    """Parse one CH column definition and return (name, type) or None."""
    stripped = col_def.strip()
    if not stripped or _CH_SKIP.match(stripped):
        return None

    # Name: backtick-quoted or plain identifier
    name_match = re.match(r"^`?(?P<name>[\w]+)`?\s+", stripped)
    if not name_match:
        return None

    name = _normalize_col_name(name_match.group("name"))
    rest = stripped[name_match.end():]

    raw_type = _extract_type_token(rest)
    if not raw_type:
        return None

    # Strip modifiers
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


# ── Public API ───────────────────────────────────────────────────────────────

_PARSERS: dict[DatabaseType, Callable[[str], dict[str, str]]] = {
    DatabaseType.GREENPLUM: _parse_ddl_gp,
    DatabaseType.CLICKHOUSE: _parse_ddl_ch,
}


def parse_ddl(ddl: str, db_type: DatabaseType) -> dict[str, str]:
    """
    Parse a CREATE TABLE DDL string and return a column→type mapping.

    The returned dict maps **lowercase column names** to their type strings
    in the notation expected by ``get_validator()``.

    Example::

        ddl = '''
            CREATE TABLE hr.employees (
                id          integer         NOT NULL,
                full_name   text,
                salary      decimal(12, 2),
                hired_at    date
            ) DISTRIBUTED BY (id);
        '''
        types = parse_ddl(ddl, DatabaseType.GREENPLUM)
        # → {"id": "integer", "full_name": "text",
        #    "salary": "decimal(12,2)", "hired_at": "date"}

    The result can be passed directly as ``LoaderConfig.dtypes``.
    Columns present in DDL but absent in the Excel file are silently ignored.
    Columns present in Excel but absent in DDL are not validated.

    Args:
        ddl:     Full ``CREATE TABLE`` statement (comments allowed).
        db_type: Target database dialect.

    Returns:
        ``dict[str, str]`` — column name → type string.

    Raises:
        ConfigurationError: if the DDL cannot be parsed (empty, no
                            parentheses, unbalanced structure).
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