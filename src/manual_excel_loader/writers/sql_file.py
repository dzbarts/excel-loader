from __future__ import annotations

from typing import Iterable

from ..enums import DatabaseType
from .base import BaseWriter, FileWriterConfig


# ── Escape maps ───────────────────────────────────────────────────────────────
# Source: clickhouse-driver/clickhouse_driver/util/escape.py
_CH_ESCAPE: dict[str, str] = {
    "\b": "\\b", "\f": "\\f", "\r": "\\r", "\n": "\\n",
    "\t": "\\t", "\0": "\\0", "\a": "\\a", "\v": "\\v",
    "\\": "\\\\",
    "'":  "\\'",   # CH uses backslash-escape, NOT doubling
}


def _escape_gp(value: object) -> str:
    """
    Render a value as a GreenPlum SQL literal.

    GreenPlum follows standard SQL:
    - NULL stays NULL
    - booleans as TRUE/FALSE
    - numbers as bare literals
    - strings: single-quote delimited, inner ' doubled as ''
      Backslash is NOT a special character in standard_conforming_strings=on (default GP7+)
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):       # bool before int — bool is subclass of int
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _escape_ch(value: object) -> str:
    """
    Render a value as a ClickHouse SQL literal.

    ClickHouse escapes ' as \\' (backslash, not doubling) and also escapes
    \\b \\f \\r \\n \\t \\0 \\a \\v \\\\  — matching clickhouse-driver behaviour.
    Booleans are written as 1/0 (CH Bool accepts both 0/1 and true/false,
    but integers are the safest cross-version form).

    Source: clickhouse-driver/clickhouse_driver/util/escape.py
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = "".join(_CH_ESCAPE.get(c, c) for c in str(value))
    return "'" + escaped + "'"


# ── Internal helpers ──────────────────────────────────────────────────────────

def _format_insert(
    config: FileWriterConfig,
    escape_fn,
    headers: list[str],
    batch: list[tuple],
) -> str:
    """
    Format a single INSERT statement for *batch* rows.

    Example output (GP):
        INSERT INTO scheme.table (col1, col2)
            VALUES ('a', 1),
                   ('b', 2);
    """
    columns = ", ".join(headers)
    target = f"{config.scheme_name}.{config.table_name}"
    rows_sql = ",\n\t\t".join(
        "(" + ", ".join(escape_fn(v) for v in row) + ")"
        for row in batch
    )
    return f"INSERT INTO {target} ({columns})\n\tVALUES {rows_sql};\n"


# ── Writer ────────────────────────────────────────────────────────────────────

class SqlFileWriter(BaseWriter):
    """
    Writes validated rows to a SQL file as batched INSERT statements.

    The escape strategy is chosen automatically from config.db_type:
    - DatabaseType.GREENPLUM  -> standard SQL escaping ('' for single quotes)
    - DatabaseType.CLICKHOUSE -> CH escaping (\\' + control characters)
    """

    def __init__(self, config: FileWriterConfig) -> None:
        self._config = config
        self._escape = (
            _escape_ch if config.db_type == DatabaseType.CLICKHOUSE
            else _escape_gp
        )

    def write(self, headers: list[str], rows: Iterable[tuple]) -> None:
        batch: list[tuple] = []

        with open(self._config.output_path, "w", encoding=self._config.encoding) as fh:
            for row in rows:
                batch.append(row)
                if len(batch) >= self._config.batch_size:
                    fh.write(_format_insert(self._config, self._escape, headers, batch))
                    batch = []      # new list, not .clear() — avoids aliasing bugs

            if batch:
                fh.write(_format_insert(self._config, self._escape, headers, batch))