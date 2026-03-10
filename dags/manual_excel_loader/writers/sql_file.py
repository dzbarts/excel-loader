from __future__ import annotations

from typing import Iterable

from ..enums import DatabaseType
from .base import BaseWriter, FileWriterConfig


# ── Таблицы экранирования ─────────────────────────────────────────────────────
# Источник CH-правил: clickhouse-driver/clickhouse_driver/util/escape.py
_CH_ESCAPE: dict[str, str] = {
    "\b": "\\b", "\f": "\\f", "\r": "\\r", "\n": "\\n",
    "\t": "\\t", "\0": "\\0", "\a": "\\a", "\v": "\\v",
    "\\": "\\\\",
    "'":  "\\'",   # CH экранирует через backslash, а не удвоение
}


def _escape_gp(value: object) -> str:
    """Рендерит значение как SQL-литерал для GreenPlum.

    GP следует стандартному SQL: строки в одинарных кавычках, внутренние '
    удваиваются. Бэкслэш — не спецсимвол при standard_conforming_strings=on (GP7+).
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):       # bool проверяем до int — bool наследует int
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _escape_ch(value: object) -> str:
    """Рендерит значение как SQL-литерал для ClickHouse.

    CH экранирует ' через \\' (бэкслэш, не удвоение), плюс управляющие символы.
    Boolean → 1/0: наиболее совместимая форма между версиями CH.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = "".join(_CH_ESCAPE.get(c, c) for c in str(value))
    return "'" + escaped + "'"


# ── Внутренние хелперы ────────────────────────────────────────────────────────

def _format_insert(
    config: FileWriterConfig,
    escape_fn,
    headers: list[str],
    batch: list[tuple],
) -> str:
    """Формирует один INSERT-стейтмент для батча строк.

    Пример вывода (GP):
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
    """Пишет строки в SQL-файл батчевыми INSERT-ами.

    Стратегия экранирования выбирается по config.db_type:
    - GREENPLUM  → стандартный SQL ('' для кавычек)
    - CLICKHOUSE → CH-экранирование (\\' + управляющие символы)
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
                    batch = []      # новый список, не .clear() — избегаем алиасинга

            if batch:
                fh.write(_format_insert(self._config, self._escape, headers, batch))
