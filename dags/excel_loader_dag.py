"""
DAG: excel_loader
=================
Параметризованный запуск загрузчика Excel → GP/CH из Airflow UI.

Запуск через UI: Trigger DAG w/ config → вставляем JSON с параметрами.
Пример конфига:
{
    "input_file": "/data/uploads/my_file.xlsx",
    "db_type": "gp",
    "table_name": "my_table",
    "scheme_name": "my_schema",
    "dump_type": "sql",
    "error_mode": "raise",
    "dtypes_ddl": "CREATE TABLE t (id integer, name text, dt date)",
    "timestamp": "load_dttm",
    "batch_size": 500,
    "skip_rows": 0,
    "skip_cols": 0
}
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.email import send_email

log = logging.getLogger(__name__)

# ── DAG-level defaults ────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,   # управляем вручную внутри задачи
}

DAG_PARAMS = {
    "input_file": Param(
        default="",
        type="string",
        description="Абсолютный путь к входящему Excel-файлу",
    ),
    "db_type": Param(
        default="gp",
        enum=["gp", "ch"],
        description="Целевая БД: 'gp' = GreenPlum, 'ch' = ClickHouse",
    ),
    "table_name": Param(default="table_name", type="string"),
    "scheme_name": Param(default="scheme_name", type="string"),
    "dump_type": Param(default="sql", enum=["sql", "csv"]),
    "error_mode": Param(
        default="raise",
        enum=["raise", "coerce", "ignore", "verify"],
        description=(
            "raise  – проверить и сохранить только валидные строки;\n"
            "coerce – заменить ошибки NULL;\n"
            "ignore – выгрузить как есть;\n"
            "verify – только проверить, без выгрузки."
        ),
    ),
    "dtypes_ddl": Param(
        default="",
        type=["string", "null"],
        description="DDL-строка или список типов через запятую (опционально)",
    ),
    "sheet_name": Param(default=None, type=["string", "null"]),
    "skip_rows": Param(default=0, type="integer"),
    "skip_cols": Param(default=0, type="integer"),
    "batch_size": Param(default=500, type="integer"),
    "timestamp": Param(
        default=None,
        type=["string", "null"],
        enum=[None, "write_ts", "load_dttm"],
    ),
    "wf_load_idn": Param(default=None, type=["string", "null"]),
    "max_row": Param(default=None, type=["integer", "null"]),
    "delimiter": Param(default=",", type="string"),
    "is_strip": Param(default=False, type="boolean"),
    "notify_email": Param(
        default="",
        type=["string", "null"],
        description="E-mail для уведомления об ошибках (опционально)",
    ),
}


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="excel_loader",
    description="Загрузка Excel → SQL/CSV с валидацией данных",
    schedule=None,           # только ручной запуск
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    params=DAG_PARAMS,
    tags=["excel", "loader", "manual"],
    doc_md=__doc__,
)
def excel_loader_dag() -> None:

    @task(task_id="validate_params")
    def validate_params(**context: Any) -> dict[str, Any]:
        """Базовая проверка параметров до запуска тяжёлой логики."""
        params = context["params"]

        input_file = params.get("input_file", "")
        if not input_file:
            raise ValueError("Параметр 'input_file' обязателен и не может быть пустым.")

        path = Path(input_file)
        if not path.exists():
            raise FileNotFoundError(f"Файл не найден: {path}")
        if path.suffix.lower() not in (".xlsx", ".xls", ".xlsm"):
            raise ValueError(f"Неподдерживаемое расширение файла: {path.suffix}")

        log.info("Файл найден: %s (размер %.1f КБ)", path, path.stat().st_size / 1024)
        return dict(params)

    @task(task_id="load_excel")
    def load_excel(params: dict[str, Any], **context: Any) -> dict[str, Any]:
        """
        Основная задача загрузки.

        Возвращает сериализуемый LoadResult-словарь, который пушится в XCom
        автоматически через return.
        """
        # Импорт здесь, чтобы не поднимать зависимости при парсинге DAG-файла
        from manual_excel_loader import load
        from manual_excel_loader.enums import DatabaseType, DumpType, ErrorMode, TimestampField
        from manual_excel_loader.exceptions import DataValidationError, FileReadError
        from manual_excel_loader.models import LoaderConfig

        cfg = LoaderConfig(
            input_file=Path(params["input_file"]),
            db_type=DatabaseType(params["db_type"]),
            table_name=params["table_name"],
            scheme_name=params["scheme_name"],
            dump_type=DumpType(params["dump_type"]),
            error_mode=ErrorMode(params["error_mode"]),
            sheet_name=params.get("sheet_name"),
            skip_rows=int(params.get("skip_rows", 0)),
            skip_cols=int(params.get("skip_cols", 0)),
            batch_size=int(params.get("batch_size", 500)),
            delimiter=params.get("delimiter", ","),
            is_strip=bool(params.get("is_strip", False)),
            max_row=params.get("max_row"),
            wf_load_idn=params.get("wf_load_idn"),
            timestamp=(
                TimestampField(params["timestamp"])
                if params.get("timestamp")
                else None
            ),
            dtypes=params.get("dtypes_ddl") or None,
        )

        try:
            result = load(cfg)
        except DataValidationError as exc:
            # Ошибки данных: логируем детали, кидаем дальше → Airflow помечает
            # задачу failed и может сделать retry или отправить алерт.
            log.error(
                "Ошибки валидации данных (%d ячеек): %s",
                len(exc.errors),
                exc,
            )
            _maybe_notify(params, subject="[excel_loader] Ошибки валидации", body=str(exc))
            raise   # re-raise → task failed

        except FileReadError as exc:
            log.error("Не удалось прочитать файл: %s", exc)
            _maybe_notify(params, subject="[excel_loader] Ошибка чтения файла", body=str(exc))
            raise

        except Exception as exc:
            log.exception("Непредвиденная ошибка: %s", exc)
            _maybe_notify(params, subject="[excel_loader] Непредвиденная ошибка", body=str(exc))
            raise

        # LoadResult → dict для XCom (должен быть JSON-сериализуем)
        result_dict = {
            "output_file": str(result.output_file) if result.output_file else None,
            "error_file": str(result.error_file) if result.error_file else None,
            "rows_written": result.rows_written,
            "rows_skipped": result.rows_skipped,
            "has_errors": result.has_errors,
        }

        log.info(
            "Загрузка завершена. Строк записано: %d, пропущено: %d. Файл: %s",
            result.rows_written,
            result.rows_skipped,
            result.output_file,
        )

        # XCom push — автоматически через return
        return result_dict

    @task(task_id="report")
    def report(result: dict[str, Any], **context: Any) -> None:
        """Финальное логирование результата."""
        log.info("=== Excel Loader — итоговый отчёт ===")
        log.info("  Выходной файл : %s", result.get("output_file"))
        log.info("  Файл ошибок   : %s", result.get("error_file"))
        log.info("  Строк записано: %d", result.get("rows_written", 0))
        log.info("  Строк пропущено: %d", result.get("rows_skipped", 0))
        if result.get("has_errors"):
            log.warning("  ⚠ В данных обнаружены ошибки, см. файл ошибок.")

    # ── Связи между задачами ──────────────────────────────────────────────────
    validated = validate_params()
    loaded = load_excel(validated)
    report(loaded)


def _maybe_notify(params: dict[str, Any], subject: str, body: str) -> None:
    """Отправляет email, если задан notify_email."""
    email = (params.get("notify_email") or "").strip()
    if email:
        try:
            send_email(to=email, subject=subject, html_content=f"<pre>{body}</pre>")
        except Exception as exc:
            log.warning("Не удалось отправить уведомление: %s", exc)


# Регистрация DAG
excel_loader_dag()