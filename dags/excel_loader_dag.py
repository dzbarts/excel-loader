"""
DAG: excel_loader
=================
Параметризованный запуск загрузчика Excel/CSV/SQL → GP/CH из Airflow UI.

Параметры
---------
validation   — источник типов для валидации данных:
    "bd"          (по умолчанию) — запрос типов колонок из целевой таблицы в БД;
                  если таблица не существует — инференс по данным файла.
    "ods_template" — типы берутся из листа klad_config (только GP + Excel-шаблон);
                  если лист не найден — предупреждение + инференс.
    "user_string" — пользователь вставляет DDL в поле ddl_string.
    "no_validation" — валидация пропускается (error_mode игнорируется).

export        — стратегия экспорта:
    "truncate_load" (по умолчанию) — очистить таблицу и загрузить.
                  GP: TRUNCATE внутри транзакции (откат при ошибке).
                  CH: псевдооткат через временную таблицу *_temp.
    "append"      — добавить данные; создать таблицу если не существует.
    "via_backup"  — переименовать оригинал в *_before_YYMMDD_HHMM,
                  создать новую таблицу, загрузить; откатить при ошибке.
    "to_sql"      — создать SQL-файл (без загрузки в БД).
    "to_csv"      — создать CSV-файл (без загрузки в БД).

Подключения к БД (фиксированы по db_type):
    GreenPlum  → conn_updcc
    ClickHouse → conn_updcc_ch
"""

from __future__ import annotations

import logging
import warnings
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.email import send_email

log = logging.getLogger(__name__)

_DB_TYPE_ALIASES: dict[str, str] = {
    "gp": "greenplum",
    "ch": "clickhouse",
    "greenplum": "greenplum",
    "clickhouse": "clickhouse",
}

_FILE_EXPORT_MODES = frozenset({"to_sql", "to_csv"})
_DB_EXPORT_MODES   = frozenset({"append", "truncate_load", "via_backup"})

# ── DAG-level defaults ────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

DAG_PARAMS = {
    # ── Файл ─────────────────────────────────────────────────────────────────
    "input_file": Param(
        default="",
        type="string",
        description="Абсолютный путь к входящему файлу (.xlsx, .xls, .xlsm, .csv, .tsv, .sql)",
    ),
    # ── База данных ───────────────────────────────────────────────────────────
    "db_type": Param(
        default="gp",
        enum=["gp", "ch"],
        description="Целевая БД: 'gp' = GreenPlum, 'ch' = ClickHouse",
    ),
    "table_name": Param(default="table_name", type="string"),
    "scheme_name": Param(default="scheme_name", type="string"),
    # ── Стратегия экспорта ────────────────────────────────────────────────────
    "export": Param(
        default="truncate_load",
        enum=["truncate_load", "append", "via_backup", "to_sql", "to_csv"],
        description=(
            "truncate_load — очистить и загрузить (откат при ошибке);\n"
            "append        — добавить данные;\n"
            "via_backup    — оригинал → backup, создать новую, загрузить;\n"
            "to_sql        — только создать SQL-файл;\n"
            "to_csv        — только создать CSV-файл."
        ),
    ),
    # ── Только для to_sql / to_csv ────────────────────────────────────────────
    "output_dir": Param(
        default="",
        type=["string", "null"],
        description=(
            "Директория для выходного файла (только для to_sql / to_csv). "
            "По умолчанию — та же папка, откуда читается input_file."
        ),
    ),
    # ── Валидация ─────────────────────────────────────────────────────────────
    "validation": Param(
        default="bd",
        enum=["bd", "ods_template", "user_string", "no_validation"],
        description=(
            "bd            — типы из БД (целевая таблица); инференс если таблицы нет;\n"
            "ods_template  — типы из klad_config (GP + Excel-шаблон); иначе инференс;\n"
            "user_string   — типы из DDL-строки в поле ddl_string;\n"
            "no_validation — без валидации."
        ),
    ),
    "ddl_string": Param(
        default="",
        type=["string", "null"],
        description="DDL-строка CREATE TABLE (только для validation='user_string')",
    ),
    "error_mode": Param(
        default="raise",
        enum=["raise", "coerce", "ignore", "verify"],
        description=(
            "raise  — проверить; при ошибках task failed;\n"
            "coerce — заменить ошибки NULL, продолжить;\n"
            "ignore — выгрузить как есть (применяется при no_validation);\n"
            "verify — только проверить, без выгрузки."
        ),
    ),
    # ── Чтение файла ──────────────────────────────────────────────────────────
    "sheet_name": Param(default=None, type=["string", "null"]),
    "skip_rows":  Param(default=0, type="integer"),
    "skip_cols":  Param(default=0, type="integer"),
    "max_row":    Param(default=None, type=["integer", "null"]),
    "delimiter":  Param(default=",", type="string"),
    "encoding_input": Param(
        default="utf-8",
        type="string",
        description="Кодировка входящего файла (только для CSV/TSV/SQL).",
    ),
    "encoding_output": Param(
        default="utf-8",
        type="string",
        description="Кодировка выходного SQL/CSV-файла.",
    ),
    # ── Прочее ───────────────────────────────────────────────────────────────
    "batch_size":   Param(default=500, type="integer"),
    "timestamp":    Param(default=None, type=["string", "null"], enum=[None, "write_ts", "load_dttm"]),
    "wf_load_idn":  Param(default=None, type=["string", "null"]),
    "is_strip":     Param(default=False, type="boolean"),
    "notify_email": Param(
        default="",
        type=["string", "null"],
        description="E-mail для уведомления об ошибках (опционально)",
    ),
}

# ── Task functions ────────────────────────────────────────────────────────────


def _validate_params_fn(**context: Any) -> dict[str, Any]:
    """Проверка параметров, нормализация db_type."""
    params = context["params"]

    input_file = params.get("input_file", "")
    if not input_file:
        raise ValueError("Параметр 'input_file' обязателен и не может быть пустым.")

    path = Path(input_file)
    if not path.exists():
        raise FileNotFoundError(f"Файл не найден: {path}")

    allowed_ext = {".xlsx", ".xls", ".xlsm", ".csv", ".tsv", ".sql", ".txt"}
    if path.suffix.lower() not in allowed_ext:
        raise ValueError(
            f"Неподдерживаемое расширение файла: {path.suffix}. "
            f"Допустимо: {', '.join(sorted(allowed_ext))}"
        )

    export = params.get("export", "truncate_load")
    validation = params.get("validation", "bd")

    if validation == "user_string" and not (params.get("ddl_string") or "").strip():
        raise ValueError(
            "Параметр 'ddl_string' обязателен при validation='user_string'."
        )

    if export in _DB_EXPORT_MODES and validation == "no_validation":
        log.warning(
            "validation='no_validation' + export='%s': данные загрузятся без проверки типов.",
            export,
        )

    log.info(
        "Файл: %s (%.1f КБ) | db_type=%s | export=%s | validation=%s",
        path,
        path.stat().st_size / 1024,
        params.get("db_type"),
        export,
        validation,
    )

    result = dict(params)
    result["db_type"] = _DB_TYPE_ALIASES.get(params.get("db_type", ""), params.get("db_type", ""))
    return result


def _resolve_dtypes_fn(run_params: dict[str, Any], **context: Any) -> dict[str, Any]:
    """Определить словарь типов колонок согласно параметру validation.

    Возвращает:
        dtypes        — dict {col_name: type_str} или None.
        create_ddl    — DDL для создания таблицы если нужно, или None.
        table_exists  — True/False (для DB-режимов).
    """
    from manual_excel_loader.enums import DatabaseType
    from manual_excel_loader.ddl import parse_ddl

    params     = run_params
    db_type    = DatabaseType(params["db_type"])
    validation = params.get("validation", "bd")
    export     = params.get("export", "truncate_load")
    scheme     = params["scheme_name"]
    table      = params["table_name"]
    input_path = Path(params["input_file"])

    dtypes: dict[str, str] | None = None
    create_ddl: str | None = None
    tbl_exists = False

    # ── bd: запрос из БД ─────────────────────────────────────────────────────
    if validation == "bd":
        from manual_excel_loader.db_schema import get_table_columns
        dtypes = get_table_columns(scheme, table, db_type)
        if dtypes is not None:
            tbl_exists = True
            log.info("BD: получены типы %d колонок из %s.%s", len(dtypes), scheme, table)
        else:
            log.info(
                "BD: таблица %s.%s не найдена → будет инференс + создание", scheme, table
            )
            dtypes = _run_inference(input_path, params, db_type)

    # ── ods_template: из klad_config ─────────────────────────────────────────
    elif validation == "ods_template":
        suffix = input_path.suffix.lower()
        if db_type == DatabaseType.GREENPLUM and suffix in {".xlsx", ".xls", ".xlsm"}:
            try:
                from manual_excel_loader.template import read_template_config, is_template
                if is_template(input_path):
                    tmpl = read_template_config(input_path)
                    dtypes = dict(tmpl.dtypes) if tmpl.dtypes else None
                    if dtypes:
                        log.info(
                            "ods_template: получены типы %d колонок из klad_config", len(dtypes)
                        )
                    else:
                        raise ValueError("klad_config не содержит типов")
                else:
                    raise ValueError("Файл не является шаблоном (нет листа klad_config)")
            except Exception as exc:
                warnings.warn(
                    f"ods_template: не удалось получить типы из klad_config ({exc}). "
                    "Переход к инференсу.",
                    stacklevel=1,
                )
                log.warning("ods_template fallback → inference: %s", exc)
                dtypes = _run_inference(input_path, params, db_type)
        else:
            log.warning(
                "ods_template применим только к GP + Excel. "
                "db_type=%s, ext=%s → инференс.",
                db_type.value,
                input_path.suffix,
            )
            dtypes = _run_inference(input_path, params, db_type)

    # ── user_string: парсим DDL ───────────────────────────────────────────────
    elif validation == "user_string":
        ddl_string = (params.get("ddl_string") or "").strip()
        dtypes = parse_ddl(ddl_string, db_type)
        log.info("user_string: распарсено %d колонок из DDL", len(dtypes))

    # ── no_validation ─────────────────────────────────────────────────────────
    else:
        dtypes = None
        log.info("no_validation: валидация пропущена")

    # ── Генерируем create_ddl если нужна загрузка в БД и таблицы нет ─────────
    if export in _DB_EXPORT_MODES and dtypes is not None:
        if not tbl_exists or export == "via_backup":
            from manual_excel_loader.ddl_generator import generate_ddl
            from manual_excel_loader.enums import TimestampField
            ts = params.get("timestamp")
            ts_field = TimestampField(ts) if ts else None
            create_ddl = generate_ddl(dtypes, scheme, table, db_type, ts_field)
            log.info("DDL сгенерирован для %s.%s", scheme, table)

    return {
        "dtypes":       dtypes,
        "create_ddl":   create_ddl,
        "table_exists": tbl_exists,
    }


def _run_inference(
    input_path: Path,
    params: dict[str, Any],
    db_type,
) -> dict[str, str]:
    """Вспомогательная функция: инференс типов из файла."""
    from manual_excel_loader.inferencer import infer_types
    from manual_excel_loader.readers import read_file

    sheet = read_file(
        input_path,
        sheet_name=params.get("sheet_name"),
        skip_rows=int(params.get("skip_rows", 0)),
        skip_cols=int(params.get("skip_cols", 0)),
        encoding=params.get("encoding_input", "utf-8"),
        delimiter=params.get("delimiter", ","),
    )
    dtypes = infer_types(sheet, db_type)
    log.info("inference: определены типы %d колонок", len(dtypes))
    return dtypes


def _load_file_fn(
    run_params: dict[str, Any],
    dtype_info: dict[str, Any],
    **context: Any,
) -> dict[str, Any]:
    """Загрузка в файл (to_sql / to_csv) — существующее поведение."""
    params = run_params
    from manual_excel_loader import load
    from manual_excel_loader.enums import DatabaseType, DumpType, ErrorMode, TimestampField
    from manual_excel_loader.exceptions import DataValidationError, FileReadError
    from manual_excel_loader.models import LoaderConfig

    export = params["export"]  # "to_sql" или "to_csv"
    dump_type = DumpType.SQL if export == "to_sql" else DumpType.CSV

    input_path = Path(params["input_file"])
    output_dir = (params.get("output_dir") or "").strip()

    cfg = LoaderConfig(
        input_file=input_path,
        db_type=DatabaseType(params["db_type"]),
        table_name=params["table_name"],
        scheme_name=params["scheme_name"],
        dump_type=dump_type,
        error_mode=ErrorMode(params.get("error_mode", "raise")),
        sheet_name=params.get("sheet_name"),
        skip_rows=int(params.get("skip_rows", 0)),
        skip_cols=int(params.get("skip_cols", 0)),
        batch_size=int(params.get("batch_size", 500)),
        delimiter=params.get("delimiter", ","),
        encoding_input=params.get("encoding_input", "utf-8"),
        encoding_output=params.get("encoding_output", "utf-8"),
        is_strip=bool(params.get("is_strip", False)),
        max_row=params.get("max_row"),
        wf_load_idn=params.get("wf_load_idn"),
        timestamp=(
            TimestampField(params["timestamp"]) if params.get("timestamp") else None
        ),
        dtypes=dtype_info.get("dtypes"),
    )

    try:
        result = load(cfg)
    except DataValidationError as exc:
        n = len(exc.validation_result.errors) if exc.validation_result else 0
        log.error("Ошибки валидации (%d ячеек): %s", n, exc)
        _maybe_notify(params, "[excel_loader] Ошибки валидации", str(exc))
        raise
    except FileReadError as exc:
        log.error("Не удалось прочитать файл: %s", exc)
        _maybe_notify(params, "[excel_loader] Ошибка чтения файла", str(exc))
        raise
    except Exception as exc:
        log.exception("Непредвиденная ошибка: %s", exc)
        _maybe_notify(params, "[excel_loader] Непредвиденная ошибка", str(exc))
        raise

    # Переместить выходной файл в output_dir если задан
    final_output = result.output_file
    if final_output and output_dir:
        dest = Path(output_dir) / final_output.name
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        final_output.rename(dest)
        final_output = dest
        log.info("Файл перемещён в %s", dest)

    return {
        "output_file":  str(final_output) if final_output else None,
        "rows_written": result.rows_written,
        "rows_skipped": result.rows_skipped,
        "has_errors":   result.has_errors,
    }


def _load_db_fn(
    run_params: dict[str, Any],
    dtype_info: dict[str, Any],
    **context: Any,
) -> dict[str, Any]:
    """Загрузка в БД (append / truncate_load / via_backup)."""
    params = run_params
    from manual_excel_loader import load_rows
    from manual_excel_loader.db_loader import load_to_db
    from manual_excel_loader.enums import DatabaseType, ErrorMode, TimestampField
    from manual_excel_loader.exceptions import DataValidationError
    from manual_excel_loader.models import LoaderConfig
    from manual_excel_loader.table_manager import finalize, prepare

    db_type    = DatabaseType(params["db_type"])
    export     = params["export"]
    scheme     = params["scheme_name"]
    table      = params["table_name"]
    dtypes     = dtype_info.get("dtypes")
    create_ddl = dtype_info.get("create_ddl")

    # ── Конфиг загрузчика ─────────────────────────────────────────────────────
    validation = params.get("validation", "bd")
    error_mode_str = "ignore" if validation == "no_validation" else params.get("error_mode", "raise")

    cfg = LoaderConfig(
        input_file=Path(params["input_file"]),
        db_type=db_type,
        table_name=table,
        scheme_name=scheme,
        error_mode=ErrorMode(error_mode_str),
        sheet_name=params.get("sheet_name"),
        skip_rows=int(params.get("skip_rows", 0)),
        skip_cols=int(params.get("skip_cols", 0)),
        batch_size=int(params.get("batch_size", 500)),
        delimiter=params.get("delimiter", ","),
        encoding_input=params.get("encoding_input", "utf-8"),
        encoding_output=params.get("encoding_output", "utf-8"),
        is_strip=bool(params.get("is_strip", False)),
        max_row=params.get("max_row"),
        wf_load_idn=params.get("wf_load_idn"),
        timestamp=(
            TimestampField(params["timestamp"]) if params.get("timestamp") else None
        ),
        dtypes=dtypes,
    )

    # ── Подготовка таблицы ────────────────────────────────────────────────────
    ctx = prepare(scheme, table, db_type, export, create_ddl)
    gp_conn   = ctx.get("conn")
    ch_client = ctx.get("client")

    # ── Чтение + валидация ────────────────────────────────────────────────────
    headers, rows_iter, validation_result = load_rows(cfg)

    # ── Вставка в БД ─────────────────────────────────────────────────────────
    rows_written = 0
    success = False
    try:
        rows_written = load_to_db(
            headers=headers,
            rows=rows_iter,
            scheme=scheme,
            table=table,
            db_type=db_type,
            batch_size=int(params.get("batch_size", 500)),
            gp_conn=gp_conn,
            ch_client=ch_client,
        )
        success = True
    except Exception as exc:
        log.error("Ошибка при вставке данных: %s", exc)
        _maybe_notify(params, "[excel_loader] Ошибка загрузки в БД", str(exc))
        raise
    finally:
        finalize(ctx, success)

    # ── Проверка результатов валидации ────────────────────────────────────────
    has_errors = not validation_result.is_valid
    if has_errors and ErrorMode(error_mode_str) == ErrorMode.RAISE:
        n = len(validation_result.errors)
        raise DataValidationError(
            f"Validation failed: {n} error(s).",
            validation_result,
        )

    return {
        "output_file":  None,
        "rows_written": rows_written,
        "rows_skipped": 0,
        "has_errors":   has_errors,
    }


def _load_data_fn(
    run_params: dict[str, Any],
    dtype_info: dict[str, Any],
    **context: Any,
) -> dict[str, Any]:
    """Роутер: файловый экспорт или загрузка в БД — в зависимости от export."""
    export = run_params.get("export", "truncate_load")
    if export in _FILE_EXPORT_MODES:
        return _load_file_fn(run_params, dtype_info, **context)
    return _load_db_fn(run_params, dtype_info, **context)


def _report_fn(result: dict[str, Any], **context: Any) -> None:
    log.info("=== Excel Loader — итоговый отчёт ===")
    log.info(" Выходной файл : %s", result.get("output_file") or "—")
    log.info(" Строк записано: %d", result.get("rows_written", 0))
    log.info(" Строк пропущено: %d", result.get("rows_skipped", 0))
    if result.get("has_errors"):
        log.warning(" ⚠ В данных обнаружены ошибки валидации.")


def _maybe_notify(params: dict[str, Any], subject: str, body: str) -> None:
    email = (params.get("notify_email") or "").strip()
    if email:
        try:
            send_email(to=email, subject=subject, html_content=f"<pre>{body}</pre>")
        except Exception as exc:
            log.warning("Не удалось отправить уведомление: %s", exc)


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="excel_loader",
    description="Загрузка Excel/CSV/SQL → GP/CH с валидацией данных",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    params=DAG_PARAMS,
    tags=["excel", "loader", "manual"],
    doc_md=__doc__,
)
def excel_loader_dag() -> None:
    validate_params = task(task_id="validate_params")(_validate_params_fn)
    resolve_dtypes  = task(task_id="resolve_dtypes")(_resolve_dtypes_fn)
    load_data       = task(task_id="load_data")(_load_data_fn)
    report          = task(task_id="report")(_report_fn)

    validated  = validate_params()
    dtype_info = resolve_dtypes(validated)
    result     = load_data(validated, dtype_info)
    report(result)


excel_loader_dag()
