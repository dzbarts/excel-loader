"""
DAG: excel_loader
=================
Параметризованный запуск загрузчика Excel/CSV/SQL → GP/PG/CH из Airflow UI.

Параметры
---------
validation   — источник типов для валидации данных:
    "bd"          (по умолчанию) — запрос типов колонок из целевой таблицы в БД;
                  если таблица не существует — инференс по данным файла.
    "ods_template" — типы берутся из листа klad_config (только GP + Excel-шаблон);
                  если лист не найден — предупреждение + инференс.
    "user_string" — пользователь вставляет DDL в поле ddl_string.
    "none"        — валидация пропускается (error_mode игнорируется).

export        — стратегия экспорта:
    "truncate_load" (по умолчанию) — очистить таблицу и загрузить.
                  GP/PG: TRUNCATE внутри транзакции (откат при ошибке).
                  CH: псевдооткат через временную таблицу *_temp.
    "append"      — добавить данные; создать таблицу если не существует.
    "via_backup"  — переименовать оригинал в *_before_YYMMDD_HHMM,
                  создать новую таблицу, загрузить; откатить при ошибке.
    "to_sql"      — создать SQL-файл (без загрузки в БД).
    "to_csv"      — создать CSV-файл (без загрузки в БД).

Подключения к БД (по умолчанию, переопределяются через db_conn_id):
    GreenPlum  → conn_updcc
    PostgreSQL → conn_updcc_pg
    ClickHouse → conn_updcc_ch
"""

from __future__ import annotations

import logging
import os
import sys
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any

_dags = next((p for p in sys.path if p.endswith("/dags")), None)
if _dags:
    sys.path.insert(0, os.path.join(_dags, "cf", "cf_excel"))

from airflow.decorators import dag, task
from airflow.models.param import Param

log = logging.getLogger(__name__)


_FILE_EXPORT_MODES = frozenset({"to_sql", "to_csv"})
_DB_EXPORT_MODES   = frozenset({"append", "truncate_load", "via_backup"})

# ── DAG-level defaults ────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "retries": 0,
    "email_on_failure": False,
}

DAG_PARAMS = {
    # ── 1. Источник ───────────────────────────────────────────────────────────
    "smb_host": Param(
        default="",
        type="string",
        description="[1. Источник] IP-адрес сервера для TCP-соединения (например: 10.50.73.65)",
    ),
    "remote_name": Param(
        default="",
        type="string",
        description="[1. Источник] NetBIOS-имя сервера (например: SPB99-FSN32)",
    ),
    "smb_share": Param(
        default="",
        type="string",
        description="[1. Источник] Имя шары (например: Disk5$)",
    ),
    "smb_dir": Param(
        default="",
        type=["string", "null"],
        description=(
            "[1. Источник] Путь к папке внутри шары (например: reports/2024). "
            "Слэши / и \\ воспринимаются одинаково. "
            "Оставьте пустым, если файл лежит в корне шары."
        ),
    ),
    "smb_file": Param(
        default="",
        type="string",
        description="[1. Источник] Имя файла (.xlsx, .xls, .xlsm, .csv, .tsv, .sql)",
    ),
    # ── 2. Чтение файла ───────────────────────────────────────────────────────
    "sheet_name": Param(
        default=None,
        type=["string", "null"],
        description="[2. Чтение] Имя листа Excel. Не указывайте для CSV/TSV/SQL или если нужен первый лист.",
    ),
    "skip_rows": Param(default=0, type="integer", description="[2. Чтение] Пропустить N строк сверху перед заголовком"),
    "skip_cols": Param(default=0, type="integer", description="[2. Чтение] Пропустить N колонок слева"),
    "max_row":   Param(default=None, type=["integer", "null"], description="[2. Чтение] Максимальное число строк для загрузки (без учёта заголовка)"),
    "delimiter": Param(default=",", type="string", description="[2. Чтение] Разделитель колонок (только для CSV/TSV)"),
    "encoding_input": Param(
        default="utf-8",
        type="string",
        description="[2. Чтение] Кодировка входящего файла (только для CSV/TSV/SQL; для Excel игнорируется).",
    ),
    "is_strip": Param(
        default=False,
        type="boolean",
        description="[2. Чтение] Обрезать пробелы у строковых значений",
    ),
    # ── 3. Целевая таблица ────────────────────────────────────────────────────
    "db_type": Param(
        default="greenplum",
        enum=["greenplum", "postgres", "clickhouse"],
        description="[3. Таблица] Целевая БД",
    ),
    "table_name":  Param(default="table_name",  type="string", description="[3. Таблица] Имя целевой таблицы"),
    "scheme_name": Param(default="scheme_name", type="string", description="[3. Таблица] Схема целевой таблицы"),
    # ── 4. Загрузка ───────────────────────────────────────────────────────────
    "export": Param(
        default="truncate_load",
        enum=["truncate_load", "append", "via_backup", "to_sql", "to_csv"],
        description=(
            "[4. Загрузка] "
            "truncate_load — очистить таблицу и загрузить (откат при ошибке); "
            "append — добавить строки; создать таблицу если её нет; "
            "via_backup — оригинал → *_before_YYMMDD, создать новую, загрузить; "
            "to_sql — создать SQL-файл без загрузки в БД; "
            "to_csv — создать CSV-файл без загрузки в БД."
        ),
    ),
    "batch_size": Param(default=10000, type="integer", description="[4. Загрузка] Размер батча при вставке в БД или записи в файл"),
    "timestamp": Param(
        default="none",
        type="string",
        enum=["none", "write_ts", "load_dttm"],
        description=(
            "[4. Загрузка] Добавить служебную колонку с временем загрузки: "
            "none — не добавлять; "
            "write_ts — TIMESTAMP WITHOUT TIME ZONE; "
            "load_dttm — DATE."
        ),
    ),
    "wf_load_idn": Param(
        default=None,
        type=["string", "null"],
        description="[4. Загрузка] Идентификатор потока загрузки (wf_load_idn) — добавляется как отдельная колонка",
    ),
    # ── 5. Валидация ──────────────────────────────────────────────────────────
    "validation": Param(
        default="bd",
        type="string",
        enum=["bd", "ods_template", "user_string", "none"],
        description=(
            "[5. Валидация] "
            "bd — типы из БД (целевая таблица); инференс если таблицы нет; "
            "ods_template — типы из листа klad_config (GP + Excel-шаблон); иначе инференс; "
            "user_string — типы из DDL-строки в поле ddl_string; "
            "none — без валидации."
        ),
    ),
    "ddl_string": Param(
        default="",
        type=["string", "null"],
        description="[5. Валидация] DDL CREATE TABLE — используется только при validation=user_string",
    ),
    "error_mode": Param(
        default="raise",
        enum=["raise", "coerce", "ignore", "verify"],
        description=(
            "[5. Валидация] "
            "raise — при ошибках валидации task завершается с ошибкой; "
            "coerce — ошибочные ячейки заменяются NULL, загрузка продолжается; "
            "ignore — загрузить без валидации (автоматически при validation=none); "
            "verify — только проверить данные, без записи."
        ),
    ),
    "save_validation_report": Param(
        default=False,
        type="boolean",
        description=(
            "[5. Валидация] Загружать TXT-отчёт об ошибках валидации на шару. "
            "При False ошибки фиксируются только в логах Airflow."
        ),
    ),
    "validation_report_include_values": Param(
        default=False,
        type="boolean",
        description=(
            "[5. Валидация] Включить примеры значений ячеек в TXT-отчёт. "
            "Внимание: отчёт может содержать чувствительные данные."
        ),
    ),
    # ── 6. Вывод (to_sql / to_csv) ────────────────────────────────────────────
    "output_smb_host": Param(
        default="",
        type=["string", "null"],
        description="[6. Вывод] Сервер для выходного файла. По умолчанию — тот же, что и smb_host.",
    ),
    "output_remote_name": Param(
        default="",
        type=["string", "null"],
        description="[6. Вывод] NetBIOS-имя сервера для выходного файла. По умолчанию — то же, что и remote_name.",
    ),
    "output_smb_share": Param(
        default="",
        type=["string", "null"],
        description="[6. Вывод] Шара для выходного файла. По умолчанию — та же, что и smb_share.",
    ),
    "output_smb_dir": Param(
        default="",
        type=["string", "null"],
        description="[6. Вывод] Папка внутри шары для выходного файла. По умолчанию — та же папка, где лежит исходный файл.",
    ),
    "encoding_output": Param(
        default="utf-8",
        type="string",
        description="[6. Вывод] Кодировка выходного SQL/CSV-файла.",
    ),
}

# ── Task functions ────────────────────────────────────────────────────────────


def _build_smb_path(host: str, share: str, smb_dir: str, smb_file: str = "") -> Path:
    """Собирает путь из SMB-компонентов.

    Windows: \\\\host\\share\\dir\\file
    Linux:   //host/share/dir/file  

    """
    sep = "\\" if os.name == "nt" else "/"
    clean_dir = smb_dir.replace("\\", sep).replace("/", sep).strip(sep) if smb_dir else ""
    parts = [f"{sep}{sep}{host}{sep}{share}"]
    if clean_dir:
        parts.append(clean_dir)
    if smb_file:
        parts.append(smb_file)
    return Path(sep.join(parts))


def _validate_params_fn(**context: Any) -> dict[str, Any]:
    """Проверка параметров, сборка пути к файлу из SMB-компонентов."""
    params = context["params"]

    smb_host    = (params.get("smb_host")    or "").strip()
    remote_name = (params.get("remote_name") or "").strip()
    smb_share   = (params.get("smb_share")   or "").strip()
    smb_file    = (params.get("smb_file")    or "").strip()
    smb_dir     = (params.get("smb_dir")     or "").strip()

    if not smb_host:
        raise ValueError("Параметр 'smb_host' обязателен.")
    if not remote_name:
        raise ValueError("Параметр 'remote_name' обязателен.")
    if not smb_share:
        raise ValueError("Параметр 'smb_share' обязателен.")
    if not smb_file:
        raise ValueError("Параметр 'smb_file' обязателен.")

    suffix = Path(smb_file).suffix.lower()
    allowed_ext = {".xlsx", ".xls", ".xlsm", ".csv", ".tsv", ".sql", ".txt"}
    if suffix not in allowed_ext:
        raise ValueError(
            f"Неподдерживаемое расширение файла: '{suffix}'. "
            f"Допустимо: {', '.join(sorted(allowed_ext))}"
        )

    export     = params.get("export", "truncate_load")
    validation = params.get("validation", "bd")

    if validation == "user_string" and not (params.get("ddl_string") or "").strip():
        raise ValueError(
            "Параметр 'ddl_string' обязателен при validation='user_string'."
        )

    if export in _DB_EXPORT_MODES and validation == "none":
        log.warning(
            "validation=none + export='%s': данные загрузятся без проверки типов.",
            export,
        )

    # Резолвим output SMB-параметры (по умолчанию — те же, что у входящего файла)
    out_host        = (params.get("output_smb_host")    or smb_host).strip()
    out_remote_name = (params.get("output_remote_name") or remote_name).strip()
    out_share       = (params.get("output_smb_share")   or smb_share).strip()
    out_dir         = (params.get("output_smb_dir")     or smb_dir).strip()

    # input_file — логический путь для именования выходного файла и логов.
    # Реальное чтение файла происходит через SMB в задачах resolve_dtypes и load_data.
    input_file = str(_build_smb_path(smb_host, smb_share, smb_dir, smb_file))

    log.info(
        "Файл: %s | db_type=%s | export=%s | validation=%s",
        input_file,
        params.get("db_type"),
        export,
        validation,
    )

    return {
        **dict(params),
        "input_file":        input_file,
        "output_smb_host":   out_host,
        "output_remote_name": out_remote_name,
        "output_smb_share":  out_share,
        "output_smb_dir":    out_dir,
    }


def _get_smb_stream(params: dict[str, Any]) -> bytes:
    """Скачать файл с SMB-шары в память."""
    from manual_excel_loader._connections import get_smb_file_bytes
    return get_smb_file_bytes(
        host=params["smb_host"],
        remote_name=params["remote_name"],
        share=params["smb_share"],
        smb_dir=params.get("smb_dir", ""),
        smb_file=params["smb_file"],
    )


def _resolve_dtypes_fn(run_params: dict[str, Any], **context: Any) -> dict[str, Any]:
    """Определяет словарь типов колонок согласно параметру validation.

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

    # Поток скачивается лениво — только если нужно читать файл.
    _stream: bytes | None = None

    def _get_stream() -> bytes:
        nonlocal _stream
        if _stream is None:
            _stream = _get_smb_stream(params)
        return _stream

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
            dtypes = _run_inference(input_path, params, db_type, stream=_get_stream())

    # ── ods_template: из klad_config ─────────────────────────────────────────
    elif validation == "ods_template":
        suffix = input_path.suffix.lower()
        if suffix in {".xlsx", ".xls", ".xlsm"}:
            try:
                from manual_excel_loader.template import read_template_config, is_template
                from manual_excel_loader.type_mapping import gp_to_ch
                s = _get_stream()
                if is_template(input_path, stream=s):
                    tmpl = read_template_config(input_path, stream=s)
                    dtypes = dict(tmpl.dtypes) if tmpl.dtypes else None
                    if not dtypes:
                        raise ValueError("klad_config не содержит типов")
                    # Типы в klad_config всегда в GP-нотации.
                    # Для не-GP баз переводим в нативные типы целевой БД.
                    if db_type not in (DatabaseType.GREENPLUM, DatabaseType.POSTGRES):
                        dtypes = {col: gp_to_ch(t) for col, t in dtypes.items()}
                        log.info(
                            "ods_template: типы переведены из GP-нотации для %s", db_type.value
                        )
                    log.info(
                        "ods_template: получены типы %d колонок из klad_config", len(dtypes)
                    )
                else:
                    raise ValueError("Файл не является шаблоном (нет листа klad_config)")
            except Exception as exc:
                warnings.warn(
                    f"ods_template: не удалось получить типы из klad_config ({exc}). "
                    "Переход к инференсу.",
                    stacklevel=1,
                )
                log.warning("ods_template fallback → inference: %s", exc)
                dtypes = _run_inference(input_path, params, db_type, stream=_get_stream())
        else:
            log.warning(
                "ods_template применим только к Excel-файлам. "
                "ext=%s → инференс.",
                input_path.suffix,
            )
            dtypes = _run_inference(input_path, params, db_type, stream=_get_stream())

    # ── user_string: парсим DDL ───────────────────────────────────────────────
    elif validation == "user_string":
        ddl_string = (params.get("ddl_string") or "").strip()
        dtypes = parse_ddl(ddl_string, db_type)
        log.info("user_string: распарсено %d колонок из DDL", len(dtypes))

    # ── none — без валидации ──────────────────────────────────────────────────
    else:
        dtypes = None
        log.info("validation=none: валидация пропущена")

    # ── Генерируем create_ddl если нужна загрузка в БД и таблицы нет ─────────
    if export in _DB_EXPORT_MODES and dtypes is not None:
        if not tbl_exists or export == "via_backup":
            from manual_excel_loader.ddl_generator import generate_ddl
            from manual_excel_loader.enums import TimestampField
            ts = params.get("timestamp")
            ts_field = TimestampField(ts) if ts and ts != "none" else None
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
    stream: bytes | None = None,
) -> dict[str, str]:
    """Вспомогательная функция: инференс типов из файла."""
    from manual_excel_loader.inferencer import infer_types
    from manual_excel_loader.readers import read_file

    sheet = read_file(
        input_path,
        stream=stream,
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
    """Загрузка в файл (to_sql / to_csv).

    SQL/CSV стримится напрямую в SMB через os.pipe(): writer пишет в текстовый
    write-end, SMB читает из бинарного read-end — локальный файл не создаётся.
    Отчёт валидации (небольшой TXT) пишется в temp-директорию и заливается отдельно.
    """
    import io
    import os
    import tempfile
    import threading
    from datetime import datetime as _dt
    params = run_params
    from dataclasses import replace as _replace

    from manual_excel_loader import load
    from manual_excel_loader._connections import write_smb_file_stream
    from manual_excel_loader.enums import DatabaseType, DumpType, ErrorMode, TimestampField
    from manual_excel_loader.exceptions import DataValidationError, FileReadError
    from manual_excel_loader.models import LoaderConfig

    export = params["export"]  # "to_sql" или "to_csv"
    dump_type = DumpType.SQL if export == "to_sql" else DumpType.CSV

    input_path      = Path(params["input_file"])
    out_host        = params["output_smb_host"]
    out_remote_name = params["output_remote_name"]
    out_share       = params["output_smb_share"]
    out_dir         = params["output_smb_dir"]
    encoding_out    = params.get("encoding_output", "utf-8")

    def _smb_path_str(filename: str) -> str:
        parts = [p for p in [out_dir, filename] if p]
        return f"//{out_host}/{out_share}/" + "/".join(parts)

    def _upload_bytes(filename: str, data: bytes) -> str:
        write_smb_file_stream(
            host=out_host, remote_name=out_remote_name,
            share=out_share, smb_dir=out_dir,
            smb_file=filename, file_obj=io.BytesIO(data),
        )
        return _smb_path_str(filename)

    # Базовый конфиг без stream — используется для pre-validation и как шаблон.
    cfg = LoaderConfig(
        input_file=input_path,
        input_stream=_get_smb_stream(params),
        db_type=DatabaseType(params["db_type"]),
        table_name=params["table_name"],
        scheme_name=params["scheme_name"],
        dump_type=dump_type,
        error_mode=ErrorMode(params.get("error_mode", "raise")),
        sheet_name=params.get("sheet_name"),
        skip_rows=int(params.get("skip_rows", 0)),
        skip_cols=int(params.get("skip_cols", 0)),
        batch_size=int(params.get("batch_size", 10000)),
        delimiter=params.get("delimiter", ","),
        encoding_input=params.get("encoding_input", "utf-8"),
        encoding_output=encoding_out,
        is_strip=bool(params.get("is_strip", False)),
        max_row=params.get("max_row"),
        wf_load_idn=params.get("wf_load_idn"),
        timestamp=(
            TimestampField(params["timestamp"]) if params.get("timestamp") != "none" else None
        ),
        dtypes=dtype_info.get("dtypes"),
        validation_report_include_values=bool(params.get("validation_report_include_values", False)),
    )

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        save_report = bool(params.get("save_validation_report", True))

        # ── Шаг 1: pre-validation (RAISE mode) — без записи, без потока ──────
        if cfg.error_mode == ErrorMode.RAISE:
            try:
                load(_replace(cfg, error_mode=ErrorMode.VERIFY, validation_report_dir=tmp_path))
            except DataValidationError as exc:
                if save_report:
                    for f in tmp_path.glob("*_validation_*.txt"):
                        try:
                            _upload_bytes(f.name, f.read_bytes())
                            log.info("Отчёт валидации загружен на шару: %s", f.name)
                        except Exception:
                            log.warning("Не удалось загрузить отчёт валидации на шару: %s", f.name)
                n = len(exc.validation_result.errors) if exc.validation_result else 0
                log.error("Ошибки валидации (%d ячеек): %s", n, exc)
                raise
            write_cfg = _replace(cfg, error_mode=ErrorMode.IGNORE, validation_report_dir=tmp_path)
        else:
            write_cfg = _replace(cfg, validation_report_dir=tmp_path)

        # ── Шаг 2: стриминг SQL/CSV напрямую в SMB через pipe ─────────────────
        ts = _dt.now().strftime("%d%m%y_%H%M%S")
        suffix = ".sql" if export == "to_sql" else ".csv"
        filename = f"{input_path.stem}_{ts}{suffix}"
        newline = "" if export == "to_csv" else None

        read_fd, write_fd = os.pipe()
        exc_holder: list[BaseException] = []
        result_holder: list = []

        def _do_load() -> None:
            try:
                with open(write_fd, "w", encoding=encoding_out, newline=newline) as wf:
                    result_holder.append(
                        load(_replace(write_cfg, output_stream=wf))
                    )
            except Exception as e:  # noqa: BLE001
                exc_holder.append(e)

        t = threading.Thread(target=_do_load, daemon=True)
        t.start()

        smb_exc: BaseException | None = None
        with open(read_fd, "rb") as rf:
            try:
                write_smb_file_stream(
                    host=out_host, remote_name=out_remote_name,
                    share=out_share, smb_dir=out_dir,
                    smb_file=filename, file_obj=rf,
                )
            except Exception as e:  # noqa: BLE001
                smb_exc = e

        t.join()

        # SMB-ошибка приоритетнее BrokenPipeError из потока writer'а
        if smb_exc:
            raise smb_exc
        if exc_holder:
            if isinstance(exc_holder[0], (FileReadError,)):
                log.error("Не удалось прочитать файл: %s", exc_holder[0])
            raise exc_holder[0]

        result = result_holder[0]
        output_smb = _smb_path_str(filename)
        log.info("Файл загружен на шару: %s", output_smb)

        error_smb: str | None = None
        if result.error_file and save_report:
            error_smb = _upload_bytes(result.error_file.name, result.error_file.read_bytes())
            log.info("Отчёт валидации загружен на шару: %s", error_smb)

    return {
        "output_file":  output_smb,
        "error_file":   error_smb,
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
    import tempfile
    params = run_params
    from dataclasses import replace as _replace

    from manual_excel_loader import load_rows
    from manual_excel_loader._connections import write_smb_file_stream
    from manual_excel_loader.enums import DatabaseType, ErrorMode, TimestampField
    from manual_excel_loader.exceptions import DataValidationError
    from manual_excel_loader.models import LoaderConfig
    from manual_excel_loader.table_manager import finalize, prepare
    from manual_excel_loader.validation_report import log_validation_result, write_report
    from manual_excel_loader.writers.db_writer import DbWriter, DbWriterConfig

    db_type    = DatabaseType(params["db_type"])
    export     = params["export"]
    scheme     = params["scheme_name"]
    table      = params["table_name"]
    dtypes     = dtype_info.get("dtypes")
    create_ddl = dtype_info.get("create_ddl")

    validation = params.get("validation", "bd")
    error_mode_str = "ignore" if validation == "none" else params.get("error_mode", "raise")
    error_mode = ErrorMode(error_mode_str)

    out_host        = params["output_smb_host"]
    out_remote_name = params["output_remote_name"]
    out_share       = params["output_smb_share"]
    out_dir         = params["output_smb_dir"]

    def _upload_report(local_path: Path) -> str:
        import io as _io
        write_smb_file_stream(
            host=out_host,
            remote_name=out_remote_name,
            share=out_share,
            smb_dir=out_dir,
            smb_file=local_path.name,
            file_obj=_io.BytesIO(local_path.read_bytes()),
        )
        parts = [p for p in [out_dir, local_path.name] if p]
        return f"//{out_host}/{out_share}/" + "/".join(parts)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        cfg = LoaderConfig(
            input_file=Path(params["input_file"]),
            input_stream=_get_smb_stream(params),
            db_type=db_type,
            table_name=table,
            scheme_name=scheme,
            error_mode=error_mode,
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
                TimestampField(params["timestamp"]) if params.get("timestamp") != "none" else None
            ),
            dtypes=dtypes,
            validation_report_dir=tmp_path,
            validation_report_include_values=bool(params.get("validation_report_include_values", False)),
        )

        # ── RAISE: валидация ДО подготовки таблицы и вставки ─────────────────────
        # Полный проход по данным без записи в БД. Если ошибки — таблица не трогается.
        save_report = bool(params.get("save_validation_report", True))

        if error_mode == ErrorMode.RAISE:
            _, rows_check, val_result = load_rows(_replace(cfg, error_mode=ErrorMode.VERIFY))
            for _ in rows_check:
                pass  # дренируем итератор чтобы накопить ошибки валидации
            log_validation_result(val_result, cfg.input_file, log)
            if not val_result.is_valid:
                if save_report:
                    report_file = write_report(
                        val_result,
                        cfg.input_file,
                        tmp_path,
                        include_sample_values=cfg.validation_report_include_values,
                    )
                    try:
                        _upload_report(report_file)
                        log.info("Отчёт валидации загружен на шару: %s", report_file.name)
                    except Exception:
                        log.warning("Не удалось загрузить отчёт валидации на шару: %s", report_file.name)
                raise DataValidationError(
                    f"Validation failed: {len(val_result.errors)} error(s).",
                    val_result,
                )
            # Валидация чистая — перезагружаем без валидации для вставки
            load_cfg = _replace(cfg, error_mode=ErrorMode.IGNORE)
            headers, rows_iter, validation_result = load_rows(load_cfg)
        else:
            headers, rows_iter, validation_result = load_rows(cfg)

    # ── Подготовка таблицы ────────────────────────────────────────────────────
    ctx = prepare(scheme, table, db_type, export, create_ddl)
    gp_conn   = ctx.get("conn")
    ch_client = ctx.get("client")

    # ── Вставка в БД ─────────────────────────────────────────────────────────
    rows_written = 0
    success = False
    try:
        writer = DbWriter(DbWriterConfig(
            db_type=db_type,
            scheme_name=scheme,
            table_name=table,
            batch_size=int(params.get("batch_size", 10000)),
            conn=gp_conn,
            client=ch_client,
        ))
        rows_written = writer.write(headers, rows_iter)
        success = True
    except Exception as exc:
        log.error("Ошибка при вставке данных: %s", exc)
        raise
    finally:
        finalize(ctx, success)

    # ── Проверка результатов валидации (для COERCE / VERIFY) ──────────────────
    has_errors = not validation_result.is_valid
    if has_errors and error_mode == ErrorMode.COERCE:
        log_validation_result(validation_result, cfg.input_file, log)

    return {
        "output_file":  None,
        "error_file":   None,
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
    log.info(" Выходной файл    : %s", result.get("output_file") or "—")
    log.info(" Строк записано   : %d", result.get("rows_written", 0))
    log.info(" Строк пропущено  : %d", result.get("rows_skipped", 0))
    if result.get("has_errors"):
        log.warning(" Ошибки валидации : %s", result.get("error_file") or "см. логи")


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
