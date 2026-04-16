"""
loader.py
=========
Главная точка входа пакета. Собирает pipeline:
    read_file() → (validate) → writer.write()

Loader не знает о деталях форматов — этим занимаются ридеры.
Loader не знает о деталях записи — этим занимаются врайтеры.
Его зона ответственности: конфигурация, валидация строк, склейка.

Публичный API:
    load(config)       — полный pipeline: чтение → валидация → запись файла.
    load_rows(config)  — только чтение и валидация; возвращает итератор строк
                         для прямой загрузки в БД через DbWriter.
"""
from __future__ import annotations

import dataclasses
import logging
from datetime import datetime
from pathlib import Path
from typing import IO, Any, Callable, Iterator

from .enums import DatabaseType, DumpType, ErrorMode
from .exceptions import ConfigurationError, DataValidationError
from .models import (
    CellValidationError,
    FileValidationResult,
    LoaderConfig,
    LoadResult,
)
from .readers import SheetData, read_file
from .result import CellResult, Ok
from .readers.excel_reader import ExcelReadConfig, read_excel
from .template import TemplateConfig, is_template, read_template_config
from .validation_report import log_validation_result, write_report
from .writers.base import FileWriterConfig
from .writers.csv_file import CsvFileWriter
from .writers.sql_file import SqlFileWriter

logger = logging.getLogger(__name__)

_EXCEL_SUFFIXES = frozenset({".xlsx", ".xls", ".xlsm"})


# ── Reader resolution ────────────────────────────────────────────────────────

def _resolve_reader(
    config: LoaderConfig,
) -> tuple[SheetData, LoaderConfig, TemplateConfig | None]:
    """Определить формат → вернуть (sheet_data, effective_config, template_config).

    Для обычного Excel и текстовых форматов: template_config = None.
    Для шаблонного Excel: конфиг копируется с параметрами из TemplateConfig.
    """
    path = config.input_file
    suffix = path.suffix.lower()
    stream = config.input_stream

    if suffix in _EXCEL_SUFFIXES:
        if is_template(path, stream=stream):
            logger.info("Detected ODS template format: %s", path.name)
            tmpl = read_template_config(path, stream=stream)
            effective = dataclasses.replace(
                config,
                skip_rows=tmpl.skip_rows,
                dtypes=tmpl.dtypes if config.dtypes is None else config.dtypes,
            )
            cfg = ExcelReadConfig(
                path=path,
                stream=stream,
                sheet_name="data",
                skip_rows=tmpl.skip_rows,
                skip_cols=config.skip_cols,
                max_row=config.max_row,
                skip_header_validation=True,
            )
            sheet = read_excel(cfg)
            return sheet, effective, tmpl
        else:
            logger.info("Detected regular Excel format: %s", path.name)
            sheet = read_file(
                path,
                stream=stream,
                sheet_name=config.sheet_name,
                skip_rows=config.skip_rows,
                skip_cols=config.skip_cols,
                max_row=config.max_row,
            )
            return sheet, config, None

    # CSV / TSV / SQL / TXT — делегируем read_file()
    logger.info("Detected %s format: %s", suffix.lstrip(".").upper(), path.name)
    sheet = read_file(
        path,
        stream=stream,
        encoding=config.encoding_input,
        delimiter=config.delimiter,
        skip_rows=config.skip_rows,
        skip_cols=config.skip_cols,
        max_row=config.max_row,
    )

    # Для SQL: если имя таблицы дефолтное — пробуем взять из файла
    effective = config
    if suffix in (".sql", ".txt") and config.table_name == "table_name":
        from .readers.sql_reader import SqlReadConfig, read_sql
        try:
            sql_data = read_sql(SqlReadConfig(path=path, stream=stream, encoding=config.encoding_input))
            if sql_data.table_name:
                effective = dataclasses.replace(config, table_name=sql_data.table_name)
        except Exception as exc:
            logger.debug("Could not extract table name from SQL file: %s", exc)

    return sheet, effective, None


# ── Writer / output helpers ──────────────────────────────────────────────────

def _build_writer_config(config: LoaderConfig, stream: IO[str]) -> FileWriterConfig:
    return FileWriterConfig(
        output_stream=stream,
        db_type=config.db_type,
        table_name=config.table_name,
        scheme_name=config.scheme_name,
        encoding=config.encoding_output,
        batch_size=config.batch_size,
        delimiter=config.delimiter,
    )


def _resolve_output_path(config: LoaderConfig) -> Path:
    ts = datetime.now().strftime("%d%m%y_%H%M%S")
    suffix = f".{config.dump_type.value}"
    filename = f"{config.input_file.stem}_{ts}{suffix}"
    base = config.output_dir if config.output_dir is not None else config.input_file.parent
    return base / filename


# ── Row-level helpers ────────────────────────────────────────────────────────

def _make_cell_name(row_idx: int, col_idx: int, skip_rows: int, skip_cols: int) -> str:
    col = col_idx + skip_cols + 1
    row = row_idx + skip_rows + 2
    letter = ""
    while col > 0:
        col, remainder = divmod(col - 1, 26)
        letter = chr(65 + remainder) + letter
    return f"{letter}{row}"


_ValidatorFn = Callable[[Any], CellResult]


def _build_validators(
    headers: list[str],
    dtypes: dict[str, str],
    db_type: DatabaseType,
) -> dict[str, _ValidatorFn]:
    from .validator import get_validator
    return {
        col: get_validator(dtype, db_type)
        for col, dtype in dtypes.items()
        if col in headers
    }


def _validate_row(
    row: tuple,
    headers: list[str],
    validators: dict[str, _ValidatorFn],
    row_idx: int,
    config: LoaderConfig,
    result: FileValidationResult,
    key_columns: frozenset[str] | None = None,
) -> tuple:
    """Валидировать одну строку, вернуть (возможно скорректированный) кортеж.

    NULL в ключевом поле — алерт (предупреждение), загрузка продолжается.
    NULL в обычном поле — пропускается без валидации типа.
    """
    output = []
    for col_idx, (col_name, value) in enumerate(zip(headers, row)):
        # ── Проверка ключевого поля на NULL (алерт, не ошибка) ──────────
        if key_columns and col_name in key_columns and value is None:
            result.add_warning(CellValidationError(
                cell_name=_make_cell_name(
                    row_idx, col_idx, config.skip_rows, config.skip_cols
                ),
                cell_value=value,
                expected_type=config.dtypes.get(col_name, "unknown"),
                message="key column is NULL",
                col_name=col_name,
            ))
            output.append(None)
            continue

        # ── NULL в обычном поле — пропускаем валидацию типа ─────────────
        if value is None:
            output.append(None)
            continue

        validate = validators.get(col_name)
        if validate is None:
            output.append(value)
            continue

        cell_result = validate(value)
        if isinstance(cell_result, Ok):
            output.append(cell_result.value)
        else:
            output.append(None)
            result.add_error(CellValidationError(
                cell_name=_make_cell_name(
                    row_idx, col_idx, config.skip_rows, config.skip_cols
                ),
                cell_value=value,
                expected_type=config.dtypes.get(col_name, "unknown"),
                message=cell_result.message,
                col_name=col_name,
            ))
    return tuple(output)


def _apply_row_transforms(row: tuple, config: LoaderConfig) -> tuple:
    if config.is_strip:
        row = tuple(v.strip() if isinstance(v, str) else v for v in row)
    if config.set_empty_str_to_null:
        row = tuple(None if (isinstance(v, str) and not v.strip()) else v for v in row)
    return row


def _insert_fixed_values(
    row: tuple,
    headers: list[str],
    fixed_values: dict[str, str],
) -> tuple:
    """Вставить фиксированные значения шаблона на правильные позиции в строке."""
    if not fixed_values:
        return row
    data_iter = iter(row)
    result = []
    for col in headers:
        if col in fixed_values:
            result.append(fixed_values[col])
        else:
            try:
                result.append(next(data_iter))
            except StopIteration:
                result.append(None)
    return tuple(result)


def _append_extra_columns(
    row: tuple,
    source_headers: list[str],
    config: LoaderConfig,
) -> tuple:
    """Добавить timestamp и wf_load_idn к строке, если они не присутствуют в источнике.

    Проверяем по source_headers (оригинальные колонки файла), а не по финальному
    списку заголовков — финальный уже содержит 'load_dttm'/'write_ts',
    поэтому проверка по нему всегда вернула бы False.
    """
    row = list(row)
    if config.timestamp and config.timestamp.value not in source_headers:
        now = datetime.now()
        if config.db_type == DatabaseType.CLICKHOUSE:
            row.append(now)
        else:
            row.append(now.strftime("%Y-%m-%d %H:%M:%S"))
    if config.wf_load_idn:
        row.append(config.input_file.name)
    return tuple(row)


def _wrap_with_progress(
    rows_iter: Iterator,
    show_progress: bool,
    desc: str = "Processing",
) -> Iterator:
    """Опционально оборачивает итератор в tqdm прогресс-бар.

    show_progress=False — возвращает итератор как есть (для Airflow и prod).
    show_progress=True  — удобен при ручном запуске из терминала.
    """
    if not show_progress:
        return rows_iter
    try:
        from tqdm import tqdm
        return tqdm(rows_iter, desc=desc, unit="row")
    except ImportError:
        logger.debug("tqdm не установлен, прогресс-бар недоступен.")
        return rows_iter


# ── Shared pipeline ──────────────────────────────────────────────────────────

@dataclasses.dataclass
class _Pipeline:
    """Внутреннее состояние pipeline: результат подготовки до итерации строк."""
    effective_config: LoaderConfig
    sheet: SheetData
    tmpl: TemplateConfig | None
    headers: list[str]
    source_headers: list[str]
    headers_to_validate: list
    validators: dict[str, _ValidatorFn]
    key_columns: frozenset[str] | None
    needs_validation: bool
    validation_result: FileValidationResult
    excluded_final_indices: frozenset[int]


def _build_pipeline(config: LoaderConfig) -> _Pipeline:
    """Прочитать файл, построить валидаторы, вернуть подготовленный pipeline."""
    needs_validation = config.error_mode in (
        ErrorMode.VERIFY, ErrorMode.RAISE, ErrorMode.COERCE,
    )
    sheet, effective_config, tmpl = _resolve_reader(config)

    if needs_validation and not effective_config.dtypes:
        raise ConfigurationError(
            "dtypes must be provided when error_mode is not IGNORE. "
            "Pass a dtypes dict or use parse_ddl() to extract types from a DDL string."
        )

    headers = list(tmpl.headers) if tmpl is not None else list(sheet.headers)
    source_headers = list(sheet.headers)

    if effective_config.timestamp and effective_config.timestamp.value not in headers:
        headers.append(effective_config.timestamp.value)
    if effective_config.wf_load_idn:
        headers.append("wf_load_idn")

    exclude = effective_config.exclude_columns
    excluded_final_indices: frozenset[int] = frozenset()
    if exclude:
        excluded_final_indices = frozenset(i for i, h in enumerate(headers) if h in exclude)
        headers = [h for h in headers if h not in exclude]

    validators: dict[str, _ValidatorFn] = {}
    headers_to_validate: list = []
    if needs_validation and effective_config.dtypes:
        headers_to_validate = (
            [h for h in tmpl.headers if h not in (tmpl.fixed_values or {})]
            if tmpl is not None
            else list(sheet.headers)
        )
        validators = _build_validators(
            headers_to_validate,
            effective_config.dtypes,
            effective_config.db_type,
        )

    return _Pipeline(
        effective_config=effective_config,
        sheet=sheet,
        tmpl=tmpl,
        headers=headers,
        source_headers=source_headers,
        headers_to_validate=headers_to_validate,
        validators=validators,
        key_columns=tmpl.key_columns if tmpl is not None else None,
        needs_validation=needs_validation,
        validation_result=FileValidationResult(),
        excluded_final_indices=excluded_final_indices,
    )


def _iter_rows(p: _Pipeline, rows_skipped: list[int]) -> Iterator:
    """Генератор обработанных строк.

    rows_skipped — одноэлементный список-счётчик пустых строк (мутируется в месте).
    """
    for row_idx, raw_row in enumerate(p.sheet.rows):
        if not any(cell is not None for cell in raw_row):
            rows_skipped[0] += 1
            continue
        row = _apply_row_transforms(raw_row, p.effective_config)
        if p.needs_validation:
            row = _validate_row(
                row,
                p.headers_to_validate,
                p.validators,
                row_idx,
                p.effective_config,
                p.validation_result,
                key_columns=p.key_columns,
            )
        if p.tmpl is not None and p.tmpl.fixed_values:
            row = _insert_fixed_values(row, p.tmpl.headers, p.tmpl.fixed_values)
        row = _append_extra_columns(row, p.source_headers, p.effective_config)
        if p.excluded_final_indices:
            row = tuple(v for i, v in enumerate(row) if i not in p.excluded_final_indices)
        yield row


# ── Public API ───────────────────────────────────────────────────────────────

def load_rows(
    config: LoaderConfig,
) -> tuple[list[str], Iterator, FileValidationResult]:
    """Прочитать и валидировать файл, вернуть (headers, rows_iter, validation_result).

    Не записывает файл — предназначен для прямой загрузки в БД через DbWriter.

    Поведение ошибок валидации определяется config.error_mode:
        IGNORE  — строки возвращаются как есть, без проверки.
        COERCE  — ошибочные ячейки → None, строка продолжает итерацию.
        RAISE / VERIFY — ошибки накапливаются в FileValidationResult;
            вызывающий код должен проверить validation_result.is_valid
            и решить, прерывать ли загрузку.
    """
    p = _build_pipeline(config)
    return p.headers, _iter_rows(p, [0]), p.validation_result


def load(config: LoaderConfig) -> LoadResult:
    """Запустить полный pipeline: определить формат → прочитать → валидировать → записать SQL/CSV.

    Поддерживаемые форматы:
        - Обычный Excel (.xlsx/.xls/.xlsm)
        - Шаблонный Excel (листы 'data' + 'klad_config')
        - CSV / TSV
        - SQL INSERT-файлы

    Режимы обработки ошибок:
        IGNORE  — записать строки как есть, без валидации
        COERCE  — валидировать; ошибочные ячейки → NULL, запись продолжается
        VERIFY  — валидировать; при ошибках поднять DataValidationError, файл не создаётся
        RAISE   — валидировать; ошибочные ячейки → NULL, при ошибках поднять DataValidationError

    При любой ошибке во время записи частично созданный output-файл удаляется
    автоматически — пользователь не получит неполный файл.

    Returns:
        LoadResult с полями rows_written, rows_skipped, output_file, error_file, has_errors.

    Note:
        Конфигурационная валидация (batch_size, skip_rows, кодировки и т.д.)
        выполняется в LoaderConfig.__post_init__ при создании объекта.
    """
    rows_skipped: list[int] = [0]
    rows_written = 0
    p = _build_pipeline(config)

    # ── VERIFY: только проверка, без записи файла ──────────────────────────
    if p.effective_config.error_mode == ErrorMode.VERIFY:
        for _ in _iter_rows(p, rows_skipped):
            rows_written += 1
        has_errors = not p.validation_result.is_valid
        log_validation_result(p.validation_result, config.input_file, logger)
        error_file: Path | None = None
        if has_errors and p.effective_config.validation_report_dir is not None:
            error_file = write_report(
                p.validation_result,
                config.input_file,
                p.effective_config.validation_report_dir,
                include_sample_values=p.effective_config.validation_report_include_values,
            )
            logger.info("Validation report saved: %s", error_file)
        if has_errors:
            raise DataValidationError(
                f"Validation failed: {len(p.validation_result.errors)} error(s).",
                p.validation_result,
            )
        return LoadResult(
            rows_written=rows_written,
            rows_skipped=rows_skipped[0],
            output_file=None,
            error_file=error_file,
            has_errors=False,
            validation_result=p.validation_result,
        )

    # ── Запись файла ───────────────────────────────────────────────────────
    # Если в конфиге передан готовый поток — пишем в него напрямую (режим стриминга).
    # Иначе — открываем локальный файл сами; при ошибке удаляем неполный файл.
    using_stream = p.effective_config.output_stream is not None

    show_progress = getattr(p.effective_config, "show_progress", False)

    def _counted_rows() -> Iterator:
        nonlocal rows_written
        for row in _iter_rows(p, rows_skipped):
            rows_written += 1
            yield row

    rows_to_write = _wrap_with_progress(
        _counted_rows(),
        show_progress=show_progress,
        desc=f"Loading {config.input_file.name}",
    )

    output_path: Path | None = None

    if using_stream:
        writer_config = _build_writer_config(p.effective_config, p.effective_config.output_stream)
        writer = (
            CsvFileWriter(writer_config)
            if p.effective_config.dump_type == DumpType.CSV
            else SqlFileWriter(writer_config)
        )
        writer.write(p.headers, rows_to_write)
    else:
        output_path = _resolve_output_path(p.effective_config)
        newline = "" if p.effective_config.dump_type == DumpType.CSV else None
        try:
            with open(
                output_path, "w",
                encoding=p.effective_config.encoding_output,
                newline=newline,
            ) as fh:
                writer_config = _build_writer_config(p.effective_config, fh)
                writer = (
                    CsvFileWriter(writer_config)
                    if p.effective_config.dump_type == DumpType.CSV
                    else SqlFileWriter(writer_config)
                )
                writer.write(p.headers, rows_to_write)
        except Exception:
            if output_path.exists():
                output_path.unlink()
                logger.warning(
                    "Частично созданный файл удалён после ошибки: %s",
                    output_path,
                )
            raise

    has_errors = not p.validation_result.is_valid

    if p.needs_validation:
        log_validation_result(p.validation_result, config.input_file, logger)

    report_file: Path | None = None
    if p.needs_validation and has_errors and p.effective_config.validation_report_dir is not None:
        report_file = write_report(
            p.validation_result,
            config.input_file,
            p.effective_config.validation_report_dir,
            include_sample_values=p.effective_config.validation_report_include_values,
        )
        logger.info("Validation report saved: %s", report_file)

    if p.effective_config.error_mode == ErrorMode.RAISE and has_errors:
        raise DataValidationError(
            f"Validation failed: {len(p.validation_result.errors)} error(s).",
            p.validation_result,
        )

    if output_path is not None:
        logger.info(
            "%s written: %d rows → %s",
            p.effective_config.dump_type.value.upper(),
            rows_written,
            output_path.name,
        )
    else:
        logger.info(
            "%s written: %d rows (stream)",
            p.effective_config.dump_type.value.upper(),
            rows_written,
        )

    return LoadResult(
        rows_written=rows_written,
        rows_skipped=rows_skipped[0],
        output_file=output_path,
        error_file=report_file,
        has_errors=has_errors,
        validation_result=p.validation_result if p.needs_validation else None,
    )
