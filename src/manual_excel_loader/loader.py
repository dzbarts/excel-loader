"""
loader.py
=========
Главная точка входа пакета.
Собирает pipeline: read_file() → (validate) → writer.write()

Loader не знает о деталях форматов — этим занимаются ридеры.
Loader не знает о деталях записи — этим занимаются врайтеры.
Его зона ответственности: конфигурация, валидация строк, склейка.

Изменения vs предыдущей версии:
  - При любой ошибке во время записи частично созданный output-файл удаляется
    (try/finally вокруг writer.write), пользователь не получает неполный файл.
  - Опциональный прогресс-бар через tqdm (LoaderConfig.show_progress=True).
    По умолчанию выключен — не мешает Airflow-логам.
"""
from __future__ import annotations

import dataclasses
import logging
from datetime import datetime
from pathlib import Path
from typing import Iterator

from .enums import DatabaseType, DumpType, ErrorMode
from .exceptions import ConfigurationError, DataValidationError
from .models import (
    CellValidationError,
    FileValidationResult,
    LoaderConfig,
    LoadResult,
)
from .readers import SheetData, read_file
from .readers.excel_reader import ExcelReadConfig, read_excel
from .template import TemplateConfig, is_template, read_template_config
from .writers.base import FileWriterConfig
from .writers.csv_file import CsvFileWriter
from .writers.sql_file import SqlFileWriter

logger = logging.getLogger(__name__)

# Единый список поддерживаемых кодировок.
# encoding_input — применяется при чтении CSV/TSV/SQL-файлов.
# encoding_output — применяется при записи SQL/CSV-файлов.
# Для Excel (.xlsx) кодировка не нужна: openpyxl читает бинарный формат.
SUPPORTED_ENCODINGS: frozenset[str] = frozenset({
    "utf-8", "utf-16", "utf-16-le", "utf-16-be",
    "ascii", "latin1", "cp1252", "cp1251", "cp866",
    "koi8-r", "koi8-u", "iso-8859-5",
    "gbk", "big5", "shift_jis", "euc-jp", "euc-kr",
})


# ── Config helpers ─────────────────────────────────────────────────────────────
def _validate_config(config: LoaderConfig) -> None:
    suffix = config.input_file.suffix.lower()
    # encoding_input нужна только для текстовых форматов
    if suffix in (".csv", ".tsv", ".sql", ".txt"):
        if config.encoding_input.lower() not in SUPPORTED_ENCODINGS:
            raise ConfigurationError(
                f"Unsupported encoding_input '{config.encoding_input}'. "
                f"Supported: {sorted(SUPPORTED_ENCODINGS)}"
            )
    if config.encoding_output.lower() not in SUPPORTED_ENCODINGS:
        raise ConfigurationError(
            f"Unsupported encoding_output '{config.encoding_output}'. "
            f"Supported: {sorted(SUPPORTED_ENCODINGS)}"
        )
    if config.batch_size <= 0:
        raise ConfigurationError(
            f"batch_size must be a positive integer, got {config.batch_size}."
        )
    if config.skip_rows < 0:
        raise ConfigurationError(
            f"skip_rows must be >= 0, got {config.skip_rows}."
        )
    if config.skip_cols < 0:
        raise ConfigurationError(
            f"skip_cols must be >= 0, got {config.skip_cols}."
        )


def _build_writer_config(config: LoaderConfig, output_path: Path) -> FileWriterConfig:
    return FileWriterConfig(
        output_path=output_path,
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
    return config.input_file.with_name(
        f"{config.input_file.stem}_{ts}"
    ).with_suffix(suffix)


# ── Reader resolution ──────────────────────────────────────────────────────────
_EXCEL_SUFFIXES = frozenset({".xlsx", ".xls", ".xlsm"})


def _resolve_reader(
    config: LoaderConfig,
) -> tuple[SheetData, LoaderConfig, TemplateConfig | None]:
    """Определить формат → вернуть (sheet_data, effective_config, template_config).

    Для обычного Excel и текстовых форматов: template_config = None.
    Для шаблонного Excel: конфиг копируется с параметрами из TemplateConfig.
    """
    path = config.input_file
    suffix = path.suffix.lower()

    if suffix in _EXCEL_SUFFIXES:
        if is_template(path):
            logger.info("Detected ODS template format: %s", path.name)
            tmpl = read_template_config(path)
            effective = dataclasses.replace(
                config,
                skip_rows=tmpl.skip_rows,
                dtypes=tmpl.dtypes if config.dtypes is None else config.dtypes,
            )
            cfg = ExcelReadConfig(
                path=path,
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
            sql_data = read_sql(SqlReadConfig(path=path, encoding=config.encoding_input))
            if sql_data.table_name:
                effective = dataclasses.replace(config, table_name=sql_data.table_name)
        except Exception:
            pass

    return sheet, effective, None


# ── Row-level helpers ──────────────────────────────────────────────────────────
def _make_cell_name(row_idx: int, col_idx: int, skip_rows: int, skip_cols: int) -> str:
    col = col_idx + skip_cols + 1
    row = row_idx + skip_rows + 2
    letter = ""
    while col > 0:
        col, remainder = divmod(col - 1, 26)
        letter = chr(65 + remainder) + letter
    return f"{letter}{row}"


def _build_validators(
    headers: list[str],
    dtypes: dict[str, str],
    db_type: DatabaseType,
) -> dict[str, object]:
    from .validator import get_validator
    return {
        col: get_validator(dtype, db_type)
        for col, dtype in dtypes.items()
        if col in headers
    }


def _validate_row(
    row: tuple,
    headers: list[str],
    validators: dict[str, object],
    row_idx: int,
    config: LoaderConfig,
    result: FileValidationResult,
    key_columns: frozenset[str] | None = None,
) -> tuple:
    """Валидировать одну строку, вернуть (возможно скорректированный) кортеж.

    NULL в ключевом поле проверяется до валидации типа — это самостоятельная
    ошибка («не может быть NULL»), а не ошибка типа («не целое число»).
    """
    from .result import Ok
    output = []
    for col_idx, (col_name, value) in enumerate(zip(headers, row)):
        # ── Проверка ключевого поля на NULL ───────────────────────────
        if key_columns and col_name in key_columns and value is None:
            result.add_error(CellValidationError(
                cell_name=_make_cell_name(
                    row_idx, col_idx, config.skip_rows, config.skip_cols
                ),
                cell_value=value,
                expected_type=config.dtypes.get(col_name, "unknown"),
                message="key column must not be NULL",
            ))
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
            ))
    return tuple(output)


def _apply_row_transforms(row: tuple, config: LoaderConfig) -> tuple:
    if config.is_strip:
        row = tuple(v.strip() if isinstance(v, str) else v for v in row)
    if config.set_empty_str_to_null:
        row = tuple(None if v == "" else v for v in row)
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
    sheet_headers: list[str],
    config: LoaderConfig,
) -> tuple:
    """Добавить timestamp и wf_load_idn к строке, если они не присутствуют в источнике.

    Проверяем по sheet_headers (оригинальные колонки файла), а не по финальному
    списку заголовков — финальный уже содержит 'load_dttm'/'write_ts', поэтому
    проверка по нему всегда вернула бы False.
    """
    row = list(row)
    if config.timestamp and config.timestamp.value not in sheet_headers:
        row.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
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


# ── Public API ─────────────────────────────────────────────────────────────────
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
    """
    _validate_config(config)

    needs_validation = config.error_mode in (
        ErrorMode.VERIFY,
        ErrorMode.RAISE,
        ErrorMode.COERCE,
    )

    # ── 1. Определить формат и прочитать ────────────────────────────────────
    sheet, effective_config, tmpl = _resolve_reader(config)

    if needs_validation and not effective_config.dtypes:
        raise ConfigurationError(
            "dtypes must be provided when error_mode is not IGNORE. "
            "Pass a dtypes dict or use parse_ddl() to extract types from a DDL string."
        )

    # ── 2. Сформировать итоговые заголовки ───────────────────────────────────
    # sheet.headers — сырые заголовки из файла (для шаблона: русские имена).
    # Для шаблона используем TemplateConfig.headers (технические EN-имена).
    if tmpl is not None:
        headers = list(tmpl.headers)
    else:
        headers = list(sheet.headers)

    # sheet_headers нужны в _append_extra_columns для проверки: есть ли
    # колонка уже в файле или её нужно сгенерировать.
    sheet_headers = list(sheet.headers)

    if effective_config.timestamp and effective_config.timestamp.value not in headers:
        headers.append(effective_config.timestamp.value)
    if effective_config.wf_load_idn:
        headers.append("wf_load_idn")

    # ── 3. Построить валидаторы ──────────────────────────────────────────────
    validators: dict[str, object] = {}
    if needs_validation and effective_config.dtypes:
        validate_against = (
            [h for h in tmpl.headers if h not in (tmpl.fixed_values or {})]
            if tmpl
            else sheet.headers
        )
        validators = _build_validators(
            validate_against,
            effective_config.dtypes,
            effective_config.db_type,
        )

    key_columns = tmpl.key_columns if tmpl is not None else None

    # ── 4. Pipeline обработки строк ──────────────────────────────────────────
    validation_result = FileValidationResult()
    _validate_headers_for_row = (
        [h for h in tmpl.headers if h not in (tmpl.fixed_values or {})]
        if tmpl is not None
        else list(sheet.headers)
    )
    rows_skipped = 0

    def _processed_rows():
        nonlocal rows_skipped
        for row_idx, raw_row in enumerate(sheet.rows):
            # Пустые строки уже отфильтрованы в reader; на всякий случай считаем
            if not any(cell is not None for cell in raw_row):
                rows_skipped += 1
                continue
            row = _apply_row_transforms(raw_row, effective_config)
            if needs_validation:
                row = _validate_row(
                    row,
                    _validate_headers_for_row,
                    validators,
                    row_idx,
                    effective_config,
                    validation_result,
                    key_columns=key_columns,
                )
            if tmpl is not None and tmpl.fixed_values:
                row = _insert_fixed_values(row, tmpl.headers, tmpl.fixed_values)
            row = _append_extra_columns(row, sheet_headers, effective_config)
            yield row

    # ── 5. Запись (или только проверка при VERIFY) ───────────────────────────
    output_path = _resolve_output_path(effective_config)
    rows_written = 0

    if effective_config.error_mode == ErrorMode.VERIFY:
        for _ in _processed_rows():
            rows_written += 1
        has_errors = not validation_result.is_valid
        if has_errors:
            raise DataValidationError(
                f"Validation failed: {len(validation_result.errors)} error(s).",
                validation_result,
            )
        logger.info(
            "VERIFY passed: %d rows, no errors. File: %s",
            rows_written,
            config.input_file.name,
        )
        return LoadResult(
            rows_written=rows_written,
            rows_skipped=rows_skipped,
            output_file=None,
            error_file=None,
            has_errors=False,
            validation_result=validation_result,
        )

    writer_config = _build_writer_config(effective_config, output_path)
    writer = (
        CsvFileWriter(writer_config)
        if effective_config.dump_type == DumpType.CSV
        else SqlFileWriter(writer_config)
    )

    show_progress = getattr(effective_config, "show_progress", False)

    def _counted_rows():
        nonlocal rows_written
        for row in _processed_rows():
            rows_written += 1
            yield row

    # Оборачиваем в прогресс-бар если запрошено
    rows_to_write = _wrap_with_progress(
        _counted_rows(),
        show_progress=show_progress,
        desc=f"Loading {config.input_file.name}",
    )

    # При любой ошибке во время записи — удалить неполный output-файл.
    # Без этого пользователь мог получить файл с частью строк и думать,
    # что загрузка прошла успешно.
    try:
        writer.write(headers, rows_to_write)
    except Exception:
        if output_path.exists():
            output_path.unlink()
            logger.warning(
                "Частично созданный файл удалён после ошибки: %s", output_path
            )
        raise

    has_errors = not validation_result.is_valid
    if effective_config.error_mode == ErrorMode.RAISE and has_errors:
        raise DataValidationError(
            f"Validation failed: {len(validation_result.errors)} error(s).",
            validation_result,
        )

    logger.info(
        "%s written: %d rows → %s",
        effective_config.dump_type.value.upper(),
        rows_written,
        output_path.name,
    )
    return LoadResult(
        rows_written=rows_written,
        rows_skipped=rows_skipped,
        output_file=output_path,
        error_file=None,
        has_errors=has_errors,
        validation_result=validation_result if needs_validation else None,
    )