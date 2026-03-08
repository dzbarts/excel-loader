from __future__ import annotations

import dataclasses
import logging
from datetime import datetime
from pathlib import Path

from .enums import DatabaseType, DumpType, ErrorMode
from .exceptions import ConfigurationError, DataValidationError
from .models import LoaderConfig, LoadResult, CellValidationError, FileValidationResult
from .reader import ExcelReadConfig, SheetData, read_excel
from .template import TemplateConfig, is_template, read_template_config
from .writers.base import FileWriterConfig
from .writers.csv_file import CsvFileWriter
from .writers.sql_file import SqlFileWriter

logger = logging.getLogger(__name__)

# Encodings supported for output files.
# encoding_input is kept in LoaderConfig for future CSV-reader support —
# openpyxl reads xlsx as binary and ignores it.
_SUPPORTED_ENCODINGS: frozenset[str] = frozenset({
    "utf-8", "utf-16", "utf-16-le", "utf-16-be",
    "ascii", "latin1", "cp1252", "cp1251", "cp866",
    "koi8-r", "koi8-u", "iso-8859-5",
    "gbk", "big5", "shift_jis", "euc-jp", "euc-kr",
})


# ── Config helpers ────────────────────────────────────────────────────────────

def _validate_config(config: LoaderConfig) -> None:
    """Raise ConfigurationError for any invalid config values."""
    if config.encoding_output.lower() not in _SUPPORTED_ENCODINGS:
        raise ConfigurationError(
            f"Unsupported encoding_output '{config.encoding_output}'. "
            f"Supported: {sorted(_SUPPORTED_ENCODINGS)}"
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
    """Generate a timestamped output path next to the input file."""
    ts = datetime.now().strftime("%d%m%y_%H%M%S")
    suffix = f".{config.dump_type.value}"
    return config.input_file.with_name(
        f"{config.input_file.stem}_{ts}"
    ).with_suffix(suffix)


# ── Reader resolution ─────────────────────────────────────────────────────────

def _resolve_reader(
    config: LoaderConfig,
) -> tuple[SheetData, LoaderConfig, TemplateConfig | None]:
    """
    Detect the input format and return (sheet_data, effective_config, template_config).

    For regular Excel files: template_config is None, config unchanged.
    For template Excel files: template_config carries the parsed metadata,
        and effective_config is a copy of config with skip_rows and dtypes
        filled in from the template — so the rest of load() doesn't need
        to know about templates at all.

    This is the single decision point for future format expansion
    (CSV reader, SQL reader, etc. will be added here).
    """
    path = config.input_file
    suffix = path.suffix.lower()

    if suffix in (".xlsx", ".xls"):
        if is_template(path):
            logger.info("Detected ODS template format: %s", path.name)
            tmpl = read_template_config(path)

            # Merge template metadata into a config copy.
            # dataclasses.replace() is the correct tool: it creates a new
            # frozen-safe copy with only the specified fields changed —
            # the original config object is never mutated.
            effective = dataclasses.replace(
                config,
                skip_rows=tmpl.skip_rows,
                # Template always targets GP; db_type from user config is respected
                # but dtypes come from the template.
                dtypes=tmpl.dtypes if config.dtypes is None else config.dtypes,
            )
            read_cfg = ExcelReadConfig(
                path=path,
                sheet_name="data",
                skip_rows=tmpl.skip_rows,
                skip_cols=config.skip_cols,
                max_row=config.max_row,
            )
            sheet = read_excel(read_cfg)
            return sheet, effective, tmpl

        else:
            logger.info("Detected regular Excel format: %s", path.name)
            read_cfg = ExcelReadConfig(
                path=path,
                sheet_name=config.sheet_name,
                skip_rows=config.skip_rows,
                skip_cols=config.skip_cols,
                max_row=config.max_row,
            )
            sheet = read_excel(read_cfg)
            return sheet, config, None

    # Future: CSV, SQL/TXT readers will go here
    raise ConfigurationError(
        f"Unsupported input format: '{suffix}'. "
        f"Supported: .xlsx, .xls"
    )


# ── Row-level helpers ─────────────────────────────────────────────────────────

def _make_cell_name(row_idx: int, col_idx: int, skip_rows: int, skip_cols: int) -> str:
    """Convert 0-based row/col indices to Excel notation, e.g. 'B5'."""
    col = col_idx + skip_cols + 1
    row = row_idx + skip_rows + 2  # +1 for header row, +1 for 1-based
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
    """
    Build a {col_name: validator_fn} mapping.

    Only columns present in BOTH headers and dtypes get validators.
    Columns in headers but not in dtypes → pass-through (no validation).
    Columns in dtypes but not in headers → silently ignored.
    """
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
    """
    Validate one row, returning a (possibly coerced) output tuple.

    Valid cells    → converted value (e.g. float for decimal, str for date).
    Invalid cells  → None; CellValidationError appended to result.
    No validator   → value passed through unchanged.
    Key col + None → additional error recorded even if type was valid.
    """
    from .result import Ok

    output = []
    for col_idx, (col_name, value) in enumerate(zip(headers, row)):
        validate = validators.get(col_name)

        if validate is None:
            output.append(value)
            continue

        cell_result = validate(value)

        if isinstance(cell_result, Ok):
            converted = cell_result.value
            if key_columns and col_name in key_columns and converted is None:
                result.add_error(CellValidationError(
                    cell_name=_make_cell_name(
                        row_idx, col_idx, config.skip_rows, config.skip_cols
                    ),
                    cell_value=value,
                    expected_type=config.dtypes[col_name],
                    message="key column must not be NULL",
                ))
            output.append(converted)
        else:
            output.append(None)
            result.add_error(CellValidationError(
                cell_name=_make_cell_name(
                    row_idx, col_idx, config.skip_rows, config.skip_cols
                ),
                cell_value=value,
                expected_type=config.dtypes[col_name],
                message=cell_result.message,
            ))

    return tuple(output)


def _apply_row_transforms(row: tuple, config: LoaderConfig) -> tuple:
    """Strip whitespace and/or coerce empty strings to None."""
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
    """
    Insert template fixed-value columns into the row at the correct positions.

    fixed_values columns are NOT present in the data rows read from Excel —
    they come from specific cells on the 'data' sheet (parsed during template
    config reading). We insert them by position based on the full headers list.

    Example:
        headers      = ["source_system", "id", "name"]  (full output order)
        data row     = (1, "Alice")                      (only "table" columns)
        fixed_values = {"source_system": "ГПН"}
        result       = ("ГПН", 1, "Alice")
    """
    if not fixed_values:
        return row

    # Reconstruct row by walking the full headers list
    data_iter = iter(row)
    result = []
    for col in headers:
        if col in fixed_values:
            result.append(fixed_values[col])
        else:
            try:
                result.append(next(data_iter))
            except StopIteration:
                # More fixed columns than expected — header/data mismatch
                result.append(None)
    return tuple(result)


def _append_extra_columns(
    row: tuple,
    headers: list[str],
    config: LoaderConfig,
) -> tuple:
    """Append timestamp and wf_load_idn columns if configured."""
    row = list(row)
    if config.timestamp and config.timestamp.value not in headers:
        row.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    if config.wf_load_idn:
        row.append(config.input_file.name)
    return tuple(row)


# ── Public API ────────────────────────────────────────────────────────────────

def load(config: LoaderConfig) -> LoadResult:
    """
    Run the full pipeline: detect format → read → validate → write SQL/CSV.

    Supports:
        - Regular Excel (.xlsx/.xls)
        - ODS template Excel (sheets 'data' + 'klad_config')

    Error modes:
        IGNORE  — write rows as-is, no validation
        COERCE  — validate; replace invalid cells with NULL, always write
        VERIFY  — validate; raise DataValidationError if errors, do not write
        RAISE   — validate; replace invalid cells with NULL, raise if errors

    Args:
        config: Full loader configuration.

    Returns:
        LoadResult with rows_written and output_path.

    Raises:
        ConfigurationError: if config is invalid or dtypes required but missing.
        DataValidationError: if error_mode is VERIFY or RAISE and errors found.
        FileReadError: if the file cannot be read.
        HeaderValidationError: if headers are invalid.
        TemplateError: if template structure is invalid.
    """
    _validate_config(config)

    needs_validation = config.error_mode in (
        ErrorMode.VERIFY,
        ErrorMode.RAISE,
        ErrorMode.COERCE,
    )

    # ── 1. Detect format and read ─────────────────────────────────────────
    sheet, effective_config, tmpl = _resolve_reader(config)

    # After resolving the reader, check dtypes requirement.
    # We check here (not before) because templates provide dtypes automatically.
    if needs_validation and not effective_config.dtypes:
        raise ConfigurationError(
            "dtypes must be provided when error_mode is not IGNORE. "
            "Pass a dtypes dict or use parse_ddl() to extract types from a DDL string."
        )

    # ── 2. Build output headers ───────────────────────────────────────────
    # Start from data headers, insert fixed-value column names at their
    # positions (from template), then append timestamp / wf_load_idn.
    if tmpl is not None:
        # Template defines the canonical output column order
        headers = list(tmpl.headers)
    else:
        headers = list(sheet.headers)

    if effective_config.timestamp and effective_config.timestamp.value not in headers:
        headers.append(effective_config.timestamp.value)
    if effective_config.wf_load_idn:
        headers.append("wf_load_idn")

    # ── 3. Build validators ───────────────────────────────────────────────
    validators: dict[str, object] = {}
    if needs_validation and effective_config.dtypes:
        # For templates we validate against the "table" columns only —
        # fixed-value columns are constants, they don't need cell-level validation.
        validate_headers = (
            [h for h in sheet.headers if h not in (tmpl.fixed_values or {})]
            if tmpl else sheet.headers
        )
        validators = _build_validators(
            validate_headers,
            effective_config.dtypes,
            effective_config.db_type,
        )

    key_columns = tmpl.key_columns if tmpl is not None else None

    # ── 4. Define row processing pipeline ────────────────────────────────
    validation_result = FileValidationResult()

    def _processed_rows():
        for row_idx, raw_row in enumerate(sheet.rows):
            row = _apply_row_transforms(raw_row, effective_config)
            if needs_validation:
                row = _validate_row(
                    row, sheet.headers, validators,
                    row_idx, effective_config, validation_result,
                    key_columns=key_columns,
                )
            # Insert fixed values AFTER validation — they are constants,
            # not data from the file, so validating them is meaningless.
            if tmpl is not None and tmpl.fixed_values:
                row = _insert_fixed_values(row, tmpl.headers, tmpl.fixed_values)
            row = _append_extra_columns(row, headers, effective_config)
            yield row

    # ── 5. Write (or verify-only) ─────────────────────────────────────────
    output_path = _resolve_output_path(effective_config)
    rows_written = 0

    if effective_config.error_mode == ErrorMode.VERIFY:
        for _ in _processed_rows():
            rows_written += 1
        if not validation_result.is_valid:
            raise DataValidationError(
                f"Validation failed: {len(validation_result.errors)} error(s).",
                validation_result,
            )
        logger.info(
            "VERIFY passed: %d rows checked, no errors. File: %s",
            rows_written, config.input_file.name,
        )
        return LoadResult(rows_written=rows_written, output_path=output_path)

    writer_config = _build_writer_config(effective_config, output_path)
    writer = (
        CsvFileWriter(writer_config)
        if effective_config.dump_type == DumpType.CSV
        else SqlFileWriter(writer_config)
    )

    def _counted_rows():
        nonlocal rows_written
        for row in _processed_rows():
            rows_written += 1
            yield row

    writer.write(headers, _counted_rows())

    if effective_config.error_mode == ErrorMode.RAISE and not validation_result.is_valid:
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
    return LoadResult(rows_written=rows_written, output_path=output_path)