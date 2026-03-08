from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .enums import DatabaseType, DumpType, ErrorMode
from .exceptions import ConfigurationError, DataValidationError
from .models import LoaderConfig, CellValidationError, FileValidationResult
from .reader import ExcelReadConfig, read_excel
from .validator import get_validator
from .writers.base import FileWriterConfig
from .writers.csv_file import CsvFileWriter
from .writers.sql_file import SqlFileWriter


# ── Result ────────────────────────────────────────────────────────────────────

@dataclass
class LoadResult:
    """Minimal summary returned by load(). Not statistics — just facts."""
    rows_written: int
    output_path: Path


# ── Internal helpers ──────────────────────────────────────────────────────────

def _build_read_config(config: LoaderConfig) -> ExcelReadConfig:
    return ExcelReadConfig(
        path=config.input_file,
        sheet_name=config.sheet_name,
        skip_rows=config.skip_rows,
        skip_cols=config.skip_cols,
        max_row=config.max_row,
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


def _make_cell_name(row_idx: int, col_idx: int, skip_rows: int, skip_cols: int) -> str:
    """Convert 0-based indices to Excel cell notation, e.g. 'B5'."""
    col = col_idx + skip_cols + 1
    row = row_idx + skip_rows + 2  # +1 for header, +1 for 1-based
    letter = ""
    while col > 0:
        col, remainder = divmod(col - 1, 26)
        letter = chr(65 + remainder) + letter
    return f"{letter}{row}"


def _validate_row(
    row: tuple,
    validators: list,
    row_idx: int,
    config: LoaderConfig,
    result: FileValidationResult,
) -> tuple:
    """
    Validate one row. Returns the (possibly coerced) output row.
    Invalid cells become None; errors are recorded in *result*.
    """
    from .result import Ok  # local import avoids circular at module level

    output = []
    for col_idx, (value, validate) in enumerate(zip(row, validators)):
        cell_result = validate(value)
        if isinstance(cell_result, Ok):
            output.append(cell_result.value)
        else:
            output.append(None)
            result.add_error(CellValidationError(
                cell_name=_make_cell_name(row_idx, col_idx, config.skip_rows, config.skip_cols),
                cell_value=value,
                expected_type=config.dtypes[col_idx],
                message=cell_result.message,
            ))
    return tuple(output)


def _apply_row_transforms(row: tuple, config: LoaderConfig) -> tuple:
    """Apply strip and null-coercion transforms."""
    if config.is_strip:
        row = tuple(v.strip() if isinstance(v, str) else v for v in row)
    if config.set_empty_str_to_null:
        row = tuple(None if v == "" else v for v in row)
    return row


def _append_extra_columns(
    row: tuple,
    headers: list[str],
    config: LoaderConfig,
) -> tuple:
    """Append timestamp and wf_load_idn columns if configured."""
    row = list(row)
    if config.timestamp and config.timestamp.value in headers:
        # already present in data — don't duplicate
        pass
    elif config.timestamp:
        row.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    if config.wf_load_idn:
        row.append(config.input_file.name)
    return tuple(row)


# ── Public API ────────────────────────────────────────────────────────────────

def load(config: LoaderConfig) -> LoadResult:
    """
    Run the full pipeline: read Excel → validate → write SQL/CSV.

    Error modes:
        IGNORE  — write rows as-is, no validation
        COERCE  — validate; replace invalid cells with NULL, always write
        VERIFY  — validate; raise DataValidationError if any errors found, do not write
        RAISE   — validate; replace invalid cells with NULL, raise if any errors found

    Args:
        config: Full loader configuration.

    Returns:
        LoadResult with rows_written and output_path.

    Raises:
        ConfigurationError: if dtypes are required but not provided.
        DataValidationError: if error_mode is VERIFY or RAISE and errors were found.
        FileReadError: if the Excel file cannot be read.
        HeaderValidationError: if headers are invalid.
    """
    needs_validation = config.error_mode in (
        ErrorMode.VERIFY, ErrorMode.RAISE, ErrorMode.COERCE
    )

    if needs_validation and not config.dtypes:
        raise ConfigurationError(
            "dtypes must be provided when error_mode is not IGNORE."
        )

    # ── 1. Read ───────────────────────────────────────────────────────────────
    sheet = read_excel(_build_read_config(config))

    # Build extra headers for timestamp / wf_load_idn
    headers = list(sheet.headers)
    if config.timestamp and config.timestamp.value not in headers:
        headers.append(config.timestamp.value)
    if config.wf_load_idn:
        headers.append("wf_load_idn")

    # ── 2. Build validators ───────────────────────────────────────────────────
    validators = []
    if needs_validation:
        for dtype in config.dtypes:
            validators.append(get_validator(dtype, config.db_type))

    # ── 3. Process rows ───────────────────────────────────────────────────────
    validation_result = FileValidationResult()

    def _processed_rows():
        for row_idx, raw_row in enumerate(sheet.rows):
            row = _apply_row_transforms(raw_row, config)

            if needs_validation:
                row = _validate_row(row, validators, row_idx, config, validation_result)

            row = _append_extra_columns(row, sheet.headers, config)
            yield row

    # ── 4. Write ──────────────────────────────────────────────────────────────
    output_path = _resolve_output_path(config)

    rows_written = 0

    if config.error_mode == ErrorMode.VERIFY:
        # consume rows only for validation, do not write
        for _ in _processed_rows():
            rows_written += 1
        if not validation_result.is_valid:
            raise DataValidationError(
                f"Validation failed with {len(validation_result.errors)} error(s).",
                validation_result,
            )
        # VERIFY succeeded — no file produced
        return LoadResult(rows_written=rows_written, output_path=output_path)

    writer_config = _build_writer_config(config, output_path)
    writer = (
        CsvFileWriter(writer_config)
        if config.dump_type == DumpType.CSV
        else SqlFileWriter(writer_config)
    )

    def _counted_rows():
        nonlocal rows_written
        for row in _processed_rows():
            rows_written += 1
            yield row

    writer.write(headers, _counted_rows())

    if config.error_mode == ErrorMode.RAISE and not validation_result.is_valid:
        raise DataValidationError(
            f"Validation failed with {len(validation_result.errors)} error(s).",
            validation_result,
        )

    return LoadResult(rows_written=rows_written, output_path=output_path)