from __future__ import annotations

from datetime import datetime
from pathlib import Path

from .enums import DatabaseType, DumpType, ErrorMode
from .exceptions import ConfigurationError, DataValidationError
from .models import LoaderConfig, LoadResult, CellValidationError, FileValidationResult
from .reader import ExcelReadConfig, read_excel
from .validator import get_validator
from .writers.base import FileWriterConfig
from .writers.csv_file import CsvFileWriter
from .writers.sql_file import SqlFileWriter

# Encodings we support for output files.
# encoding_input is stored in config for future CSV-reader support,
# but openpyxl ignores it (xlsx is binary).
_SUPPORTED_ENCODINGS: frozenset[str] = frozenset({
    "utf-8", "utf-16", "utf-16-le", "utf-16-be",
    "ascii", "latin1", "cp1252", "cp1251", "cp866",
    "koi8-r", "koi8-u", "iso-8859-5",
    "gbk", "big5", "shift_jis", "euc-jp", "euc-kr",
})


# ── Internal helpers ─────────────────────────────────────────────────────────

def _validate_config(config: LoaderConfig) -> None:
    """Raise ConfigurationError for any invalid config values."""
    if config.encoding_output.lower() not in _SUPPORTED_ENCODINGS:
        raise ConfigurationError(
            f"Unsupported encoding_output '{config.encoding_output}'. "
            f"Supported: {sorted(_SUPPORTED_ENCODINGS)}"
        )


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


def _build_validators(
    headers: list[str],
    dtypes: dict[str, str],
    db_type: DatabaseType,
) -> dict[str, object]:
    """
    Build a name→validator mapping for columns that appear in both
    the Excel headers and the dtypes dict.

    Columns in Excel but missing from dtypes → no validation (pass-through).
    Columns in dtypes but missing from Excel → silently ignored.
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
    Validate one row using name-based validator lookup.

    - Valid cells: converted value (e.g. "2024-01-01" for a date).
    - Invalid cells: None; error recorded in *result*.
    - Columns without a validator: value passed through unchanged.
    - Key columns with None value: recorded as key-null error.
    """
    from .result import Ok

    output = []
    for col_idx, (col_name, value) in enumerate(zip(headers, row)):
        validate = validators.get(col_name)

        if validate is None:
            # No validator for this column — pass through as-is
            output.append(value)
            continue

        cell_result = validate(value)

        if isinstance(cell_result, Ok):
            converted = cell_result.value
            # Key-column NULL check
            if key_columns and col_name in key_columns and converted is None:
                result.add_error(CellValidationError(
                    cell_name=_make_cell_name(row_idx, col_idx, config.skip_rows, config.skip_cols),
                    cell_value=value,
                    expected_type=config.dtypes[col_name],
                    message="key column must not be NULL",
                ))
            output.append(converted)
        else:
            output.append(None)
            result.add_error(CellValidationError(
                cell_name=_make_cell_name(row_idx, col_idx, config.skip_rows, config.skip_cols),
                cell_value=value,
                expected_type=config.dtypes[col_name],
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
    if config.timestamp and config.timestamp.value not in headers:
        row.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    if config.wf_load_idn:
        row.append(config.input_file.name)
    return tuple(row)


# ── Public API ───────────────────────────────────────────────────────────────

def load(config: LoaderConfig) -> LoadResult:
    """
    Run the full pipeline: read Excel → validate → write SQL/CSV.

    Error modes:
        IGNORE  — write rows as-is, no validation
        COERCE  — validate; replace invalid cells with NULL, always write
        VERIFY  — validate; raise DataValidationError if any errors, do not write
        RAISE   — validate; replace invalid cells with NULL, raise if any errors

    Args:
        config: Full loader configuration.

    Returns:
        LoadResult with rows_written and output_path.

    Raises:
        ConfigurationError: if config is invalid or dtypes are required but missing.
        DataValidationError: if error_mode is VERIFY or RAISE and errors were found.
        FileReadError: if the Excel file cannot be read.
        HeaderValidationError: if headers are invalid.
    """
    _validate_config(config)

    needs_validation = config.error_mode in (
        ErrorMode.VERIFY,
        ErrorMode.RAISE,
        ErrorMode.COERCE,
    )
    if needs_validation and not config.dtypes:
        raise ConfigurationError(
            "dtypes must be provided when error_mode is not IGNORE."
        )

    # ── 1. Read ──────────────────────────────────────────────────────────────
    sheet = read_excel(_build_read_config(config))

    # Build final header list (data headers + any extra columns we append)
    headers = list(sheet.headers)
    if config.timestamp and config.timestamp.value not in headers:
        headers.append(config.timestamp.value)
    if config.wf_load_idn:
        headers.append("wf_load_idn")

    # ── 2. Build validators (name-based, order-independent) ──────────────────
    validators: dict[str, object] = {}
    if needs_validation and config.dtypes:
        validators = _build_validators(sheet.headers, config.dtypes, config.db_type)

    # ── 3. Process rows ──────────────────────────────────────────────────────
    validation_result = FileValidationResult()

    def _processed_rows():
        for row_idx, raw_row in enumerate(sheet.rows):
            row = _apply_row_transforms(raw_row, config)
            if needs_validation:
                row = _validate_row(
                    row, sheet.headers, validators,
                    row_idx, config, validation_result,
                )
            row = _append_extra_columns(row, sheet.headers, config)
            yield row

    # ── 4. Write ─────────────────────────────────────────────────────────────
    output_path = _resolve_output_path(config)
    rows_written = 0

    if config.error_mode == ErrorMode.VERIFY:
        # Consume rows for validation only — no file produced
        for _ in _processed_rows():
            rows_written += 1
        if not validation_result.is_valid:
            raise DataValidationError(
                f"Validation failed with {len(validation_result.errors)} error(s).",
                validation_result,
            )
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