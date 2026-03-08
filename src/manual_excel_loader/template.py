# src/manual_excel_loader/template.py
"""
Parser for ODS Excel templates with a 'data' + 'klad_config' sheet structure.

Public API
----------
read_template_config(path) -> TemplateConfig
    Parse the klad_config sheet and return a fully-resolved TemplateConfig.
    Raises TemplateError for any structural problem.

is_template(path) -> bool
    Quick check: does the workbook have both required sheets?

TemplateConfig can be passed to load_template() which is a thin wrapper
around load() that pre-fills LoaderConfig from the template metadata.
"""
from __future__ import annotations

import re
import warnings
from dataclasses import dataclass, field
from pathlib import Path

import openpyxl

from .exceptions import FileReadError, TemplateError


# ── Data model ───────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class TemplateConfig:
    """
    Everything the loader needs to know, extracted from klad_config.

    Attributes:
        skip_rows:      How many rows to skip before the header row on 'data'.
                        Derived from the row number in cell B0 of klad_config.
        headers:        Technical (EN) column names, in klad_config order.
                        These become the output SQL/CSV column names.
        dtypes:         Column name → GP type string.
                        Passed directly to LoaderConfig.dtypes.
        key_columns:    Set of column names where NULL is not allowed.
        fixed_values:   Column name → literal string value for columns
                        that have a fixed value (not read from the data rows).
                        These are inserted into every output row.
        russian_headers: Russian display names from column A of klad_config,
                        used only for header-matching validation.
    """
    skip_rows: int
    headers: list[str]
    dtypes: dict[str, str]
    key_columns: frozenset[str]
    fixed_values: dict[str, str]
    russian_headers: list[str]


# ── Internal helpers ─────────────────────────────────────────────────────────

_CELL_ADDR = re.compile(r"^[A-Z]+(\d+)$")


def _parse_skip_rows(cell_value: str) -> int:
    """
    Extract skip_rows from klad_config B0, e.g. 'A3' → skip_rows=1.

    The cell contains an Excel address like 'A3' meaning data starts at row 3.
    The column letter carries no meaning here — only the row number matters.
    skip_rows = row_number - 2  (subtract 1 for header row, 1 for 1-based index)
    """
    if not isinstance(cell_value, str):
        raise TemplateError(
            f"klad_config cell B1 must be an Excel address like 'A3', "
            f"got: {cell_value!r}"
        )
    m = _CELL_ADDR.fullmatch(cell_value.strip().upper())
    if not m:
        raise TemplateError(
            f"klad_config cell B1 must match pattern like 'A3' "
            f"(uppercase letters + digits), got: {cell_value!r}"
        )
    row_number = int(m.group(1))
    if row_number < 2:
        raise TemplateError(
            f"Data cannot start before row 2 (got row {row_number} from '{cell_value}')."
        )
    return row_number - 2  # -1 for header, -1 for 0-based skip_rows


def _validate_header_alignment(
    data_headers_ru: list[str],
    config_headers_ru: list[str],
    fixed_cols: list[str],
) -> None:
    """
    Check that Russian header names on 'data' sheet match those in klad_config,
    considering that fixed-value columns are NOT present in the data sheet.
    """
    # klad_config rows where col B != 'table' are fixed values — skip them
    expected = [h for h in config_headers_ru if h not in fixed_cols]

    if data_headers_ru != expected:
        lines = ["Header mismatch between 'data' sheet and 'klad_config':"]
        lines.append(f"  data sheet:   {data_headers_ru}")
        lines.append(f"  klad_config:  {expected}")
        lines.append(
            "  Tip: copy-paste column names from one sheet to the other — "
            "invisible whitespace differences are a common cause."
        )
        raise TemplateError("\n".join(lines))


# ── Public API ───────────────────────────────────────────────────────────────

def is_template(path: Path) -> bool:
    """
    Return True if the workbook at *path* has both 'data' and 'klad_config' sheets.

    Does not validate the content — use read_template_config() for that.
    """
    warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
    try:
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        result = "data" in wb.sheetnames and "klad_config" in wb.sheetnames
        wb.close()
        return result
    except Exception:
        return False


def read_template_config(path: Path) -> TemplateConfig:
    """
    Parse the 'klad_config' sheet and return a TemplateConfig.

    klad_config sheet structure (1-indexed rows):
        Row 1: B1 = Excel address of first data row, e.g. "A3"
        Row 2: header row for klad_config itself (skipped)
        Row 3+: one row per output column:
            A = Russian display name (must match header on 'data' sheet)
            B = "table" if value comes from data rows,
                or Excel cell address (e.g. "A2") for a fixed value
            C = "true" if this is a key column (NULL not allowed)
            D = Technical (EN) column name → used in output SQL/CSV
            E = GP data type string (e.g. "integer", "text", "timestamp")
        Parsing stops at the first fully-empty row or a sentinel value.

    Args:
        path: Path to the Excel template file.

    Returns:
        TemplateConfig with all metadata needed by the loader.

    Raises:
        FileReadError:  if the file cannot be opened.
        TemplateError:  if the template structure is invalid.
    """
    warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

    try:
        wb = openpyxl.load_workbook(path, data_only=True, read_only=False)
    except FileNotFoundError:
        raise FileReadError(f"file not found: {path}")
    except Exception as exc:
        raise FileReadError(f"cannot open file '{path}': {exc}") from exc

    if "klad_config" not in wb.sheetnames:
        wb.close()
        raise TemplateError(
            f"Sheet 'klad_config' not found in '{path}'. "
            f"Available sheets: {wb.sheetnames}"
        )
    if "data" not in wb.sheetnames:
        wb.close()
        raise TemplateError(
            f"Sheet 'data' not found in '{path}'. "
            f"Available sheets: {wb.sheetnames}"
        )

    cfg_sheet = wb["klad_config"]
    data_sheet = wb["data"]

    try:
        result = _parse_klad_config(cfg_sheet, data_sheet)
    finally:
        wb.close()

    return result


_SENTINEL = "<< конец описания шаблона пустая строка в таблице"


def _parse_klad_config(cfg_sheet, data_sheet) -> TemplateConfig:
    """Core parsing logic — separated for testability."""

    rows = list(cfg_sheet.iter_rows(values_only=True, max_col=6))

    if not rows:
        raise TemplateError("klad_config sheet is empty.")

    # ── Row 0: B0 contains the address of the first data row ─────────────────
    first_row = rows[0]
    skip_rows = _parse_skip_rows(first_row[1])  # column B (0-indexed = 1)

    # ── Rows 2+: column definitions ──────────────────────────────────────────
    russian_headers_cfg: list[str] = []   # column A — for alignment check
    tech_headers: list[str] = []          # column D — output names
    dtypes: dict[str, str] = {}           # tech_name → GP type
    key_columns: set[str] = set()
    fixed_values: dict[str, str] = {}     # tech_name → literal value
    fixed_russian_names: list[str] = []   # Russian names of fixed-value cols

    for row_idx, row in enumerate(rows[2:], start=2):  # skip rows 0 and 1
        # Stop at empty row or sentinel
        if all(cell is None for cell in row):
            break
        if any(isinstance(cell, str) and _SENTINEL in cell for cell in row):
            break

        ru_name = row[0]   # A: Russian display name
        source = row[1]    # B: "table" or cell address
        is_key = row[2]    # C: "true" / True if key column
        tech_name = row[3] # D: technical EN name
        dtype = row[4]     # E: GP type

        # Validate required fields
        if not isinstance(tech_name, str) or not tech_name.strip():
            raise TemplateError(
                f"klad_config row {row_idx + 1}: column D (technical name) is empty."
            )
        if not isinstance(dtype, str) or not dtype.strip():
            raise TemplateError(
                f"klad_config row {row_idx + 1}: column E (data type) is empty "
                f"for column '{tech_name}'."
            )
        if not isinstance(source, str) or not source.strip():
            raise TemplateError(
                f"klad_config row {row_idx + 1}: column B (source) is empty "
                f"for column '{tech_name}'. Expected 'table' or a cell address."
            )

        tech = tech_name.strip().lower()
        russian_headers_cfg.append(str(ru_name).strip() if ru_name is not None else "")
        tech_headers.append(tech)
        dtypes[tech] = dtype.strip().lower()

        if str(is_key).strip().lower() == "true":
            key_columns.add(tech)

        if source.strip().lower() == "table":
            pass  # value comes from the data rows — nothing to do here
        else:
            # Fixed value: source is a cell address on the 'data' sheet
            cell_addr = source.strip().upper()
            if not _CELL_ADDR.fullmatch(cell_addr):
                raise TemplateError(
                    f"klad_config row {row_idx + 1}: column B must be 'table' or "
                    f"a cell address like 'A2', got: {source!r}"
                )
            cell_value = data_sheet[cell_addr].value
            fixed_values[tech] = str(cell_value) if cell_value is not None else ""
            fixed_russian_names.append(str(ru_name).strip() if ru_name is not None else "")

    if not tech_headers:
        raise TemplateError("klad_config defines no columns.")

    # ── Validate header alignment with 'data' sheet ───────────────────────────
    # Read Russian header row from the 'data' sheet (row = skip_rows + 1, 1-based)
    header_row_num = skip_rows + 1
    data_header_row = list(data_sheet.iter_rows(
        min_row=header_row_num,
        max_row=header_row_num,
        values_only=True,
    ))
    if not data_header_row or not data_header_row[0]:
        raise TemplateError(
            f"'data' sheet has no header row at row {header_row_num} "
            f"(derived from skip_rows={skip_rows})."
        )

    # Only non-fixed columns should appear in the data sheet header
    data_headers_ru = [
        str(v).strip()
        for v in data_header_row[0]
        if v is not None
    ]
    _validate_header_alignment(data_headers_ru, russian_headers_cfg, fixed_russian_names)

    return TemplateConfig(
        skip_rows=skip_rows,
        headers=tech_headers,
        dtypes=dtypes,
        key_columns=frozenset(key_columns),
        fixed_values=fixed_values,
        russian_headers=russian_headers_cfg,
    )