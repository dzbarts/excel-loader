from __future__ import annotations

import re
from pathlib import Path

import openpyxl
import pytest

from manual_excel_loader.loader import load
from manual_excel_loader.models import LoaderConfig, LoadResult
from manual_excel_loader.enums import DatabaseType, DumpType, ErrorMode, TimestampField
from manual_excel_loader.exceptions import (
    ConfigurationError,
    DataValidationError,
)

GP = DatabaseType.GREENPLUM
CH = DatabaseType.CLICKHOUSE


# ── Workbook factories ──────────────────────────────────────────────────────

def make_regular_xlsx(
    tmp_path: Path,
    headers: list[str],
    rows: list[tuple],
    filename: str = "data.xlsx",
) -> Path:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(headers)
    for row in rows:
        ws.append(list(row))
    path = tmp_path / filename
    wb.save(path)
    return path


def make_template_xlsx(
    tmp_path: Path,
    col_defs: list[dict],
    data_rows: list[tuple] | None = None,
    first_data_row: str = "A3",
    fixed_cell_values: dict[str, str] | None = None,
    filename: str = "template.xlsx",
) -> Path:
    """Создать тестовый шаблонный workbook с листами 'data' + 'klad_config'.

    col_defs keys: ru_name, source ("table" or cell addr), is_key, tech_name, dtype
    """
    wb = openpyxl.Workbook()
    ws_data = wb.active
    ws_data.title = "data"

    if fixed_cell_values:
        for addr, val in fixed_cell_values.items():
            ws_data[addr] = val

    row_num = int(re.search(r"\d+", first_data_row).group())
    header_row = row_num - 1
    ru_headers = [d["ru_name"] for d in col_defs if d["source"] == "table"]
    for ci, name in enumerate(ru_headers, 1):
        ws_data.cell(row=header_row, column=ci, value=name)

    if data_rows:
        for ri, data_row in enumerate(data_rows):
            for ci, val in enumerate(data_row, 1):
                ws_data.cell(row=row_num + ri, column=ci, value=val)

    ws_cfg = wb.create_sheet("klad_config")
    ws_cfg.cell(1, 2, first_data_row)
    ws_cfg.cell(2, 1, "Рус. имя")
    for i, d in enumerate(col_defs):
        r = 3 + i
        ws_cfg.cell(r, 1, d.get("ru_name", ""))
        ws_cfg.cell(r, 2, d.get("source", "table"))
        ws_cfg.cell(r, 3, d.get("is_key", ""))
        ws_cfg.cell(r, 4, d.get("tech_name", ""))
        ws_cfg.cell(r, 5, d.get("dtype", "text"))

    path = tmp_path / filename
    wb.save(path)
    return path


def base_config(path: Path, **kwargs) -> LoaderConfig:
    """Минимальный валидный LoaderConfig для GP SQL-выгрузки."""
    return LoaderConfig(
        input_file=path,
        db_type=GP,
        table_name="employees",
        scheme_name="hr",
        **kwargs,
    )


# ── Regular Excel — basic pipeline ──────────────────────────────────────────

class TestRegularExcelBasic:
    def test_returns_load_result(self, tmp_path):
        path = make_regular_xlsx(tmp_path, ["id", "name"], [(1, "Alice")])
        result = load(base_config(path))
        assert isinstance(result, LoadResult)

    def test_rows_written_count(self, tmp_path):
        path = make_regular_xlsx(tmp_path, ["id", "name"], [(1, "A"), (2, "B"), (3, "C")])
        result = load(base_config(path))
        assert result.rows_written == 3

    def test_output_file_created(self, tmp_path):
        path = make_regular_xlsx(tmp_path, ["id", "name"], [(1, "Alice")])
        result = load(base_config(path))
        assert result.output_file is not None
        assert result.output_file.exists()

    def test_sql_output_contains_insert(self, tmp_path):
        path = make_regular_xlsx(tmp_path, ["id", "name"], [(1, "Alice")])
        result = load(base_config(path))
        content = result.output_file.read_text()
        assert "INSERT INTO hr.employees" in content
        assert "id" in content and "name" in content

    def test_csv_output(self, tmp_path):
        path = make_regular_xlsx(tmp_path, ["id", "name"], [(1, "Alice")])
        cfg = base_config(path, dump_type=DumpType.CSV)
        result = load(cfg)
        lines = result.output_file.read_text().splitlines()
        assert lines[0] == "id,name"
        assert "1" in lines[1] and "Alice" in lines[1]

    def test_null_cell_becomes_null_in_sql(self, tmp_path):
        path = make_regular_xlsx(tmp_path, ["id", "val"], [(1, None)])
        result = load(base_config(path))
        assert "NULL" in result.output_file.read_text()

    def test_blank_rows_skipped(self, tmp_path):
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["id", "name"])
        ws.append([1, "Alice"])
        ws.append([None, None])
        ws.append([2, "Bob"])
        path = tmp_path / "blanks.xlsx"
        wb.save(path)
        result = load(base_config(path))
        assert result.rows_written == 2

    def test_original_config_not_mutated(self, tmp_path):
        path = make_regular_xlsx(tmp_path, ["id"], [(1,)])
        cfg = base_config(path)
        original_skip = cfg.skip_rows
        load(cfg)
        assert cfg.skip_rows == original_skip

    def test_has_errors_false_when_no_validation(self, tmp_path):
        """IGNORE mode — has_errors всегда False (валидация не запускалась)."""
        path = make_regular_xlsx(tmp_path, ["id"], [(1,)])
        result = load(base_config(path, error_mode=ErrorMode.IGNORE))
        assert result.has_errors is False

    def test_rows_skipped_count(self, tmp_path):
        """rows_skipped должен отражать число пустых строк в источнике."""
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["id"])
        ws.append([1])
        ws.append([None])  # пустая
        ws.append([2])
        path = tmp_path / "gaps.xlsx"
        wb.save(path)
        result = load(base_config(path))
        # reader уже фильтрует пустые строки — rows_written=2
        assert result.rows_written == 2


# ── Error modes ──────────────────────────────────────────────────────────────

class TestErrorModes:
    def _path_with_bad_data(self, tmp_path: Path) -> Path:
        return make_regular_xlsx(
            tmp_path,
            ["id", "name"],
            [(1, "Alice"), ("NOT_AN_INT", "Bob")]
        )

    def test_ignore_writes_all_rows(self, tmp_path):
        path = self._path_with_bad_data(tmp_path)
        result = load(base_config(path, error_mode=ErrorMode.IGNORE))
        assert result.rows_written == 2

    def test_coerce_writes_null_for_invalid(self, tmp_path):
        path = self._path_with_bad_data(tmp_path)
        result = load(base_config(
            path,
            error_mode=ErrorMode.COERCE,
            dtypes={"id": "integer", "name": "text"},
        ))
        content = result.output_file.read_text()
        assert result.rows_written == 2
        assert "NULL" in content

    def test_coerce_has_errors_true(self, tmp_path):
        path = self._path_with_bad_data(tmp_path)
        result = load(base_config(
            path,
            error_mode=ErrorMode.COERCE,
            dtypes={"id": "integer", "name": "text"},
        ))
        assert result.has_errors is True

    def test_verify_raises_on_errors(self, tmp_path):
        path = self._path_with_bad_data(tmp_path)
        with pytest.raises(DataValidationError) as exc_info:
            load(base_config(
                path,
                error_mode=ErrorMode.VERIFY,
                dtypes={"id": "integer", "name": "text"},
            ))
        assert len(exc_info.value.validation_result.errors) > 0

    def test_verify_no_file_produced(self, tmp_path):
        path = self._path_with_bad_data(tmp_path)
        with pytest.raises(DataValidationError) as exc_info:
            load(base_config(
                path,
                error_mode=ErrorMode.VERIFY,
                dtypes={"id": "integer", "name": "text"},
            ))
        # output_file не должен быть создан
        assert exc_info.value.validation_result is not None

    def test_verify_passes_when_data_clean(self, tmp_path):
        path = make_regular_xlsx(tmp_path, ["id", "name"], [(1, "Alice"), (2, "Bob")])
        result = load(base_config(
            path,
            error_mode=ErrorMode.VERIFY,
            dtypes={"id": "integer", "name": "text"},
        ))
        assert result.rows_written == 2
        assert result.output_file is None  # VERIFY не создаёт файл

    def test_raise_writes_file_but_also_raises(self, tmp_path):
        path = self._path_with_bad_data(tmp_path)
        with pytest.raises(DataValidationError):
            load(base_config(
                path,
                error_mode=ErrorMode.RAISE,
                dtypes={"id": "integer", "name": "text"},
            ))

    def test_raise_requires_dtypes(self, tmp_path):
        path = make_regular_xlsx(tmp_path, ["id"], [(1,)])
        with pytest.raises(ConfigurationError, match="dtypes"):
            load(base_config(path, error_mode=ErrorMode.RAISE))

    def test_error_carries_cell_name(self, tmp_path):
        path = self._path_with_bad_data(tmp_path)
        with pytest.raises(DataValidationError) as exc_info:
            load(base_config(
                path,
                error_mode=ErrorMode.VERIFY,
                dtypes={"id": "integer", "name": "text"},
            ))
        errors = exc_info.value.validation_result.errors
        assert re.match(r"[A-Z]+\d+", errors[0].cell_name)


# ── Extra columns ─────────────────────────────────────────────────────────────

class TestExtraColumns:
    def test_timestamp_appended_to_sql(self, tmp_path):
        path = make_regular_xlsx(tmp_path, ["id"], [(1,)])
        result = load(base_config(path, timestamp=TimestampField.LOAD_DTTM))
        assert "load_dttm" in result.output_file.read_text()

    def test_timestamp_value_looks_like_datetime(self, tmp_path):
        path = make_regular_xlsx(tmp_path, ["id"], [(1,)])
        result = load(base_config(path, timestamp=TimestampField.LOAD_DTTM))
        content = result.output_file.read_text()
        assert re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", content)

    def test_wf_load_idn_appended(self, tmp_path):
        path = make_regular_xlsx(tmp_path, ["id"], [(1,)])
        result = load(base_config(path, wf_load_idn="source_file"))
        content = result.output_file.read_text()
        assert "wf_load_idn" in content
        assert path.name in content

    def test_existing_timestamp_col_not_duplicated(self, tmp_path):
        """Если Excel уже содержит load_dttm — не добавлять второй раз."""
        path = make_regular_xlsx(
            tmp_path,
            ["id", "load_dttm"],
            [(1, "2024-01-01 00:00:00")]
        )
        result = load(base_config(path, timestamp=TimestampField.LOAD_DTTM))
        content = result.output_file.read_text()
        header_line = content.splitlines()[0]
        assert header_line.count("load_dttm") == 1


# ── Config validation ─────────────────────────────────────────────────────────

class TestConfigValidation:
    def test_unsupported_encoding_output_raises(self, tmp_path):
        path = make_regular_xlsx(tmp_path, ["id"], [(1,)])
        with pytest.raises(ConfigurationError, match="encoding"):
            load(base_config(path, encoding_output="not-a-real-encoding"))

    def test_unsupported_encoding_input_csv_raises(self, tmp_path):
        """encoding_input проверяется для CSV, но не для Excel."""
        import csv
        csv_path = tmp_path / "data.csv"
        with open(csv_path, "w") as f:
            writer = csv.writer(f)
            writer.writerow(["id"])
            writer.writerow([1])
        with pytest.raises(ConfigurationError, match="encoding"):
            load(base_config(csv_path, encoding_input="not-a-real-encoding"))

    def test_unsupported_file_format_raises(self, tmp_path):
        path = tmp_path / "data.json"
        path.write_text("{}")
        with pytest.raises(ConfigurationError, match="format"):
            load(base_config(path))

    def test_encoding_input_not_validated_for_excel(self, tmp_path):
        """Для Excel encoding_input не проверяется — он не используется."""
        path = make_regular_xlsx(tmp_path, ["id"], [(1,)])
        # Не должно падать даже с невалидной кодировкой для Excel
        result = load(base_config(path, encoding_input="not-a-real-encoding"))
        assert result.rows_written == 1


# ── Template Excel ────────────────────────────────────────────────────────────

class TestTemplateExcel:
    def _simple_template(self, tmp_path: Path) -> Path:
        col_defs = [
            {"ru_name": "ИД", "source": "table", "is_key": "true", "tech_name": "id", "dtype": "integer"},
            {"ru_name": "Имя", "source": "table", "is_key": "", "tech_name": "name", "dtype": "text"},
        ]
        return make_template_xlsx(
            tmp_path, col_defs, data_rows=[(1, "Alice"), (2, "Bob")],
        )

    def test_template_detected_automatically(self, tmp_path):
        path = self._simple_template(tmp_path)
        result = load(base_config(path))
        assert result.rows_written == 2

    def test_template_output_uses_tech_names(self, tmp_path):
        path = self._simple_template(tmp_path)
        result = load(base_config(path))
        content = result.output_file.read_text()
        assert "id" in content and "name" in content
        assert "ИД" not in content and "Имя" not in content

    def test_fixed_value_inserted_in_every_row(self, tmp_path):
        col_defs = [
            {"ru_name": "Система", "source": "A1", "is_key": "", "tech_name": "source_system", "dtype": "text"},
            {"ru_name": "Имя", "source": "table", "is_key": "", "tech_name": "name", "dtype": "text"},
        ]
        path = make_template_xlsx(
            tmp_path, col_defs,
            data_rows=[("Alice",), ("Bob",)],
            fixed_cell_values={"A1": "ГПН-ИТ"},
        )
        result = load(base_config(path))
        content = result.output_file.read_text()
        assert content.count("ГПН-ИТ") == 2

    def test_template_skip_rows_overrides_user(self, tmp_path):
        """skip_rows из шаблона должен иметь приоритет над пользовательским."""
        path = self._simple_template(tmp_path)
        result = load(base_config(path, skip_rows=99))
        assert result.rows_written == 2

    def test_original_config_skip_rows_not_mutated(self, tmp_path):
        """dataclasses.replace() — конфиг вызывающего кода не должен меняться."""
        path = self._simple_template(tmp_path)
        cfg = base_config(path, skip_rows=0)
        load(cfg)
        assert cfg.skip_rows == 0

    def test_key_column_null_recorded_as_error(self, tmp_path):
        col_defs = [
            {"ru_name": "ИД", "source": "table", "is_key": "true", "tech_name": "id", "dtype": "integer"},
            {"ru_name": "Имя", "source": "table", "is_key": "", "tech_name": "name", "dtype": "text"},
        ]
        path = make_template_xlsx(tmp_path, col_defs, data_rows=[(None, "Alice")])
        with pytest.raises(DataValidationError) as exc_info:
            load(base_config(path, error_mode=ErrorMode.VERIFY))
        errors = exc_info.value.validation_result.errors
        assert any("key" in e.message.lower() for e in errors)