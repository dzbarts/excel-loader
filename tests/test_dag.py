"""
tests/test_dag.py
=================
Тесты для DAG без поднятия Airflow — мокаем context и зависимости.

Паттерн: импортируем функции-задачи напрямую через TaskDecorator,
вызываем их как обычные Python-функции, передавая нужный context.
"""
from __future__ import annotations

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_context(params: dict) -> dict:
    """Минимальный Airflow context для тестов."""
    return {
        "params": params,
        "task_instance": MagicMock(),
        "run_id": "test_run_id",
    }


BASE_PARAMS = {
    "input_file": "/tmp/test.xlsx",
    "db_type": "gp",
    "table_name": "test_table",
    "scheme_name": "test_schema",
    "dump_type": "sql",
    "error_mode": "raise",
    "dtypes_ddl": "",
    "sheet_name": None,
    "skip_rows": 0,
    "skip_cols": 0,
    "batch_size": 100,
    "timestamp": None,
    "wf_load_idn": None,
    "max_row": None,
    "delimiter": ",",
    "is_strip": False,
    "notify_email": "",
}


# ── validate_params ───────────────────────────────────────────────────────────

class TestValidateParams:
    """Тесты задачи validate_params."""

    def test_missing_input_file_raises(self):
        """Пустой input_file → ValueError."""
        from dags.excel_loader_dag import validate_params

        params = {**BASE_PARAMS, "input_file": ""}
        with pytest.raises(ValueError, match="input_file"):
            validate_params.function(**_make_context(params))

    def test_file_not_found_raises(self, tmp_path):
        """Несуществующий файл → FileNotFoundError."""
        from dags.excel_loader_dag import validate_params

        params = {**BASE_PARAMS, "input_file": str(tmp_path / "missing.xlsx")}
        with pytest.raises(FileNotFoundError):
            validate_params.function(**_make_context(params))

    def test_wrong_extension_raises(self, tmp_path):
        """Неподдерживаемое расширение → ValueError."""
        from dags.excel_loader_dag import validate_params

        f = tmp_path / "data.txt"
        f.write_text("hello")
        params = {**BASE_PARAMS, "input_file": str(f)}
        with pytest.raises(ValueError, match="расширение"):
            validate_params.function(**_make_context(params))

    def test_valid_file_returns_params(self, tmp_path):
        """Валидный файл → возвращает dict с параметрами."""
        from dags.excel_loader_dag import validate_params

        f = tmp_path / "data.xlsx"
        f.write_bytes(b"PK")  # достаточно для exists()
        params = {**BASE_PARAMS, "input_file": str(f)}
        result = validate_params.function(**_make_context(params))
        assert result["input_file"] == str(f)
        assert result["db_type"] == "gp"


# ── load_excel ────────────────────────────────────────────────────────────────

class TestLoadExcel:
    """Тесты задачи load_excel — мокаем реальный load()."""

    def _mock_result(self, **kwargs):
        result = MagicMock()
        result.output_file = Path("/tmp/output.sql")
        result.error_file = None
        result.rows_written = 42
        result.rows_skipped = 0
        result.has_errors = False
        for k, v in kwargs.items():
            setattr(result, k, v)
        return result

    def test_successful_load_returns_dict(self, tmp_path):
        """Успешная загрузка → возвращает сериализуемый dict."""
        from dags.excel_loader_dag import load_excel

        f = tmp_path / "data.xlsx"
        f.write_bytes(b"PK")
        params = {**BASE_PARAMS, "input_file": str(f)}

        mock_result = self._mock_result()
        with patch("manual_excel_loader.load", return_value=mock_result) as mock_load:
            result = load_excel.function(params, **_make_context(params))

        mock_load.assert_called_once()
        assert result["rows_written"] == 42
        assert result["has_errors"] is False
        assert result["output_file"] == "/tmp/output.sql"

    def test_data_validation_error_reraises(self, tmp_path):
        """DataValidationError пробрасывается наверх (task failed)."""
        from dags.excel_loader_dag import load_excel

        f = tmp_path / "data.xlsx"
        f.write_bytes(b"PK")
        params = {**BASE_PARAMS, "input_file": str(f)}

        from manual_excel_loader.exceptions import DataValidationError
        exc = DataValidationError("bad data", errors=[])

        with patch("manual_excel_loader.load", side_effect=exc):
            with pytest.raises(DataValidationError):
                load_excel.function(params, **_make_context(params))

    def test_file_read_error_reraises(self, tmp_path):
        """FileReadError пробрасывается наверх."""
        from dags.excel_loader_dag import load_excel

        f = tmp_path / "data.xlsx"
        f.write_bytes(b"PK")
        params = {**BASE_PARAMS, "input_file": str(f)}

        from manual_excel_loader.exceptions import FileReadError
        with patch("manual_excel_loader.load", side_effect=FileReadError("oops")):
            with pytest.raises(FileReadError):
                load_excel.function(params, **_make_context(params))

    def test_notify_email_called_on_error(self, tmp_path):
        """При ошибке и notify_email → send_email вызывается."""
        from dags.excel_loader_dag import load_excel
        from manual_excel_loader.exceptions import DataValidationError

        f = tmp_path / "data.xlsx"
        f.write_bytes(b"PK")
        params = {**BASE_PARAMS, "input_file": str(f), "notify_email": "ops@example.com"}
        exc = DataValidationError("bad", errors=[])

        with patch("manual_excel_loader.load", side_effect=exc), \
             patch("dags.excel_loader_dag.send_email") as mock_email:
            with pytest.raises(DataValidationError):
                load_excel.function(params, **_make_context(params))

        mock_email.assert_called_once()
        call_kwargs = mock_email.call_args
        assert "ops@example.com" in str(call_kwargs)

    def test_no_email_when_notify_empty(self, tmp_path):
        """Без notify_email → send_email не вызывается."""
        from dags.excel_loader_dag import load_excel
        from manual_excel_loader.exceptions import DataValidationError

        f = tmp_path / "data.xlsx"
        f.write_bytes(b"PK")
        params = {**BASE_PARAMS, "input_file": str(f), "notify_email": ""}
        exc = DataValidationError("bad", errors=[])

        with patch("manual_excel_loader.load", side_effect=exc), \
             patch("dags.excel_loader_dag.send_email") as mock_email:
            with pytest.raises(DataValidationError):
                load_excel.function(params, **_make_context(params))

        mock_email.assert_not_called()

    def test_xcom_result_is_json_serializable(self, tmp_path):
        """Результат load_excel должен быть JSON-сериализуемым для XCom."""
        import json
        from dags.excel_loader_dag import load_excel

        f = tmp_path / "data.xlsx"
        f.write_bytes(b"PK")
        params = {**BASE_PARAMS, "input_file": str(f)}
        mock_result = self._mock_result()

        with patch("manual_excel_loader.load", return_value=mock_result):
            result = load_excel.function(params, **_make_context(params))

        # Не должно кидать исключение
        serialized = json.dumps(result)
        assert isinstance(serialized, str)


# ── report ────────────────────────────────────────────────────────────────────

class TestReport:
    """Задача report просто логирует — проверяем что не падает."""

    def test_report_runs_without_error(self, caplog):
        from dags.excel_loader_dag import report
        import logging

        result = {
            "output_file": "/tmp/out.sql",
            "error_file": None,
            "rows_written": 100,
            "rows_skipped": 5,
            "has_errors": False,
        }

        with caplog.at_level(logging.INFO):
            report.function(result, **_make_context(BASE_PARAMS))

        assert "100" in caplog.text

    def test_report_warns_on_errors(self, caplog):
        from dags.excel_loader_dag import report
        import logging

        result = {
            "output_file": "/tmp/out.sql",
            "error_file": "/tmp/errors.txt",
            "rows_written": 10,
            "rows_skipped": 2,
            "has_errors": True,
        }

        with caplog.at_level(logging.WARNING):
            report.function(result, **_make_context(BASE_PARAMS))

        assert "ошибки" in caplog.text.lower() or "error" in caplog.text.lower()