"""
tests/test_dag.py
=================

Тесты DAG без поднятия Airflow.

Паттерн: импортируем module-level функции (_validate_params_fn, _load_excel_fn,
_report_fn) напрямую и вызываем как обычные Python-функции с мокнутым context.

Почему функции на уровне модуля, а не внутри @dag:
- @task внутри @dag создаёт локальные переменные, недоступные снаружи
- module-level функции импортируются и тестируются без запуска Airflow
- сам DAG просто оборачивает их в task() при регистрации DAG
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import openpyxl
import pytest


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_context(params: dict | None = None) -> dict:
    """Минимальный Airflow context для тестов.

    Для _validate_params_fn (читает context["params"]) — передаём params.
    Для _load_excel_fn (получает params позиционно) — не передаём,
    иначе "got multiple values for argument 'params'".
    """
    ctx: dict = {"task_instance": MagicMock(), "run_id": "test_run_id"}
    if params is not None:
        ctx["params"] = params
    return ctx


# db_type здесь "gp" — _validate_params_fn должен нормализовать в "greenplum"
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
    "encoding_input": "utf-8",
    "encoding_output": "utf-8",
    "is_strip": False,
    "notify_email": "",
}


def _make_load_result(**overrides):
    """Создаёт реальный LoadResult (не MagicMock) с нужными полями."""
    from manual_excel_loader.models import LoadResult
    defaults = dict(
        rows_written=42,
        rows_skipped=0,
        output_file=Path("/tmp/output.sql"),
        error_file=None,
        has_errors=False,
        validation_result=None,
    )
    defaults.update(overrides)
    return LoadResult(**defaults)


# ── validate_params ───────────────────────────────────────────────────────────

class TestValidateParams:
    def test_missing_input_file_raises(self):
        """Пустой input_file → ValueError."""
        from dags.excel_loader_dag import _validate_params_fn
        params = {**BASE_PARAMS, "input_file": ""}
        with pytest.raises(ValueError, match="input_file"):
            _validate_params_fn(**_make_context(params))

    def test_file_not_found_raises(self, tmp_path):
        """Несуществующий файл → FileNotFoundError."""
        from dags.excel_loader_dag import _validate_params_fn
        params = {**BASE_PARAMS, "input_file": str(tmp_path / "missing.xlsx")}
        with pytest.raises(FileNotFoundError):
            _validate_params_fn(**_make_context(params))

    def test_wrong_extension_raises(self, tmp_path):
        """Неподдерживаемое расширение → ValueError."""
        from dags.excel_loader_dag import _validate_params_fn
        f = tmp_path / "data.parquet"
        f.write_bytes(b"PAR1")
        params = {**BASE_PARAMS, "input_file": str(f)}
        with pytest.raises(ValueError, match="расширение"):
            _validate_params_fn(**_make_context(params))

    def test_valid_file_returns_params(self, tmp_path):
        """Валидный файл → возвращает dict с нормализованным db_type."""
        from dags.excel_loader_dag import _validate_params_fn
        f = tmp_path / "data.xlsx"
        f.write_bytes(b"PK")
        params = {**BASE_PARAMS, "input_file": str(f), "db_type": "gp"}
        result = _validate_params_fn(**_make_context(params))
        assert result["input_file"] == str(f)
        # "gp" должен быть нормализован в "greenplum" в validate_params
        assert result["db_type"] == "greenplum"

    def test_db_type_ch_normalised(self, tmp_path):
        """Алиас 'ch' → 'clickhouse'."""
        from dags.excel_loader_dag import _validate_params_fn
        f = tmp_path / "data.xlsx"
        f.write_bytes(b"PK")
        params = {**BASE_PARAMS, "input_file": str(f), "db_type": "ch"}
        result = _validate_params_fn(**_make_context(params))
        assert result["db_type"] == "clickhouse"

    def test_csv_extension_allowed(self, tmp_path):
        """CSV-файлы тоже должны пройти валидацию."""
        from dags.excel_loader_dag import _validate_params_fn
        f = tmp_path / "data.csv"
        f.write_text("id\n1\n")
        params = {**BASE_PARAMS, "input_file": str(f)}
        result = _validate_params_fn(**_make_context(params))
        assert result["input_file"] == str(f)


# ── load_excel ────────────────────────────────────────────────────────────────

class TestLoadExcel:
    def test_successful_load_returns_dict(self, tmp_path):
        """Успешная загрузка → возвращает сериализуемый dict."""
        from dags.excel_loader_dag import _load_excel_fn
        f = tmp_path / "data.xlsx"
        f.write_bytes(b"PK")
        params = {**BASE_PARAMS, "input_file": str(f), "db_type": "greenplum"}

        with patch("manual_excel_loader.load", return_value=_make_load_result()) as mock_load:
            result = _load_excel_fn(params, **_make_context())

        mock_load.assert_called_once()
        assert result["rows_written"] == 42
        assert result["has_errors"] is False
        assert result["output_file"] == "/tmp/output.sql"
        assert result["rows_skipped"] == 0

    def test_result_dict_has_all_expected_keys(self, tmp_path):
        """Все поля, которые ожидает _report_fn, должны присутствовать."""
        from dags.excel_loader_dag import _load_excel_fn
        f = tmp_path / "data.xlsx"
        f.write_bytes(b"PK")
        params = {**BASE_PARAMS, "input_file": str(f), "db_type": "greenplum"}

        with patch("manual_excel_loader.load", return_value=_make_load_result()):
            result = _load_excel_fn(params, **_make_context())

        for key in ("output_file", "error_file", "rows_written", "rows_skipped", "has_errors"):
            assert key in result, f"Missing key: {key}"

    def test_data_validation_error_reraises(self, tmp_path):
        """DataValidationError пробрасывается наверх (task failed)."""
        from dags.excel_loader_dag import _load_excel_fn
        from manual_excel_loader.exceptions import DataValidationError
        from manual_excel_loader.models import FileValidationResult

        f = tmp_path / "data.xlsx"
        f.write_bytes(b"PK")
        params = {**BASE_PARAMS, "input_file": str(f), "db_type": "greenplum"}

        with patch(
            "manual_excel_loader.load",
            side_effect=DataValidationError("bad", validation_result=FileValidationResult())
        ):
            with pytest.raises(DataValidationError):
                _load_excel_fn(params, **_make_context())

    def test_file_read_error_reraises(self, tmp_path):
        """FileReadError пробрасывается наверх."""
        from dags.excel_loader_dag import _load_excel_fn
        from manual_excel_loader.exceptions import FileReadError

        f = tmp_path / "data.xlsx"
        f.write_bytes(b"PK")
        params = {**BASE_PARAMS, "input_file": str(f), "db_type": "greenplum"}

        with patch("manual_excel_loader.load", side_effect=FileReadError("oops")):
            with pytest.raises(FileReadError):
                _load_excel_fn(params, **_make_context())

    def test_notify_email_called_on_error(self, tmp_path):
        """При ошибке и notify_email → send_email вызывается."""
        from dags.excel_loader_dag import _load_excel_fn
        from manual_excel_loader.exceptions import DataValidationError
        from manual_excel_loader.models import FileValidationResult

        f = tmp_path / "data.xlsx"
        f.write_bytes(b"PK")
        params = {**BASE_PARAMS, "input_file": str(f), "db_type": "greenplum", "notify_email": "ops@example.com"}

        with patch(
            "manual_excel_loader.load",
            side_effect=DataValidationError("bad", validation_result=FileValidationResult())
        ), patch("dags.excel_loader_dag.send_email") as mock_email:
            with pytest.raises(DataValidationError):
                _load_excel_fn(params, **_make_context())
        mock_email.assert_called_once()
        assert "ops@example.com" in str(mock_email.call_args)

    def test_no_email_when_notify_empty(self, tmp_path):
        """Без notify_email → send_email не вызывается."""
        from dags.excel_loader_dag import _load_excel_fn
        from manual_excel_loader.exceptions import DataValidationError
        from manual_excel_loader.models import FileValidationResult

        f = tmp_path / "data.xlsx"
        f.write_bytes(b"PK")
        params = {**BASE_PARAMS, "input_file": str(f), "db_type": "greenplum", "notify_email": ""}

        with patch(
            "manual_excel_loader.load",
            side_effect=DataValidationError("bad", validation_result=FileValidationResult())
        ), patch("dags.excel_loader_dag.send_email") as mock_email:
            with pytest.raises(DataValidationError):
                _load_excel_fn(params, **_make_context())
        mock_email.assert_not_called()

    def test_xcom_result_is_json_serializable(self, tmp_path):
        """Результат должен быть JSON-сериализуемым для XCom."""
        from dags.excel_loader_dag import _load_excel_fn

        f = tmp_path / "data.xlsx"
        f.write_bytes(b"PK")
        params = {**BASE_PARAMS, "input_file": str(f), "db_type": "greenplum"}

        with patch("manual_excel_loader.load", return_value=_make_load_result()):
            result = _load_excel_fn(params, **_make_context())
        assert isinstance(json.dumps(result), str)

    def test_pipeline_without_mock(self, tmp_path):
        """Интеграционный тест: реальный load() без мока.

        Проверяет что контракт между load() и DAG не расходится.
        Файл намеренно простой — нам нужен проход через весь pipeline.
        """
        from dags.excel_loader_dag import _load_excel_fn

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["id", "name"])
        ws.append([1, "Alice"])
        ws.append([2, "Bob"])
        f = tmp_path / "real.xlsx"
        wb.save(f)

        params = {
            **BASE_PARAMS,
            "input_file": str(f),
            "db_type": "greenplum",
            "error_mode": "ignore",
            "dtypes_ddl": "",
        }

        result = _load_excel_fn(params, **_make_context())

        assert result["rows_written"] == 2
        assert result["has_errors"] is False
        assert result["output_file"] is not None
        assert Path(result["output_file"]).exists()


# ── report ────────────────────────────────────────────────────────────────────

class TestReport:
    def test_report_runs_without_error(self, caplog):
        """Задача не падает и логирует строк."""
        from dags.excel_loader_dag import _report_fn
        result = {
            "output_file": "/tmp/out.sql",
            "error_file": None,
            "rows_written": 100,
            "rows_skipped": 5,
            "has_errors": False,
        }
        with caplog.at_level(logging.INFO):
            _report_fn(result, **_make_context())
        assert "100" in caplog.text

    def test_report_warns_on_errors(self, caplog):
        """has_errors=True → WARNING в логах."""
        from dags.excel_loader_dag import _report_fn
        result = {
            "output_file": "/tmp/out.sql",
            "error_file": "/tmp/errors.txt",
            "rows_written": 10,
            "rows_skipped": 2,
            "has_errors": True,
        }
        with caplog.at_level(logging.WARNING):
            _report_fn(result, **_make_context())
        assert any(
            "ошибки" in r.message.lower() or "error" in r.message.lower()
            for r in caplog.records
        )