"""
Тест: при ошибке во время записи частично созданный output-файл удаляется.

Почему это важно:
    Если writer упал на середине — файл существует, но неполный.
    Пользователь мог бы считать загрузку успешной и использовать обрезанные данные.
    Loader должен удалять такой файл в блоке except.

Что проверяем:
    1. output-файл создаётся в процессе записи (pre-condition).
    2. После исключения внутри writer.write() файл не существует.
    3. Исключение пробрасывается наверх (loader не глотает ошибку).
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch, MagicMock

import openpyxl
import pytest

from manual_excel_loader.enums import DatabaseType, DumpType, ErrorMode
from manual_excel_loader.models import LoaderConfig


def _make_config(input_file: Path, tmp_path: Path) -> LoaderConfig:
    return LoaderConfig(
        input_file=input_file,
        db_type=DatabaseType.GREENPLUM,
        table_name="t",
        scheme_name="s",
        dump_type=DumpType.SQL,
        error_mode=ErrorMode.IGNORE,
    )


def _make_excel(tmp_path: Path) -> Path:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["id", "name"])
    ws.append([1, "Alice"])
    ws.append([2, "Bob"])
    path = tmp_path / "input.xlsx"
    wb.save(path)
    return path


class TestOutputFileCleanupOnError:
    def test_partial_file_deleted_when_writer_raises(self, tmp_path: Path):
        """Если writer.write() бросает исключение — output-файл должен быть удалён.

        Имитируем сбой внутри writer: подменяем SqlFileWriter.write() так,
        что он создаёт файл (pre-condition: файл существует до исключения),
        затем бросает RuntimeError.
        """
        from manual_excel_loader import load

        excel_path = _make_excel(tmp_path)
        config = _make_config(excel_path, tmp_path)

        captured_output_path: list[Path] = []

        def _failing_write(headers, rows):
            # Имитируем частичную запись: создаём файл, потом падаем
            output_path = captured_output_path[0]
            output_path.write_text("partial content")
            raise RuntimeError("disk full")

        with patch(
            "manual_excel_loader.loader.SqlFileWriter"
        ) as mock_writer_cls:
            # Перехватываем момент создания writer, чтобы узнать output_path
            original_init = mock_writer_cls.side_effect

            def capture_init(writer_config):
                captured_output_path.append(writer_config.output_path)
                mock_instance = MagicMock()
                mock_instance.write.side_effect = _failing_write
                return mock_instance

            mock_writer_cls.side_effect = capture_init

            with pytest.raises(RuntimeError, match="disk full"):
                load(config)

        # output_path был захвачен — проверяем что файл удалён
        assert len(captured_output_path) == 1, "Writer должен быть создан ровно один раз"
        assert not captured_output_path[0].exists(), (
            f"Output-файл должен быть удалён после ошибки, "
            f"но существует: {captured_output_path[0]}"
        )

    def test_exception_is_reraised_after_cleanup(self, tmp_path: Path):
        """Loader не должен глотать исключение — оно пробрасывается наверх."""
        from manual_excel_loader import load

        excel_path = _make_excel(tmp_path)
        config = _make_config(excel_path, tmp_path)

        with patch("manual_excel_loader.loader.SqlFileWriter") as mock_writer_cls:
            mock_instance = MagicMock()
            mock_instance.write.side_effect = OSError("no space left on device")
            mock_writer_cls.return_value = mock_instance

            with pytest.raises(OSError, match="no space left"):
                load(config)

    def test_no_file_created_if_writer_raises_before_writing(self, tmp_path: Path):
        """Если writer упал до создания файла — нет файла, нет ошибки удаления."""
        from manual_excel_loader import load

        excel_path = _make_excel(tmp_path)
        config = _make_config(excel_path, tmp_path)

        with patch("manual_excel_loader.loader.SqlFileWriter") as mock_writer_cls:
            mock_instance = MagicMock()
            # writer.write() бросает сразу, не создавая файл
            mock_instance.write.side_effect = PermissionError("access denied")
            mock_writer_cls.return_value = mock_instance

            with pytest.raises(PermissionError):
                load(config)

        # Файлов в tmp_path не должно быть (кроме input.xlsx)
        output_files = [
            f for f in tmp_path.iterdir()
            if f.suffix in (".sql", ".csv") and f.stem != "input"
        ]
        assert output_files == [], f"Не должно быть output-файлов: {output_files}"