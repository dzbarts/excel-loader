"""
Генератор двух PDF-документов для excel-loader.

PDF 1: Полное описание проекта.
PDF 2: Анализ эволюции от start_script.py до текущей реализации.

Запуск:
    .venv/bin/python intro/_make_pdfs.py
"""
from __future__ import annotations

import sys
from pathlib import Path

try:
    from fpdf import FPDF
except ImportError:
    sys.exit("Установите fpdf2: pip install fpdf2")

# ── Fonts ──────────────────────────────────────────────────────────────────────

FONT_DIR = Path("/usr/share/fonts/truetype/dejavu")
FONT_REG  = FONT_DIR / "DejaVuSans.ttf"
FONT_BOLD = FONT_DIR / "DejaVuSans-Bold.ttf"
FONT_MONO = FONT_DIR / "DejaVuSansMono.ttf"

OUT_DIR = Path(__file__).parent


# ── Builder ────────────────────────────────────────────────────────────────────

class Doc(FPDF):
    """FPDF subclass with convenience helpers."""

    MARGIN = 20
    LINE_H = 6
    CODE_LINE_H = 5

    def __init__(self):
        super().__init__()
        self.add_font("dj",   "",  str(FONT_REG))
        self.add_font("dj",   "B", str(FONT_BOLD))
        self.add_font("djm",  "",  str(FONT_MONO))
        self.set_margins(self.MARGIN, self.MARGIN, self.MARGIN)
        self.set_auto_page_break(auto=True, margin=self.MARGIN)

    # ── overrides ──────────────────────────────────────────────────────────────

    def header(self):
        self.set_font("dj", "", 8)
        self.set_text_color(150)
        self.cell(0, 8, self._doc_title, align="R")
        self.ln(2)
        self.set_draw_color(200)
        self.line(self.MARGIN, self.get_y(), self.w - self.MARGIN, self.get_y())
        self.ln(3)
        self.set_text_color(0)

    def footer(self):
        self.set_y(-15)
        self.set_font("dj", "", 8)
        self.set_text_color(150)
        self.cell(0, 10, f"{self.page_no()}", align="C")
        self.set_text_color(0)

    # ── public helpers ─────────────────────────────────────────────────────────

    def start(self, doc_title: str):
        self._doc_title = doc_title
        self.add_page()

    def title_page(self, title: str, subtitle: str = ""):
        self.set_y(80)
        self.set_font("dj", "B", 22)
        self.multi_cell(0, 12, title, align="C")
        if subtitle:
            self.ln(4)
            self.set_font("dj", "", 12)
            self.set_text_color(80)
            self.multi_cell(0, 8, subtitle, align="C")
            self.set_text_color(0)
        self.ln(8)
        self.set_font("dj", "", 10)
        self.set_text_color(120)
        self.cell(0, 6, "excel-loader  ·  2026", align="C")
        self.set_text_color(0)
        self.add_page()

    def h1(self, text: str):
        self.ln(4)
        self.set_font("dj", "B", 15)
        self.set_fill_color(245, 245, 245)
        self.set_draw_color(200)
        self.rect(self.MARGIN, self.get_y(), self.w - 2 * self.MARGIN, 9, "FD")
        self.set_xy(self.MARGIN + 3, self.get_y() + 1)
        self.cell(0, 7, text)
        self.ln(11)

    def h2(self, text: str):
        self.ln(3)
        self.set_font("dj", "B", 12)
        self.cell(0, 7, text)
        self.ln(1)
        self.set_draw_color(210)
        self.line(self.MARGIN, self.get_y(), self.MARGIN + 80, self.get_y())
        self.ln(5)

    def h3(self, text: str):
        self.ln(2)
        self.set_font("dj", "B", 10)
        self.cell(0, 6, text)
        self.ln(7)

    def body(self, text: str):
        self.set_font("dj", "", 10)
        self.multi_cell(0, self.LINE_H, text)
        self.ln(2)

    def bullet(self, items: list[str], indent: int = 5):
        self.set_font("dj", "", 10)
        for item in items:
            self.set_x(self.MARGIN + indent)
            self.cell(4, self.LINE_H, "–")
            self.set_x(self.MARGIN + indent + 4)
            self.multi_cell(self.w - 2 * self.MARGIN - indent - 4, self.LINE_H, item)
        self.ln(2)

    def code(self, text: str):
        self.set_font("djm", "", 8)
        self.set_fill_color(248, 248, 248)
        self.set_draw_color(220)
        lines = text.strip().splitlines()
        x0 = self.MARGIN
        w  = self.w - 2 * self.MARGIN
        h  = len(lines) * self.CODE_LINE_H + 4
        self.rect(x0, self.get_y(), w, h, "FD")
        self.set_x(x0 + 2)
        self.ln(2)
        for line in lines:
            self.set_x(x0 + 3)
            self.cell(w - 4, self.CODE_LINE_H, line)
            self.ln(self.CODE_LINE_H)
        self.ln(3)

    def table(self, headers: list[str], rows: list[list[str]],
              col_widths: list[float] | None = None):
        """Simple bordered table."""
        usable = self.w - 2 * self.MARGIN
        if col_widths is None:
            cw = usable / len(headers)
            col_widths = [cw] * len(headers)

        # header row
        self.set_font("dj", "B", 9)
        self.set_fill_color(235, 235, 235)
        self.set_draw_color(180)
        self.set_x(self.MARGIN)
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 7, h, border=1, fill=True, align="C")
        self.ln()

        # data rows
        self.set_font("dj", "", 9)
        self.set_fill_color(255, 255, 255)
        for row in rows:
            # check page break
            if self.get_y() + 6 > self.h - self.MARGIN:
                self.add_page()
                # re-draw header
                self.set_font("dj", "B", 9)
                self.set_fill_color(235, 235, 235)
                self.set_x(self.MARGIN)
                for i, hh in enumerate(headers):
                    self.cell(col_widths[i], 7, hh, border=1, fill=True, align="C")
                self.ln()
                self.set_font("dj", "", 9)
                self.set_fill_color(255, 255, 255)

            self.set_x(self.MARGIN)
            y_before = self.get_y()
            for i, cell_val in enumerate(row):
                self.set_xy(self.MARGIN + sum(col_widths[:i]), y_before)
                self.multi_cell(col_widths[i], 6, str(cell_val), border=1)
            # advance by the tallest cell — use the y after last multi_cell as approx
            new_y = self.get_y()
            if new_y == y_before:          # all cells fit in one line
                self.set_y(y_before + 6)
            # else: y was already advanced by multi_cell

        self.ln(3)


# ═══════════════════════════════════════════════════════════════════════════════
# PDF 1 — Описание проекта
# ═══════════════════════════════════════════════════════════════════════════════

def build_project_doc() -> Doc:
    doc = Doc()
    doc.start("excel-loader — описание проекта")

    # ── Title ──────────────────────────────────────────────────────────────────
    doc.title_page(
        "excel-loader",
        "Загрузчик Excel / CSV / SQL → GreenPlum / ClickHouse\nс валидацией данных и поддержкой Airflow"
    )

    # ── 1. Назначение ──────────────────────────────────────────────────────────
    doc.h1("1. Назначение")
    doc.body(
        "excel-loader — инструмент для загрузки табличных данных из Excel-файлов, CSV, TSV и SQL INSERT-дампов "
        "в базы данных GreenPlum и ClickHouse. Поддерживает два режима запуска: "
        "Python API для ручного использования и Apache Airflow DAG для автоматизации."
    )
    doc.body(
        "Основные задачи: чтение и нормализация данных из разных форматов, "
        "построчная валидация типов относительно схемы целевой БД, "
        "формирование SQL/CSV-файлов или прямая запись в БД, "
        "отчётность об ошибках."
    )

    # ── 2. Архитектура ─────────────────────────────────────────────────────────
    doc.h1("2. Архитектура")
    doc.body(
        "Pipeline строится в loader.py и состоит из трёх независимых слоёв:"
    )
    doc.bullet([
        "Ридеры (readers/) — читают файл, возвращают SheetData(headers, rows). "
        "Loader не знает, откуда пришли данные.",
        "Валидатор (validator.py) — чистые функции, построенные один раз на колонку при старте. "
        "Горячий путь не бросает исключений: каждая ячейка возвращает Ok(value) или Err(reason).",
        "Врайтеры (writers/) — реализуют BaseWriter.write(headers, rows). "
        "Loader не знает, куда уходят данные.",
    ])
    doc.body("Путь записи в БД управляется отдельно:")
    doc.code(
        "table_manager.prepare()\n"
        "    ↓\n"
        "load_rows(config)   →   db_loader.load_to_db()\n"
        "    ↓\n"
        "table_manager.finalize()"
    )
    doc.body(
        "Конфигурация полностью описывается LoaderConfig. "
        "Все ограничения проверяются в __post_init__ при создании объекта — "
        "на этапе выполнения pipeline не нужно их дублировать."
    )

    # ── 3. Структура проекта ───────────────────────────────────────────────────
    doc.h1("3. Структура проекта")
    doc.code(
        "excel-loader/\n"
        "├── dags/\n"
        "│   ├── excel_loader_dag.py        # Airflow DAG\n"
        "│   └── manual_excel_loader/       # основной пакет\n"
        "│       ├── readers/\n"
        "│       │   ├── excel_reader.py    # openpyxl-ридер\n"
        "│       │   ├── csv_reader.py      # csv/tsv-ридер\n"
        "│       │   └── sql_reader.py      # парсер SQL INSERT\n"
        "│       ├── writers/\n"
        "│       │   ├── base.py            # BaseWriter (ABC)\n"
        "│       │   ├── csv_file.py        # запись в CSV\n"
        "│       │   ├── sql_file.py        # запись в SQL\n"
        "│       │   └── database.py        # PostgresWriter, ClickHouseWriter\n"
        "│       ├── loader.py              # главный pipeline\n"
        "│       ├── validator.py           # валидаторы типов GP / CH\n"
        "│       ├── ddl.py                 # DDL-парсер\n"
        "│       ├── models.py              # LoaderConfig, LoadResult\n"
        "│       ├── enums.py               # DatabaseType, ErrorMode, DumpType\n"
        "│       ├── inferencer.py          # инференс типов по данным\n"
        "│       ├── ddl_generator.py       # генерация CREATE TABLE\n"
        "│       ├── db_schema.py           # запрос схемы из БД\n"
        "│       ├── table_manager.py       # управление таблицей\n"
        "│       ├── db_loader.py           # потоковая вставка строк\n"
        "│       ├── template.py            # парсер ODS-шаблонов\n"
        "│       ├── validation_report.py   # форматирование отчёта\n"
        "│       ├── result.py              # Ok / Err result-тип\n"
        "│       ├── exceptions.py          # иерархия исключений\n"
        "│       └── _connections.py        # фабрики подключений\n"
        "├── tests/                         # 15 тест-файлов\n"
        "├── conftest.py\n"
        "├── Dockerfile\n"
        "├── docker-compose.yml\n"
        "├── Makefile\n"
        "└── pyproject.toml"
    )

    # ── 4. Модули ──────────────────────────────────────────────────────────────
    doc.h1("4. Модули и их функции")

    doc.h2("4.1  loader.py  — главный pipeline")
    doc.body(
        "Единственный модуль, который знает обо всех остальных. Оркестрирует чтение, "
        "валидацию и запись. Содержит два публичных входа:"
    )
    doc.bullet([
        "load(config) — полный pipeline: определить формат → прочитать → валидировать → записать SQL/CSV-файл.",
        "load_rows(config) — только чтение и валидация; возвращает (headers, rows_iter, validation_result) "
        "для прямой загрузки в БД.",
    ])
    doc.body("Внутренние функции:")
    doc.bullet([
        "_resolve_reader() — определяет формат файла, автоматически обнаруживает ODS-шаблоны.",
        "_build_validators() — строит словарь col → callable один раз, до цикла по строкам.",
        "_validate_row() — валидирует одну строку, возвращает (скорректированный) кортеж.",
        "_apply_row_transforms() — is_strip, set_empty_str_to_null.",
        "_insert_fixed_values() — вставляет фиксированные значения шаблона на нужные позиции.",
        "_append_extra_columns() — добавляет timestamp и wf_load_idn.",
        "_wrap_with_progress() — опционально оборачивает итератор в tqdm.",
        "_make_cell_name() — вычисляет Excel-адрес ячейки (A1-нотация) для сообщений об ошибках.",
    ])

    doc.h2("4.2  validator.py  — валидаторы типов")
    doc.body(
        "Два словаря _GP_VALIDATORS и _CH_VALIDATORS, построенных один раз при импорте модуля. "
        "Ключ — строка типа (нижний регистр), значение — callable(Any) → CellResult."
    )
    doc.body("Примитивные функции-валидаторы:")
    doc.bullet([
        "validate_integer(value, min_val, max_val) — приводит к int, проверяет диапазон.",
        "validate_float(value, min_val, max_val) — приводит к float.",
        "validate_decimal(value, precision, scale) — использует decimal.Decimal для корректной обработки "
        "научной нотации и NaN/Inf. Проверяет количество цифр до и после запятой.",
        "validate_string(value) — всегда Ok(str(value)).",
        "validate_datetime(value, min_dt, max_dt, fmt) — принимает datetime, date, time "
        "или строку (через dateutil.parser). Проверяет диапазон дат.",
        "validate_time(value) — парсит время, возвращает HH:MM:SS.",
        "validate_boolean_gp(value) — принимает: true/false/0/1/t/f/y/n/yes/no/on/off/null.",
        "validate_uuid(value) — regex-проверка UUID-формата.",
        "validate_interval(value) — regex-проверка GP INTERVAL (напр. '1 year 2 months').",
    ])
    doc.body("Публичный интерфейс — get_validator(type_name, db_type):")
    doc.bullet([
        "Exact match по словарю.",
        "Regex: decimal(P,S) / numeric(P,S).",
        "Regex: varchar(N) / character varying(N).",
        "Regex: char(N) / character(N) — ровно N символов.",
        "Regex (только CH): FixedString(N) — N байт в UTF-8.",
        "Regex (только CH): DateTime64(precision) / DateTime64(precision, 'tz').",
    ])
    doc.body(
        "Если тип не распознан — бросает UnsupportedDataTypeError с перечнем поддерживаемых типов."
    )

    doc.h2("4.3  ddl.py  — DDL-парсер")
    doc.body(
        "parse_ddl(ddl, db_type) → dict[str, str]. "
        "Парсит CREATE TABLE, возвращает {col_name: type_str}. "
        "Использует посимвольный обход для корректной обработки вложенных скобок "
        "(Decimal(10,2), Nullable(String), Array(Tuple(Int32,String)))."
    )
    doc.bullet([
        "GreenPlum: поддерживает multi-word типы (timestamp without time zone, double precision, "
        "character varying), модификаторы NOT NULL, DEFAULT, ENCODING, STORAGE, DISTRIBUTED.",
        "ClickHouse: раскрывает Nullable(X) → X, обрезает модификаторы DEFAULT, CODEC, COMMENT.",
        "Убирает SQL-комментарии (-- и /* */).",
        "Разбивает колонки по верхнеуровневым запятым (не внутри вложенных скобок).",
    ])

    doc.h2("4.4  readers/")
    doc.bullet([
        "excel_reader.py — читает .xlsx/.xls/.xlsm через openpyxl. "
        "Возвращает SheetData(headers, rows). Валидирует заголовки: не пустые, "
        "только [A-Za-z0-9_], без дубликатов.",
        "csv_reader.py — потоковое чтение CSV/TSV. iter_csv() — ленивый генератор строк "
        "(не грузит весь файл в память).",
        "sql_reader.py — парсер SQL INSERT-файлов. Поддерживает батчевые и одиночные INSERT. "
        "Читает имя таблицы из выражения INSERT INTO.",
        "__init__.py — экспортирует read_file() — единую точку входа; "
        "определяет формат по расширению и делегирует нужному ридеру.",
    ])

    doc.h2("4.5  writers/")
    doc.bullet([
        "base.py — BaseWriter (ABC) с абстрактным write(headers, rows). "
        "FileWriterConfig и DbWriterConfig — dataclass-конфиги.",
        "sql_file.py — генерирует батчевые INSERT-файлы. "
        "Для GP: экранирует NULL и одинарные кавычки. "
        "Для CH: добавляет NULL-маркеры в стиле ClickHouse.",
        "csv_file.py — пишет CSV через csv.writer с настраиваемым разделителем.",
        "database.py — PostgresWriter использует psycopg2.sql.Identifier "
        "(защита от SQL-инъекции через заголовки Excel); "
        "при ошибке батча — автоматический ROLLBACK. "
        "ClickHouseWriter передаёт строки кортежами, соединение закрывается в finally.",
    ])

    doc.h2("4.6  inferencer.py  — инференс типов")
    doc.body(
        "infer_types(sheet_data, db_type) → dict[str, str]. "
        "Читает первые 200 непустых значений каждой колонки, "
        "определяет тип по приоритету: str > datetime > date > time > bool > float > int."
    )
    doc.body(
        "Без pandas: использует нативные Python-типы из openpyxl (openpyxl уже "
        "десериализует ячейки в int, float, datetime, bool). "
        "bool проверяется ДО int, так как bool наследует int в Python."
    )
    doc.body(
        "Для CH: time → String (ClickHouse не имеет нативного типа Time). "
        "float → Float64, int → Int64, datetime → DateTime, date → Date32."
    )

    doc.h2("4.7  db_schema.py, table_manager.py, db_loader.py")
    doc.bullet([
        "db_schema.py — get_table_columns(scheme, table, db_type, conn/client) → dict[col, type]. "
        "Запрашивает information_schema (GP) или system.columns (CH). "
        "table_exists() — проверка существования таблицы.",
        "table_manager.py — prepare() настраивает таблицу перед загрузкой: "
        "append (создать если нет), truncate_load (TRUNCATE в транзакции; для CH — через temp-таблицу), "
        "via_backup (RENAME оригинала в backup, создать новую). "
        "finalize() коммитит или откатывает изменения.",
        "db_loader.py — load_to_db() потоково вставляет строки батчами. "
        "Для GP: psycopg2 execute_values. Для CH: clickhouse-driver client.execute. "
        "Принимает опциональное уже открытое соединение — для сохранения транзакции truncate_load.",
    ])

    doc.h2("4.8  template.py  — ODS-шаблоны")
    doc.body(
        "is_template(path) — проверяет наличие листов 'data' и 'klad_config'. "
        "read_template_config(path) → TemplateConfig:"
    )
    doc.bullet([
        "headers — технические EN-имена колонок (колонка D в klad_config).",
        "dtypes — типы GP (колонка E).",
        "skip_rows — адрес первой строки данных из ячейки B1 klad_config.",
        "key_columns — frozenset колонок, помеченных 'true' в колонке C.",
        "fixed_values — dict {col: value} для строк, где колонка B не равна 'table'.",
    ])

    doc.h2("4.9  validation_report.py")
    doc.body(
        "log_validation_result() — выводит ошибки в лог с группировкой по колонке и типу. "
        "Диапазоны строк сжимаются: '5, 6, 7, 8' → '5–8'."
    )
    doc.body(
        "write_report() — сохраняет TXT-файл {stem}_validation_{timestamp}.txt "
        "рядом с входным файлом или в заданной директории. "
        "По умолчанию не включает значения ячеек — "
        "validation_report_include_values=True добавляет строки Sample values."
    )

    doc.h2("4.10  enums.py, models.py, result.py, exceptions.py")
    doc.bullet([
        "enums.py — DatabaseType (GREENPLUM/CLICKHOUSE), ErrorMode (RAISE/COERCE/VERIFY/IGNORE), "
        "DumpType (SQL/CSV), TimestampField (WRITE_TS/LOAD_DTTM). Все наследуют str — совместимость с JSON.",
        "models.py — LoaderConfig (@dataclass с __post_init__-валидацией), "
        "CellValidationError (cell_name, cell_value, expected_type, message, col_name), "
        "FileValidationResult (список ошибок, is_valid), LoadResult (rows_written, output_file и др.).",
        "result.py — Ok[T] и Err — Result-тип для валидаторов. "
        "Позволяет передавать результат без исключений в горячем пути.",
        "exceptions.py — иерархия: ExcelLoaderError → FileReadError, HeaderValidationError, "
        "DataValidationError (несёт .validation_result), ConfigurationError, "
        "UnsupportedDataTypeError, DumpCreationError, TemplateError.",
    ])

    doc.h2("4.11  excel_loader_dag.py  — Airflow DAG")
    doc.body("Задачи DAG:")
    doc.bullet([
        "validate_params — проверяет файл, расширение, нормализует db_type ('gp' → 'greenplum'). "
        "Передаёт нормализованные параметры через XCom.",
        "resolve_dtypes — определяет типы колонок: bd (из БД / инференс), "
        "ods_template (из klad_config), user_string (парсинг DDL), no_validation.",
        "load_data — запускает полный pipeline. При ошибке отправляет email-уведомление.",
        "report — логирует итог: rows_written, rows_skipped, has_errors.",
    ])
    doc.body(
        "Все task-функции (_validate_params_fn, _load_data_fn и т.д.) вынесены на уровень модуля — "
        "это позволяет тестировать их без поднятия Airflow."
    )

    # ── 5. Конфигурация ────────────────────────────────────────────────────────
    doc.h1("5. LoaderConfig — все параметры")
    doc.table(
        ["Параметр", "Тип", "По умолчанию", "Описание"],
        [
            ["input_file",    "Path",              "—",           "Путь к файлу"],
            ["db_type",       "DatabaseType",       "—",           "GREENPLUM / CLICKHOUSE"],
            ["sheet_name",    "str | None",         "None",        "Лист Excel (None = активный)"],
            ["skip_rows",     "int",                "0",           "Строк пропустить перед заголовком"],
            ["skip_cols",     "int",                "0",           "Столбцов пропустить слева"],
            ["table_name",    "str",                "table_name",  "Имя целевой таблицы"],
            ["scheme_name",   "str",                "scheme_name", "Схема / база"],
            ["dump_type",     "DumpType",           "SQL",         "Формат вывода: SQL или CSV"],
            ["error_mode",    "ErrorMode",          "IGNORE",      "Режим обработки ошибок"],
            ["encoding_input","str",                "utf-8",       "Кодировка входящего файла (CSV/SQL)"],
            ["encoding_output","str",               "utf-8",       "Кодировка выходного файла"],
            ["batch_size",    "int",                "500",         "Строк в одном INSERT / батче"],
            ["delimiter",     "str",                ",",           "Разделитель CSV"],
            ["timestamp",     "TimestampField|None","None",        "Добавить колонку с временной меткой"],
            ["max_row",       "int | None",         "None",        "Ограничить число строк"],
            ["wf_load_idn",   "str | None",         "None",        "Добавить колонку с именем файла"],
            ["is_strip",      "bool",               "False",       "Обрезать пробелы в строках"],
            ["set_empty_str_to_null","bool",        "True",        "Пустые строки → NULL"],
            ["dtypes",        "dict|None",          "None",        "Типы колонок из parse_ddl()"],
            ["show_progress", "bool",               "False",       "tqdm прогресс-бар (только CLI)"],
            ["validation_report_dir","Path|None",   "None",        "Директория для TXT-отчёта"],
            ["validation_report_include_values","bool","False",    "Включить примеры значений в отчёт"],
        ],
        col_widths=[45, 38, 30, 57],
    )

    # ── 6. Режимы обработки ошибок ─────────────────────────────────────────────
    doc.h1("6. Режимы обработки ошибок (ErrorMode)")
    doc.table(
        ["Режим", "Поведение"],
        [
            ["IGNORE",  "Загрузка без валидации — данные передаются как есть."],
            ["COERCE",  "Валидация; ошибочные ячейки → NULL, загрузка продолжается."],
            ["VERIFY",  "Только валидация, файл не создаётся; при ошибках — DataValidationError."],
            ["RAISE",   "Валидация + загрузка (ошибки → NULL); при ошибках — DataValidationError."],
        ],
        col_widths=[35, 135],
    )
    doc.body(
        "При VERIFY и RAISE параметр dtypes обязателен. "
        "При любой ошибке записи частично созданный output-файл удаляется автоматически."
    )

    # ── 7. Типы данных ─────────────────────────────────────────────────────────
    doc.h1("7. Поддерживаемые типы данных")
    doc.h2("GreenPlum")
    doc.bullet([
        "Целые: smallint, integer, bigint, smallserial, serial, bigserial",
        "Вещественные: real, double precision",
        "Десятичные: decimal(P,S), numeric(P,S)",
        "Строки: text, char(N), character(N), varchar(N), character varying(N)",
        "Дата/время: date, time, time without time zone, time with time zone, "
        "timestamp, timestamp without time zone, timestamp with time zone, interval",
        "Прочие: boolean, uuid, tsrange",
    ])
    doc.h2("ClickHouse")
    doc.bullet([
        "Знаковые целые: Int8, Int16, Int32, Int64, Int128, Int256",
        "Беззнаковые целые: UInt8, UInt16, UInt32, UInt64, UInt128, UInt256",
        "Вещественные: Float32, Float64",
        "Десятичные: Decimal(P,S)",
        "Строки: String, FixedString(N)",
        "Дата/время: Date, Date32, DateTime, DateTime64(N), DateTime64(N, 'tz')",
        "Прочие: Bool, UUID",
        "Nullable(X) — автоматически разворачивается DDL-парсером",
    ])

    # ── 8. ODS-шаблоны ─────────────────────────────────────────────────────────
    doc.h1("8. Шаблоны ODS (klad_config)")
    doc.body(
        "Файл Excel с двумя листами: data (данные) и klad_config (метаданные). "
        "Наличие обоих листов автоматически переключает loader в режим шаблона — "
        "dtypes, skip_rows и заголовки берутся из klad_config."
    )
    doc.body("Структура листа klad_config:")
    doc.table(
        ["Строка / колонка", "Содержимое"],
        [
            ["B1",              "Адрес первой строки данных на листе data (например, A3)"],
            ["Строка 2",        "Заголовок — игнорируется"],
            ["A (строки 3+)",   "Русское отображаемое имя (совпадает с заголовком на data)"],
            ["B (строки 3+)",   "'table' — данные из строки; или адрес ячейки (A2) — фиксированное значение"],
            ["C (строки 3+)",   "'true' — ключевое поле (NULL недопустим)"],
            ["D (строки 3+)",   "Техническое EN-имя колонки в выходном файле"],
            ["E (строки 3+)",   "Тип данных GP (integer, text, timestamp и т.д.)"],
        ],
        col_widths=[50, 120],
    )

    # ── 9. Стратегии экспорта ──────────────────────────────────────────────────
    doc.h1("9. Стратегии экспорта")
    doc.h2("Файловые режимы")
    doc.bullet([
        "to_sql — батчевые INSERT, файл рядом с входным (или в output_dir).",
        "to_csv — CSV с разделителем delimiter.",
    ])
    doc.h2("DB-режимы")
    doc.table(
        ["Режим", "GreenPlum", "ClickHouse"],
        [
            ["append",        "INSERT; CREATE TABLE если нет",
                              "INSERT; CREATE TABLE если нет"],
            ["truncate_load", "TRUNCATE + INSERT в одной транзакции. Rollback при ошибке.",
                              "Данные копируются во временную таблицу; при ошибке — восстанавливаются."],
            ["via_backup",    "RENAME → table_before_YYMMDD_HHMM. При ошибке — DROP новой + RENAME back.",
                              "RENAME TABLE. При ошибке — аналогичный откат."],
        ],
        col_widths=[35, 82, 53],
    )

    # ── 10. Источники валидации ────────────────────────────────────────────────
    doc.h1("10. Источники валидации (параметр validation)")
    doc.table(
        ["Значение", "Поведение"],
        [
            ["bd",           "Запросить схему таблицы из БД. Если таблицы нет — инференс типов по данным."],
            ["ods_template",  "Типы из листа klad_config. Если лист не найден — предупреждение + инференс."],
            ["user_string",   "Парсинг DDL-строки из параметра ddl_string."],
            ["no_validation", "Пропустить валидацию."],
        ],
        col_widths=[40, 130],
    )

    # ── 11. Необычные особенности ──────────────────────────────────────────────
    doc.h1("11. Необычные и нетривиальные особенности")
    doc.h3("Result-тип вместо исключений в горячем пути")
    doc.body(
        "Каждый валидатор возвращает Ok(value) или Err(message) — никаких try/except в цикле по строкам. "
        "Это снижает накладные расходы при большом числе ошибок и делает поведение предсказуемым: "
        "все ошибки всегда собираются, независимо от режима."
    )
    doc.h3("Валидаторы строятся один раз на колонку")
    doc.body(
        "get_validator() вызывается перед циклом по строкам и возвращает callable. "
        "В горячем пути (на каждую ячейку) происходит только вызов функции — "
        "без поиска по словарям, без regex-сопоставления."
    )
    doc.h3("Посимвольный парсер DDL вместо regex")
    doc.body(
        "Разбивка тела CREATE TABLE на колонки (_split_column_defs) и извлечение "
        "типа (_extract_type_token) работают через отслеживание глубины скобок. "
        "Это позволяет корректно обрабатывать любые вложенные типы (Array(Tuple(Int32, String))) "
        "без сложных regex."
    )
    doc.h3("Транзакционная безопасность truncate_load для CH")
    doc.body(
        "ClickHouse не поддерживает транзакции, поэтому truncate_load реализован через "
        "временную таблицу: данные копируются в table_temp → TRUNCATE оригинала → загрузка новых. "
        "При ошибке — INSERT из table_temp обратно. Это не атомарно, но минимизирует время пустой таблицы."
    )
    doc.h3("Автоудаление неполного output-файла")
    doc.body(
        "Если запись прерывается исключением (нехватка диска, ошибка ридера), "
        "loader автоматически удаляет частично созданный файл. "
        "Пользователь никогда не получит файл с неполными данными."
    )
    doc.h3("Инференс типов без pandas")
    doc.body(
        "inferencer.py работает напрямую с Python-типами из openpyxl — "
        "int, float, datetime, bool. pandas не используется нигде в пакете, "
        "что упрощает деплой в ограниченных корпоративных окружениях."
    )
    doc.h3("bool проверяется до int")
    doc.body(
        "В Python bool является подклассом int: isinstance(True, int) == True. "
        "Поэтому в inferencer.py и во всех типовых проверках bool-флаг проверяется первым, "
        "иначе булевы значения были бы классифицированы как целые числа."
    )
    doc.h3("Защита от SQL-инъекции через заголовки Excel")
    doc.body(
        "PostgresWriter использует psycopg2.sql.Identifier для экранирования имён "
        "таблицы, схемы и всех колонок. Даже если пользователь передаст Excel с заголовком "
        "'id; DROP TABLE', он будет безопасно экранирован."
    )
    doc.h3("Excel-адресация ошибок (A1-нотация)")
    doc.body(
        "_make_cell_name() вычисляет буквенно-цифровой адрес ячейки с учётом skip_rows и skip_cols — "
        "чтобы пользователь мог сразу открыть Excel и перейти к проблемной ячейке."
    )
    doc.h3("Лениво открываемые соединения в db_loader")
    doc.body(
        "load_to_db() принимает уже открытое соединение (для truncate_load, "
        "где транзакцию нельзя разрывать) или None — тогда соединение открывается "
        "и закрывается внутри функции. Это исключает утечку соединений при Airflow-воркерах."
    )

    # ── 12. Исключения ─────────────────────────────────────────────────────────
    doc.h1("12. Иерархия исключений")
    doc.table(
        ["Исключение", "Когда возникает"],
        [
            ["ExcelLoaderError",       "Базовый класс"],
            ["FileReadError",          "Файл не найден или не читается"],
            ["HeaderValidationError",  "Заголовок пустой, содержит недопустимые символы или дубликаты"],
            ["DataValidationError",    "Ячейки не прошли валидацию; несёт .validation_result"],
            ["ConfigurationError",     "Некорректный LoaderConfig или DDL-строка"],
            ["UnsupportedDataTypeError","Тип данных не поддерживается для выбранной БД"],
            ["DumpCreationError",      "Ошибка при записи выходного файла"],
            ["TemplateError",          "Нарушена структура шаблона ODS"],
        ],
        col_widths=[60, 110],
    )

    return doc


# ═══════════════════════════════════════════════════════════════════════════════
# PDF 2 — Анализ эволюции
# ═══════════════════════════════════════════════════════════════════════════════

def build_evolution_doc() -> Doc:
    doc = Doc()
    doc.start("excel-loader — эволюция от start_script.py")

    doc.title_page(
        "От скрипта к пакету:\nэволюция excel-loader",
        "Анализ архитектурных изменений\nот start_script.py до текущей реализации"
    )

    # ── Введение ───────────────────────────────────────────────────────────────
    doc.h1("Введение")
    doc.body(
        "В начале проект существовал в виде одного файла start_script.py — монолитного класса "
        "RowDataProcessor, который объединял чтение, валидацию, конвертацию и запись данных. "
        "Сегодня это полноценный Python-пакет из 20+ модулей с Airflow DAG, "
        "полным покрытием тестами и двумя целевыми БД."
    )
    doc.body(
        "Этот документ разбирает каждое существенное изменение: что было, что стало и почему."
    )

    # ── 1. Что представлял start_script.py ────────────────────────────────────
    doc.h1("1. Что представлял start_script.py")
    doc.h2("Структура")
    doc.body(
        "Один класс RowDataProcessor (~1 000+ строк). Всё в одном месте: "
        "чтение Excel, валидация, генерация SQL/CSV, DDL, конвертация SQL/CSV обратно в Excel."
    )
    doc.body("Основные методы:")
    doc.bullet([
        "process_excel_data() — главный метод: чтение → валидация → запись SQL/CSV. "
        "Принимал десятки параметров напрямую.",
        "sql_to_excel() / csv_to_excel() — обратная конвертация в Excel.",
        "get_ddl() — генерация CREATE TABLE DDL.",
        "Внутренние _validate_*() методы — валидаторы как методы класса.",
    ])
    doc.h2("Зависимости")
    doc.bullet([
        "pandas — для чтения и обработки данных.",
        "openpyxl — для Excel.",
        "tqdm, dateutil, re, pathlib, inspect, functools, warnings.",
    ])
    doc.h2("Известные ограничения на старте")
    doc.bullet([
        "Нет Airflow DAG — только ручной запуск.",
        "Нет прямой записи в БД — только SQL/CSV-файлы.",
        "Нет формального управления транзакциями.",
        "Нет структурированного отчёта об ошибках.",
        "Нет инференса типов по данным.",
        "Один файл — сложно тестировать отдельные части.",
        "pandas — тяжёлая зависимость для корпоративного Nexus.",
    ])

    # ── 2. Ключевые изменения ──────────────────────────────────────────────────
    doc.h1("2. Ключевые изменения и их причины")

    doc.h2("2.1  Монолит → модульная архитектура")
    doc.table(
        ["Было", "Стало"],
        [
            ["Один класс RowDataProcessor",
             "20+ модулей с чёткими зонами ответственности"],
            ["Методы-валидаторы как часть класса",
             "Чистые функции в validator.py, не привязанные к состоянию"],
            ["Чтение и запись смешаны в одном методе",
             "Ридеры и врайтеры — отдельные модули, скрытые за абстракцией"],
        ],
        col_widths=[85, 85],
    )
    doc.body(
        "Причина: монолит невозможно тестировать по частям. "
        "Изменение логики чтения ломало тесты записи. "
        "Модульная структура позволяет изолировать каждый компонент."
    )

    doc.h2("2.2  pandas удалён")
    doc.table(
        ["Было", "Стало"],
        [
            ["import pandas as pd — используется для чтения данных",
             "Только openpyxl + stdlib. pandas нет нигде в пакете."],
        ],
        col_widths=[85, 85],
    )
    doc.body(
        "Причина: pandas — тяжёлая зависимость (~30 MB). "
        "В корпоративных Airflow-окружениях с ограниченным Nexus "
        "каждая зависимость требует одобрения. "
        "openpyxl уже десериализует ячейки в нативные Python-типы — "
        "pandas не давал ничего, что нельзя было сделать без него."
    )

    doc.h2("2.3  Конфигурация: параметры → dataclass LoaderConfig")
    doc.table(
        ["Было", "Стало"],
        [
            ["process_excel_data(filepath, db_type, table_name, ...) — "
             "10+ позиционных параметров",
             "LoaderConfig(@dataclass) — единый объект, "
             "все ограничения в __post_init__"],
        ],
        col_widths=[85, 85],
    )
    doc.body(
        "Причина: длинные сигнатуры методов хрупкие — добавление нового параметра "
        "ломает все вызовы. LoaderConfig расширяется без изменения публичного API. "
        "__post_init__ гарантирует корректность конфига до начала pipeline."
    )

    doc.h2("2.4  Исключения и try/except → Result-тип Ok/Err")
    doc.table(
        ["Было", "Стало"],
        [
            ["Валидаторы бросали исключения или возвращали bool/None",
             "Ok(value) / Err(message) — без исключений в горячем пути"],
        ],
        col_widths=[85, 85],
    )
    doc.body(
        "Причина: исключения дорого обрабатываются в CPython. "
        "При файлах с тысячами ошибок подход на исключениях значительно медленнее. "
        "Result-тип делает поведение функции явным в сигнатуре: "
        "вызывающий код обязан проверить результат."
    )

    doc.h2("2.5  Валидаторы: методы класса → чистые функции + словарь")
    doc.table(
        ["Было", "Стало"],
        [
            ["self._validate_integer(value, type_str) — пересоздаётся на каждый вызов",
             "_GP_VALIDATORS / _CH_VALIDATORS — dict строится один раз при импорте. "
             "get_validator() возвращает готовый callable"],
        ],
        col_widths=[85, 85],
    )
    doc.body(
        "Причина: в оригинале при каждом вызове метода-валидатора парсился тип "
        "(regex для decimal, varchar и т.д.). "
        "Теперь get_validator() вызывается один раз на колонку перед циклом — "
        "в горячем пути только вызов функции."
    )

    doc.h2("2.6  Режимы обработки ошибок стали явными")
    doc.table(
        ["Было", "Стало"],
        [
            ["Параметры типа strict=True/False, продолжать при ошибке или нет — "
             "неформализованно",
             "ErrorMode(RAISE / COERCE / VERIFY / IGNORE) — "
             "четыре явных, задокументированных режима"],
        ],
        col_widths=[85, 85],
    )
    doc.body(
        "Причина: пользователи не понимали, что именно произойдёт при ошибке. "
        "VERIFY (только проверить, не писать файл) был особенно востребован "
        "для предварительной проверки данных."
    )

    doc.h2("2.7  DDL-парсер: отдельный модуль с посимвольным обходом")
    doc.table(
        ["Было", "Стало"],
        [
            ["Упрощённый парсинг DDL внутри метода класса через split() и простые regex",
             "ddl.py — посимвольный обход скобок. "
             "Корректно обрабатывает Nullable(Decimal(10,2)), Array(Tuple(...))"],
        ],
        col_widths=[85, 85],
    )
    doc.body(
        "Причина: regex-подход ломался на вложенных типах ClickHouse. "
        "Посимвольный обход с отслеживанием depth скобок — единственный надёжный способ."
    )

    doc.h2("2.8  Добавлены ридеры для CSV и SQL INSERT")
    doc.table(
        ["Было", "Стало"],
        [
            ["Только Excel. CSV/SQL — через внешние скрипты или вручную",
             "csv_reader.py (потоковый), sql_reader.py (парсер INSERT). "
             "Единый read_file() по расширению."],
        ],
        col_widths=[85, 85],
    )
    doc.body(
        "Причина: данные часто приходили уже в CSV или как SQL-дампы. "
        "Общая точка входа read_file() скрывает формат от loader — "
        "он работает с SheetData независимо от источника."
    )

    doc.h2("2.9  Стратегия врайтеров: BaseWriter + три реализации")
    doc.table(
        ["Было", "Стало"],
        [
            ["Запись SQL/CSV жёстко вшита в process_excel_data()",
             "BaseWriter(ABC) + CsvFileWriter, SqlFileWriter, "
             "PostgresWriter, ClickHouseWriter"],
        ],
        col_widths=[85, 85],
    )
    doc.body(
        "Причина: Strategy Pattern. Добавление нового формата вывода "
        "(например, Parquet) требует только нового класса — "
        "loader.py менять не нужно."
    )

    doc.h2("2.10  Прямая запись в БД")
    doc.table(
        ["Было", "Стало"],
        [
            ["Только SQL/CSV-файлы. Загрузка в БД — отдельный шаг вручную",
             "db_loader.py (потоковая вставка), table_manager.py (truncate/backup), "
             "PostgresWriter, ClickHouseWriter"],
        ],
        col_widths=[85, 85],
    )
    doc.body(
        "Причина: промежуточный файл — лишний шаг и потенциальная точка отказа. "
        "Прямая запись через load_rows() позволяет загружать без "
        "создания файла на диске."
    )

    doc.h2("2.11  Управление транзакциями")
    doc.table(
        ["Было", "Стало"],
        [
            ["Нет управления транзакциями",
             "table_manager.prepare() + finalize(). truncate_load в одной транзакции. "
             "CH: псевдооткат через temp-таблицу."],
        ],
        col_widths=[85, 85],
    )
    doc.body(
        "Причина: TRUNCATE + INSERT без транзакции оставляет таблицу пустой "
        "при ошибке на полпути. "
        "Для ClickHouse без транзакций разработан псевдо-атомарный "
        "механизм через temp-таблицу."
    )

    doc.h2("2.12  Инференс типов")
    doc.table(
        ["Было", "Стало"],
        [
            ["Нет. Типы всегда указывались вручную",
             "inferencer.py — анализирует первые 200 строк, "
             "возвращает dict[col, type] для GP или CH"],
        ],
        col_widths=[85, 85],
    )
    doc.body(
        "Причина: при validation=bd, если таблицы нет в БД, нужно откуда-то взять типы. "
        "Инференс — fallback, который избавляет от необходимости указывать DDL вручную."
    )

    doc.h2("2.13  Генератор DDL по данным")
    doc.table(
        ["Было", "Стало"],
        [
            ["get_ddl() — базовая генерация, без инференса",
             "ddl_generator.py — generate_ddl() по результату inferencer. "
             "Nullable для CH, timestamp_col, порядок колонок."],
        ],
        col_widths=[85, 85],
    )

    doc.h2("2.14  Отчёт валидации")
    doc.table(
        ["Было", "Стало"],
        [
            ["Ошибки выводились как список в print/log",
             "validation_report.py: группировка по колонке + тип, "
             "диапазоны строк, опциональный TXT-файл с примерами значений"],
        ],
        col_widths=[85, 85],
    )
    doc.body(
        "Причина: при тысячах ошибок вывод каждой отдельно нечитаем. "
        "Группировка '52 ошибки в колонке sale_date, строки 21–72' "
        "даёт actionable результат."
    )

    doc.h2("2.15  Поддержка ODS-шаблонов (klad_config)")
    doc.table(
        ["Было", "Стало"],
        [
            ["Частичная поддержка шаблонов внутри RowDataProcessor",
             "template.py: is_template(), read_template_config() — "
             "автодетект, TemplateConfig dataclass, fixed_values, key_columns"],
        ],
        col_widths=[85, 85],
    )
    doc.body(
        "Причина: шаблоны klad_config — корпоративный стандарт для части проектов. "
        "Вынесение логики в отдельный модуль позволяет тестировать её независимо."
    )

    doc.h2("2.16  Airflow DAG")
    doc.table(
        ["Было", "Стало"],
        [
            ["Нет. Только Python API",
             "excel_loader_dag.py: параметризованный DAG с 4 задачами, "
             "XCom для передачи параметров, email-уведомления"],
        ],
        col_widths=[85, 85],
    )
    doc.body(
        "Причина: основной сценарий использования — загрузка данных по расписанию "
        "или по запросу через UI. DAG даёт retry-логику, логирование, "
        "мониторинг и историю запусков из коробки."
    )

    doc.h2("2.17  Тесты")
    doc.table(
        ["Было", "Стало"],
        [
            ["Нет формальных тестов",
             "15 тест-файлов, покрывают все модули. "
             "DAG-функции тестируются без запуска Airflow."],
        ],
        col_widths=[85, 85],
    )
    doc.body(
        "Причина: без тестов каждое изменение требует ручной проверки. "
        "Task-функции DAG вынесены на уровень модуля специально для тестируемости."
    )

    # ── 3. Что осталось неизменным ─────────────────────────────────────────────
    doc.h1("3. Что осталось неизменным")
    doc.bullet([
        "Базовая бизнес-логика: чтение Excel → валидация типов → SQL/CSV.",
        "Поддержка GreenPlum и ClickHouse как целевых БД.",
        "Поддержка ODS-шаблонов с листом klad_config.",
        "Паттерны для validation_interval, validation_decimal — перенесены из start_script.py.",
        "Опция tqdm прогресс-бара для ручного запуска.",
        "Обработка skip_rows / skip_cols.",
        "Нормализация булевых значений (true/false/0/1/yes/no/on/off).",
    ])

    # ── 4. Итог ────────────────────────────────────────────────────────────────
    doc.h1("4. Итог")
    doc.body(
        "Проект прошёл путь от одного рабочего скрипта до продакшн-пакета. "
        "Каждое архитектурное решение было продиктовано конкретной проблемой: "
        "невозможностью тестировать части по отдельности, ломкой при расширении, "
        "ограничениями деплоя или требованиями надёжности."
    )
    doc.body(
        "Основные принципы, которые провели эту эволюцию:"
    )
    doc.bullet([
        "Single Responsibility — каждый модуль делает одно.",
        "Dependency Inversion — loader зависит от абстракций (BaseWriter, SheetData), не от конкретных форматов.",
        "Fail fast, fail clean — partial output удаляется, транзакции откатываются.",
        "Zero surprise — ErrorMode явно описывает поведение при ошибке.",
        "Minimal deps — никаких тяжёлых зависимостей без необходимости.",
    ])

    return doc


# ═══════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    p1 = OUT_DIR / "excel_loader_overview.pdf"
    p2 = OUT_DIR / "excel_loader_evolution.pdf"

    print("Generating project overview PDF…")
    doc1 = build_project_doc()
    doc1.output(str(p1))
    print(f"  → {p1}")

    print("Generating evolution analysis PDF…")
    doc2 = build_evolution_doc()
    doc2.output(str(p2))
    print(f"  → {p2}")

    print("Done.")
