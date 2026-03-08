# excel-loader

Загрузчик Excel / CSV / SQL → **GreenPlum** / **ClickHouse** с валидацией данных.

Запускается как Python API (ручной режим) или через **Apache Airflow DAG** (автоматизированный режим).

---

## Содержание

- [Возможности](#возможности)
- [Структура проекта](#структура-проекта)
- [Архитектура](#архитектура)
- [Быстрый старт — Python API](#быстрый-старт--python-api)
- [Запуск через Airflow](#запуск-через-airflow)
- [Параметры LoaderConfig](#параметры-loaderconfig)
- [Режимы обработки ошибок](#режимы-обработки-ошибок)
- [DDL-парсер и типы данных](#ddl-парсер-и-типы-данных)
- [Шаблоны ODS (data + klad_config)](#шаблоны-ods)
- [Прямая запись в БД](#прямая-запись-в-бд)
- [Деплой в Airflow](#деплой-в-airflow)
- [Разработка и тесты](#разработка-и-тесты)
- [Исключения](#исключения)

---

## Возможности

| Функция | Статус |
|---|---|
| Чтение Excel (.xlsx, .xls, .xlsm) | ✅ |
| Чтение CSV / TSV | ✅ |
| Чтение SQL INSERT-файлов | ✅ |
| Валидация типов данных (GP / CH) | ✅ |
| Поддержка шаблонов ODS (data + klad_config) | ✅ |
| Генерация DDL по Excel | ✅ |
| Выгрузка в SQL-файл | ✅ |
| Выгрузка в CSV-файл | ✅ |
| Прямая запись в GreenPlum | ✅ |
| Прямая запись в ClickHouse | ✅ |
| Airflow DAG с параметрами из UI | ✅ |

---

## Структура проекта

```
excel-loader/
├── dags/                              ← копировать целиком в $AIRFLOW_HOME/dags/
│   ├── excel_loader_dag.py            # Airflow DAG
│   └── manual_excel_loader/           # пакет — pip install не нужен
│       ├── readers/
│       │   ├── excel_reader.py        # openpyxl-ридер (.xlsx/.xls/.xlsm)
│       │   ├── csv_reader.py          # csv-ридер (.csv/.tsv)
│       │   └── sql_reader.py          # парсер SQL INSERT-файлов
│       ├── writers/
│       │   ├── base.py                # BaseWriter (ABC) + FileWriterConfig, DbWriterConfig
│       │   ├── csv_file.py            # запись в CSV-файл
│       │   ├── sql_file.py            # запись в SQL-файл с батчевыми INSERT
│       │   └── database.py            # PostgresWriter (GP) + ClickHouseWriter
│       ├── enums.py                   # DatabaseType, ErrorMode, DumpType, TimestampField
│       ├── exceptions.py              # иерархия исключений
│       ├── loader.py                  # главный pipeline: read → validate → write
│       ├── models.py                  # LoaderConfig, LoadResult, CellValidationError
│       ├── result.py                  # Result-тип: Ok / Err для валидаторов
│       ├── template.py                # парсер шаблонов klad_config
│       ├── validator.py               # валидаторы типов GP и CH
│       └── ddl.py                     # DDL-парсер CREATE TABLE → dict[col, type]
├── tests/
│   ├── test_dag.py
│   ├── test_loader.py
│   ├── test_validator.py
│   ├── test_template.py
│   ├── test_writers.py
│   ├── test_csv_reader.py
│   ├── test_sql_reader.py
│   ├── test_database_writers.py
│   └── test_ddl.py
├── conftest.py                        # добавляет dags/ в sys.path для тестов
├── pyproject.toml
└── Makefile
```

---

## Архитектура

Pipeline строится в `loader.py` и состоит из трёх независимых слоёв:

```
read_file()          →       validate_row()        →       writer.write()
  ↓                              ↓                               ↓
SheetData                  FileValidationResult             SQL / CSV файл
(headers + rows iter)      (CellValidationError list)       или БД напрямую
```

**Ридеры** возвращают единый тип `SheetData(headers, rows)` — `loader` не знает, откуда пришли данные.  
**Врайтеры** реализуют `BaseWriter.write(headers, rows)` — `loader` не знает, куда уходят данные.  
**Валидаторы** построены один раз на колонку при старте и вызываются для каждой ячейки через Result-тип `Ok / Err` — без исключений в горячем пути.

Конфигурация полностью описывается `LoaderConfig`. Все ограничения проверяются в `__post_init__` при создании объекта.

---

## Быстрый старт — Python API

### Установка

```bash
pip install -e ".[dev]"
```

### Минимальный пример

```python
from pathlib import Path
from manual_excel_loader import load
from manual_excel_loader.models import LoaderConfig
from manual_excel_loader.enums import DatabaseType, DumpType, ErrorMode

config = LoaderConfig(
    input_file=Path("my_file.xlsx"),
    db_type=DatabaseType.GREENPLUM,
    table_name="my_table",
    scheme_name="my_schema",
    dump_type=DumpType.SQL,
    error_mode=ErrorMode.RAISE,
    dtypes="CREATE TABLE t (id integer, name text, dt date)",
    timestamp="load_dttm",
    batch_size=500,
    show_progress=True,   # tqdm прогресс-бар — только для ручного запуска
)

result = load(config)
print(f"Записано строк: {result.rows_written}")
print(f"Выходной файл:  {result.output_file}")
```

> `show_progress=True` — только для терминала. При запуске через Airflow оставьте `False` (по умолчанию) — tqdm-вывод засоряет логи воркера.

### Передача типов данных

`dtypes` принимает три формата:

```python
# 1. DDL-строка (парсится автоматически)
dtypes = "CREATE TABLE t (id integer, name text, amount decimal(12,2))"

# 2. dict вручную
dtypes = {"id": "integer", "name": "text", "amount": "decimal(12,2)"}

# 3. Через parse_ddl() явно
from manual_excel_loader.ddl import parse_ddl
dtypes = parse_ddl(ddl_string, DatabaseType.GREENPLUM)
```

---

## Запуск через Airflow

Запуск через **Trigger DAG w/ config** → вставить JSON:

```json
{
    "input_file": "/data/uploads/report.xlsx",
    "db_type": "gp",
    "table_name": "sales_data",
    "scheme_name": "raw",
    "dump_type": "sql",
    "error_mode": "raise",
    "dtypes_ddl": "CREATE TABLE t (id integer, amount decimal(18,2), dt date)",
    "timestamp": "load_dttm",
    "batch_size": 500,
    "notify_email": "team@example.com"
}
```

### Параметры DAG

| Параметр | Тип | Обязательный | Описание |
|---|---|---|---|
| `input_file` | string | ✅ | Абсолютный путь к файлу внутри контейнера |
| `db_type` | `gp` / `ch` | ✅ | Целевая БД |
| `table_name` | string | — | Имя таблицы (default: `table_name`) |
| `scheme_name` | string | — | Схема / база (default: `scheme_name`) |
| `dump_type` | `sql` / `csv` | — | Тип выходного файла (default: `sql`) |
| `error_mode` | `raise`/`coerce`/`ignore`/`verify` | — | Обработка ошибок валидации |
| `dtypes_ddl` | string | — | DDL-строка или пусто |
| `sheet_name` | string | — | Лист Excel (default: активный) |
| `skip_rows` | integer | — | Пропустить N строк перед заголовком |
| `skip_cols` | integer | — | Пропустить N столбцов слева |
| `batch_size` | integer | — | Размер батча SQL (default: 500) |
| `timestamp` | `write_ts` / `load_dttm` | — | Добавить временну́ю метку |
| `wf_load_idn` | string | — | Добавить колонку с именем файла-источника |
| `max_row` | integer | — | Ограничить число строк |
| `delimiter` | string | — | Разделитель CSV (default: `,`) |
| `encoding_input` | string | — | Кодировка входящего файла (default: `utf-8`) |
| `encoding_output` | string | — | Кодировка выходного файла (default: `utf-8`) |
| `is_strip` | boolean | — | Обрезать пробелы в строковых ячейках |
| `notify_email` | string | — | Email для уведомления об ошибках |

### Задачи DAG

```
validate_params  →  load_excel  →  report
```

- **validate_params** — проверяет существование файла, расширение, нормализует `db_type` (`"gp"` → `"greenplum"`). Передаёт params в XCom.
- **load_excel** — запускает полный pipeline. При ошибке: уведомляет по email (если задан), пробрасывает исключение → task failed + retry.
- **report** — логирует итог: строк записано/пропущено, наличие ошибок.

---

## Параметры LoaderConfig

```python
@dataclass
class LoaderConfig:
    input_file: Path               # путь к файлу
    db_type: DatabaseType          # GREENPLUM или CLICKHOUSE
    sheet_name: str | None         # лист Excel (None = активный)
    skip_rows: int = 0             # строк пропустить перед заголовком
    skip_cols: int = 0             # столбцов пропустить слева
    table_name: str = "table_name"
    scheme_name: str = "scheme_name"
    dump_type: DumpType = DumpType.SQL
    error_mode: ErrorMode = ErrorMode.IGNORE
    encoding_input: str = "utf-8"  # только для CSV/TSV/SQL
    encoding_output: str = "utf-8"
    batch_size: int = 500
    delimiter: str = ","
    timestamp: TimestampField | None = None   # write_ts или load_dttm
    max_row: int | None = None
    wf_load_idn: str | None = None
    is_strip: bool = False
    set_empty_str_to_null: bool = True
    dtypes: dict[str, str] | None = None     # col → type, из parse_ddl()
    show_progress: bool = False              # tqdm, только для ручного запуска
```

Все ограничения (batch_size > 0, skip_rows ≥ 0, поддерживаемые кодировки и т.д.) проверяются в `__post_init__` при создании объекта.

---

## Режимы обработки ошибок

| `error_mode` | Поведение |
|---|---|
| `IGNORE` | Загрузка без валидации — данные как есть |
| `COERCE` | Валидация; ошибочные ячейки → NULL, загрузка продолжается |
| `VERIFY` | Только валидация, файл не создаётся; при ошибках — `DataValidationError` |
| `RAISE` | Валидация + загрузка (ошибки → NULL); при ошибках — `DataValidationError` |

При `VERIFY` и `RAISE` параметр `dtypes` обязателен.

При любой ошибке во время записи частично созданный output-файл **удаляется автоматически** — пользователь не получит неполный файл.

---

## DDL-парсер и типы данных

`parse_ddl()` принимает `CREATE TABLE` и возвращает `dict[col_name, type_str]`:

```python
from manual_excel_loader.ddl import parse_ddl
from manual_excel_loader.enums import DatabaseType

ddl = """
CREATE TABLE hr.employees (
    id          integer        NOT NULL,
    full_name   text,
    salary      decimal(12, 2),
    hired_at    date
) DISTRIBUTED BY (id);
"""

types = parse_ddl(ddl, DatabaseType.GREENPLUM)
# → {"id": "integer", "full_name": "text", "salary": "decimal(12,2)", "hired_at": "date"}
```

Поддерживает GreenPlum и ClickHouse. Корректно обрабатывает вложенные типы (`Decimal(10,2)`, `Nullable(String)`, `Array(Tuple(Int32, String))`), SQL-комментарии (`--` и `/* */`), multi-word типы (`timestamp without time zone`, `double precision`, `character varying(N)`), модификаторы (`NOT NULL`, `DEFAULT`, `ENCODING`).

### Поддерживаемые типы GreenPlum

`smallint`, `integer`, `bigint`, `real`, `double precision`, `decimal(P,S)`, `numeric(P,S)`, `text`, `char(N)`, `character(N)`, `varchar(N)`, `character varying(N)`, `date`, `time`, `timestamp`, `timestamp without time zone`, `timestamp with time zone`, `interval`, `tsrange`, `boolean`, `uuid`, `smallserial`, `serial`, `bigserial`

### Поддерживаемые типы ClickHouse

`Int8`–`Int256`, `UInt8`–`UInt256`, `Float32`, `Float64`, `Decimal(P,S)`, `String`, `FixedString(N)`, `Bool`, `UUID`, `Date`, `Date32`, `DateTime`, `DateTime64`, `Time`

---

## Шаблоны ODS

Шаблон — Excel-файл с двумя листами: **`data`** (данные) и **`klad_config`** (метаданные).

Если оба листа присутствуют, loader автоматически переключается в режим шаблона — параметры `db_type`, `dtypes`, `sheet_name` берутся из `klad_config`, а не из `LoaderConfig`.

### Структура листа `klad_config`

| Строка | Описание |
|---|---|
| 1 | B1 = адрес первой строки данных на листе `data`, например `A3` |
| 2 | Заголовок (игнорируется) |
| 3+ | По одной строке на каждую колонку |

Колонки описания (строки 3+):

| Колонка | Содержимое |
|---|---|
| A | Русское отображаемое имя (должно совпадать с заголовком на листе `data`) |
| B | `table` — значение берётся из строк данных; или адрес ячейки (`A2`) — фиксированное значение |
| C | `true` — ключевое поле (NULL недопустим) |
| D | Техническое EN-имя колонки (используется в выходном SQL/CSV) |
| E | Тип данных GP (например, `integer`, `text`, `timestamp`) |

Конец описания — первая пустая строка.

---

## Прямая запись в БД

Запись напрямую в GreenPlum или ClickHouse без промежуточного файла:

```python
from manual_excel_loader.writers.base import DbWriterConfig
from manual_excel_loader.writers.database import PostgresWriter, ClickHouseWriter

# GreenPlum
pg_writer = PostgresWriter(DbWriterConfig(
    host="localhost", port=5432,
    database="de_db", user="admin", password="...",
    table_name="my_table", scheme_name="my_schema",
    batch_size=1000,
))
pg_writer.write(headers=["id", "name"], rows=[(1, "Alice"), (2, "Bob")])

# ClickHouse
ch_writer = ClickHouseWriter(DbWriterConfig(
    host="localhost", port=9000,
    database="de_db", user="default", password="...",
    table_name="my_table", scheme_name="my_db",
    batch_size=5000,
))
ch_writer.write(headers=["id", "name"], rows=[(1, "Alice"), (2, "Bob")])
```

**PostgresWriter** использует `psycopg2.sql.Identifier` для экранирования имён таблицы и колонок — защита от SQL-инъекции через заголовки Excel. При ошибке батча — автоматический `ROLLBACK`, частичные данные в БД не остаются.

**ClickHouseWriter** передаёт строки как кортежи (не словари) — меньше накладных расходов. Соединение закрывается в `finally` — нет утечки соединений при долгой работе Airflow-воркера.

---

## Деплой в Airflow

**pip install не требуется.** Airflow автоматически добавляет `dags/` в `sys.path`.

```bash
cp -r dags/ $AIRFLOW_HOME/dags/
```

Зависимости (`openpyxl`, `python-dateutil`, `tqdm`) должны быть установлены в Python-окружении Airflow. Все они входят в список разрешённых пакетов корпоративного Nexus.

---

## Разработка и тесты

### Установка окружения

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Команды

```bash
make test        # все тесты
make test-dag    # только DAG-тесты (не требуют запущенного Airflow)
make coverage    # покрытие с HTML-отчётом в htmlcov/
make lint        # ruff check
make fmt         # ruff format
```

### Покрытие

Тесты покрывают все модули пакета. DAG-тесты проверяют task-функции напрямую, без поднятия Airflow — `_validate_params_fn`, `_load_excel_fn` и `_report_fn` вынесены на уровень модуля именно для этого.

### Кодировки

Поддерживаемые кодировки для `encoding_input` / `encoding_output`:

`utf-8`, `utf-16`, `utf-16-le`, `utf-16-be`, `ascii`, `latin1`, `cp1252`, `cp1251`, `cp866`, `koi8-r`, `koi8-u`, `iso-8859-5`, `gbk`, `big5`, `shift_jis`, `euc-jp`, `euc-kr`

Для Excel кодировка игнорируется — `openpyxl` читает бинарный XLSX-формат.

---

## Исключения

Все исключения наследуют `ExcelLoaderError`.

| Исключение | Когда |
|---|---|
| `FileReadError` | Файл не найден или не читается |
| `HeaderValidationError` | Заголовок пустой, содержит недопустимые символы или дубликаты |
| `DataValidationError` | Ячейки не прошли валидацию типов; несёт `.validation_result` |
| `ConfigurationError` | Некорректный конфиг или DDL |
| `UnsupportedDataTypeError` | Тип данных не поддерживается для выбранной БД |
| `DumpCreationError` | Ошибка записи выходного файла |
| `TemplateError` | Нарушена структура шаблона ODS |

```python
from manual_excel_loader.exceptions import DataValidationError

try:
    result = load(config)
except DataValidationError as exc:
    for err in exc.validation_result.errors:
        print(f"{err.cell_name}: {err.cell_value!r} — {err.message}")
```