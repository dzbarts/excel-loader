# excel-loader

Берёт Excel, CSV или SQL — проверяет типы, кричит об ошибках и кладёт данные в **GreenPlum** или **ClickHouse**.
Запускается из терминала или через **Apache Airflow DAG**.

---

## Содержание

- [Что умеет](#что-умеет)
- [Структура проекта](#структура-проекта)
- [Архитектура](#архитектура)
- [Быстрый старт — Docker](#быстрый-старт--docker)
- [Быстрый старт — Python API](#быстрый-старт--python-api)
- [Запуск через Airflow](#запуск-через-airflow)
- [Параметры LoaderConfig](#параметры-loaderconfig)
- [Режимы обработки ошибок](#режимы-обработки-ошибок)
- [Отчёт валидации](#отчёт-валидации)
- [DDL-парсер и типы данных](#ddl-парсер-и-типы-данных)
- [Шаблоны ODS (data + klad_config)](#шаблоны-ods)
- [Стратегии экспорта](#стратегии-экспорта)
- [Источники валидации](#источники-валидации)
- [Прямая запись в БД](#прямая-запись-в-бд)
- [Деплой в Airflow](#деплой-в-airflow)
- [Разработка и тесты](#разработка-и-тесты)
- [Исключения](#исключения)

---

## Что умеет

| Функция | |
|---|---|
| Чтение Excel (.xlsx, .xls, .xlsm) | ✅ |
| Чтение CSV / TSV | ✅ |
| Чтение SQL INSERT-файлов | ✅ |
| Валидация типов данных (GP / CH) | ✅ |
| Отчёт об ошибках — логи + опциональный TXT | ✅ |
| Шаблоны ODS (data + klad_config) | ✅ |
| Генерация DDL по данным файла | ✅ |
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
│   └── manual_excel_loader/           # основной пакет
│       ├── readers/
│       │   ├── excel_reader.py        # openpyxl-ридер (.xlsx/.xls/.xlsm)
│       │   ├── csv_reader.py          # csv-ридер (.csv/.tsv)
│       │   └── sql_reader.py          # парсер SQL INSERT-файлов
│       ├── writers/
│       │   ├── base.py                # BaseWriter (ABC) + конфиги
│       │   ├── csv_file.py            # запись в CSV
│       │   ├── sql_file.py            # запись в SQL с батчевыми INSERT
│       │   └── database.py            # PostgresWriter (GP) + ClickHouseWriter
│       ├── _connections.py            # get_gp_conn() / get_ch_client()
│       ├── db_schema.py               # get_table_columns() / table_exists()
│       ├── inferencer.py              # infer_types() — угадывает типы по первым 200 строкам
│       ├── ddl_generator.py           # generate_ddl() — генерирует CREATE TABLE
│       ├── table_manager.py           # prepare() / finalize() — жизненный цикл таблицы
│       ├── db_loader.py               # load_to_db() — потоковая вставка в GP / CH
│       ├── enums.py                   # DatabaseType, ErrorMode, DumpType, TimestampField
│       ├── exceptions.py              # иерархия исключений
│       ├── loader.py                  # главный pipeline: read → validate → write
│       ├── models.py                  # LoaderConfig, LoadResult, CellValidationError
│       ├── result.py                  # Ok / Err — result-тип для валидаторов
│       ├── template.py                # парсер шаблонов klad_config
│       ├── validation_report.py       # форматирование и запись отчёта
│       ├── validator.py               # валидаторы типов GP и CH
│       └── ddl.py                     # DDL-парсер CREATE TABLE → dict[col, type]
├── tests/                             # 15 тест-файлов, покрывают все модули
├── conftest.py                        # добавляет dags/ в sys.path
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── pyproject.toml
└── Makefile
```

---

## Архитектура

Pipeline собирается в `loader.py` из трёх независимых слоёв:

```
read_file()        →      validate_row()       →      writer.write()
    ↓                          ↓                            ↓
SheetData              FileValidationResult          SQL / CSV файл
(headers + rows iter)  (CellValidationError list)
                               ↓
                       validation_report        →   DB
                       (логи + TXT-файл)
                                                →   table_manager.prepare()
                                                         ↓
                                                    db_loader.load_to_db()
                                                         ↓
                                                    table_manager.finalize()
                                                    (append / truncate / backup)
```

**Ридеры** возвращают `SheetData(headers, rows)` — loader не знает, откуда пришли данные.
**Врайтеры** реализуют `BaseWriter.write(headers, rows)` — loader не знает, куда уходят данные.
**Валидаторы** строятся один раз на колонку и вызываются через Result-тип `Ok / Err` — никаких исключений в горячем пути.
**DB-путь** управляется через `table_manager` → `db_loader`. Соединения открываются фабриками из Airflow-коннекторов.

Конфигурация полностью описывается `LoaderConfig`. Все ограничения проверяются в `__post_init__` при создании объекта — pipeline ни о чём не переспрашивает.

---

## Быстрый старт — Docker

Нужен **Docker** и **Docker Compose** v2.

```bash
git clone <repo>
cd excel-loader
make setup
```

`make setup` делает всё сам:
1. Создаёт `.env` из `.env.example`
2. Генерирует `AIRFLOW_FERNET_KEY` и `AIRFLOW_SECRET_KEY`
3. Собирает Docker-образ и поднимает стек
4. Ждёт завершения `airflow-init`

После запуска:

| Сервис | Адрес |
|---|---|
| Airflow UI | http://localhost:8080 |
| ClickHouse HTTP | http://localhost:8123 |
| PostgreSQL | localhost:5432 |

Логин по умолчанию: `admin` / `admin` (меняется в `.env`).

### Пересборка с нуля

```bash
make down
rm .env
make setup
```

### Прочие команды

```bash
make up       # поднять уже собранный стек
make down     # остановить и удалить контейнеры
make restart  # перезапустить сервисы
make logs     # стриминг логов
make ps       # статус контейнеров
```

---

## Быстрый старт — Python API

### Установка

```bash
python3 -m venv .venv
source .venv/bin/activate
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
    show_progress=True,   # tqdm прогресс-бар — только для терминала
)

result = load(config)
print(f"Записано строк: {result.rows_written}")
print(f"Выходной файл:  {result.output_file}")
```

> `show_progress=True` — только в терминале. В Airflow оставьте `False` (по умолчанию) — tqdm засоряет логи воркера.

### Как передать типы данных

`dtypes` принимает три варианта:

```python
# 1. Строка DDL — парсируется автоматически
dtypes = "CREATE TABLE t (id integer, name text, amount decimal(12,2))"

# 2. Словарь вручную
dtypes = {"id": "integer", "name": "text", "amount": "decimal(12,2)"}

# 3. Через parse_ddl() явно
from manual_excel_loader.ddl import parse_ddl
dtypes = parse_ddl(ddl_string, DatabaseType.GREENPLUM)
```

---

## Запуск через Airflow

**Trigger DAG w/ config** → вставить JSON:

```json
{
    "input_file": "/data/uploads/report.xlsx",
    "db_type": "gp",
    "table_name": "sales_data",
    "scheme_name": "raw",
    "export": "truncate_load",
    "validation": "bd",
    "error_mode": "raise",
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
| `validation` | `bd`/`ods_template`/`user_string`/`no_validation` | — | Источник типов для валидации |
| `export` | `truncate_load`/`append`/`via_backup`/`to_sql`/`to_csv` | — | Режим выгрузки |
| `ddl_string` | string | — | DDL-строка (только при `validation=user_string`) |
| `output_dir` | string | — | Каталог для файла (только при `export=to_sql`/`to_csv`) |
| `error_mode` | `raise`/`coerce`/`ignore`/`verify` | — | Как реагировать на ошибки валидации |
| `sheet_name` | string | — | Лист Excel (default: активный) |
| `skip_rows` | integer | — | Пропустить N строк перед заголовком |
| `skip_cols` | integer | — | Пропустить N столбцов слева |
| `batch_size` | integer | — | Размер батча (default: 500) |
| `timestamp` | `write_ts` / `load_dttm` | — | Добавить колонку с временной меткой |
| `wf_load_idn` | string | — | Добавить колонку с именем файла-источника |
| `max_row` | integer | — | Ограничить число строк |
| `delimiter` | string | — | Разделитель CSV (default: `,`) |
| `encoding_input` | string | — | Кодировка входящего файла (default: `utf-8`) |
| `encoding_output` | string | — | Кодировка выходного файла (default: `utf-8`) |
| `is_strip` | boolean | — | Обрезать пробелы в строковых ячейках |
| `notify_email` | string | — | Email для уведомления об ошибках |

> Подключения к БД фиксированы: GP → `conn_updcc`, CH → `conn_updcc_ch`.

### Что происходит внутри DAG

```
validate_params  →  resolve_dtypes  →  load_data  →  report
```

- **validate_params** — проверяет существование файла, расширение, нормализует `db_type` (`"gp"` → `"greenplum"`). Передаёт параметры через XCom.
- **resolve_dtypes** — выясняет, откуда брать типы: из БД, из шаблона, из DDL-строки или никуда не ходить.
- **load_data** — запускает pipeline целиком. При ошибке: уведомляет по email (если задан) и пробрасывает исключение → task failed + retry.
- **report** — логирует итог: строк записано/пропущено, есть ли ошибки.

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
    show_progress: bool = False              # tqdm, только для терминала

    # Отчёт валидации
    validation_report_dir: Path | None = None       # None — только логи
    validation_report_include_values: bool = False  # добавить примеры ячеек в файл
```

Все ограничения (`batch_size > 0`, `skip_rows ≥ 0`, поддерживаемые кодировки и т.д.) проверяются в `__post_init__` при создании объекта.

---

## Режимы обработки ошибок

| `error_mode` | Поведение |
|---|---|
| `IGNORE` | Грузим как есть, без валидации |
| `COERCE` | Валидируем; ошибочные ячейки → NULL, загрузка продолжается |
| `VERIFY` | Только проверка, файл не создаётся; при ошибках — `DataValidationError` |
| `RAISE` | Валидация + запись (ошибки → NULL); при ошибках — `DataValidationError` |

При `VERIFY` и `RAISE` параметр `dtypes` обязателен.

Если запись падает в середине — частично созданный output-файл **удаляется автоматически**. Неполных файлов не остаётся.

Результат валидации **всегда** пишется в логи — независимо от режима.

---

## Отчёт валидации

Ошибки группируются по колонке и типу, строки сжимаются в диапазоны:

```
WARNING  Validation: 54 error(s) in sales.xlsx (2 column(s) affected)
WARNING    [datetime] column sale_date (C) — 52 cell(s), rows: 21–72
WARNING    [integer]  column amount (B)    — 2 cell(s), rows: 5, 12
WARNING  Fix: open sales.xlsx and correct the column(s) listed above
```

### TXT-файл (опционально)

Передайте `validation_report_dir` — при наличии ошибок появится файл `{stem}_validation_{timestamp}.txt`:

```python
config = LoaderConfig(
    ...
    error_mode=ErrorMode.COERCE,
    dtypes={"sale_date": "datetime", "amount": "integer"},
    validation_report_dir=Path("my_file.xlsx").parent,
)

result = load(config)
if result.error_file:
    print(f"Отчёт: {result.error_file}")
```

Содержимое:

```
=== Validation Report: sales.xlsx ===
Generated: 2026-03-09 14:22:01

Result: FAILED — 54 error(s) in 2 column(s)

[datetime]  column sale_date (C)  (52 error(s))
  Rows: 21–72

[integer]  column amount (B)  (2 error(s))
  Rows: 5, 12

Fix hint: open sales.xlsx and correct the cell ranges listed above.
```

По умолчанию значения ячеек **не включаются** — они могут быть чувствительными данными. Чтобы добавить:

```python
validation_report_include_values=True
# → Sample values: "2024-13-45" (C21),  "n/a" (C25),  "" (C40)
```

Если ошибок нет — файл не создаётся, `result.error_file == None`.

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

Парсер работает посимвольным обходом — не ломается на вложенных типах вроде `Nullable(Decimal(10,2))` или `Array(Tuple(Int32, String))`. Понимает SQL-комментарии (`--` и `/* */`), multi-word типы (`timestamp without time zone`, `double precision`), модификаторы (`NOT NULL`, `DEFAULT`, `ENCODING`).

### Поддерживаемые типы GreenPlum

`smallint`, `integer`, `bigint`, `real`, `double precision`, `decimal(P,S)`, `numeric(P,S)`, `text`, `char(N)`, `character(N)`, `varchar(N)`, `character varying(N)`, `date`, `time`, `time without time zone`, `time with time zone`, `timestamp`, `timestamp without time zone`, `timestamp with time zone`, `interval`, `tsrange`, `boolean`, `uuid`, `smallserial`, `serial`, `bigserial`

### Поддерживаемые типы ClickHouse

`Int8`–`Int256`, `UInt8`–`UInt256`, `Float32`, `Float64`, `Decimal(P,S)`, `String`, `FixedString(N)`, `Bool`, `UUID`, `Date`, `Date32`, `DateTime`, `DateTime64(N)`, `DateTime64(N, 'tz')` — `Nullable(X)` разворачивается автоматически.

---

## Шаблоны ODS

Шаблон — Excel-файл с двумя листами: **`data`** (данные) и **`klad_config`** (метаданные).

Если оба листа есть — loader автоматически переключается в режим шаблона. `db_type`, `dtypes` и заголовки берутся из `klad_config`, а не из `LoaderConfig`.

### Структура листа `klad_config`

| Строка | Описание |
|---|---|
| 1 | B1 = адрес первой строки данных на листе `data`, например `A3` |
| 2 | Заголовок — игнорируется |
| 3+ | По одной строке на каждую колонку |

Колонки описания (строки 3+):

| Колонка | Содержимое |
|---|---|
| A | Русское отображаемое имя (должно совпадать с заголовком на листе `data`) |
| B | `table` — значение из строки данных; или адрес ячейки (`A2`) — фиксированное значение |
| C | `true` — ключевое поле (NULL недопустим) |
| D | Техническое EN-имя колонки в выходном SQL/CSV |
| E | Тип данных GP (`integer`, `text`, `timestamp` и т.д.) |

Конец описания — первая пустая строка в колонке A.

---

## Стратегии экспорта

### Файловые режимы (без подключения к БД)

| Режим | Описание |
|---|---|
| `to_sql` | SQL-файл с батчевыми INSERT. Сохраняется в `output_dir`. |
| `to_csv` | CSV-файл. Сохраняется в `output_dir`. |

### DB-режимы

| Режим | GP | CH |
|---|---|---|
| `append` | INSERT; CREATE TABLE если не существует. | Аналогично. |
| `truncate_load` | TRUNCATE + INSERT в одной транзакции. ROLLBACK при ошибке. | Данные копируются во временную таблицу; при ошибке — восстанавливаются обратно. |
| `via_backup` | RENAME → `table_before_YYMMDD_HHMM`, создать новую, загрузить. При ошибке — DROP новой + RENAME back. | Аналогично через `RENAME TABLE`. |

---

## Источники валидации

Параметр `validation` определяет, откуда брать типы колонок.

| Значение | Поведение |
|---|---|
| `bd` | Запросить схему из БД. Если таблицы нет — инференс типов по первым 200 строкам + авто-DDL. |
| `ods_template` | Типы из листа `klad_config`. Если лист не найден — предупреждение + инференс. |
| `user_string` | Парсинг DDL-строки из параметра `ddl_string`. |
| `no_validation` | Пропустить валидацию, грузить как есть. |

---

## Прямая запись в БД

Запись напрямую, без промежуточного файла:

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

**PostgresWriter** экранирует имена таблицы и всех колонок через `psycopg2.sql.Identifier` — даже если в Excel заголовок выглядит как `id; DROP TABLE`, он пройдёт безопасно. При ошибке батча — автоматический `ROLLBACK`.

**ClickHouseWriter** передаёт строки кортежами (не словарями) — меньше накладных расходов. Соединение закрывается в `finally` — нет утечек при долгой работе воркера.

### Через Airflow DAG

При запуске через DAG прямая запись управляется параметрами `export` и `validation`. Соединения создаются автоматически через Airflow-коннекторы — host/port/password вручную не нужны:

```json
{
    "input_file": "/data/uploads/report.xlsx",
    "db_type": "gp",
    "table_name": "sales_data",
    "scheme_name": "raw",
    "export": "truncate_load",
    "validation": "bd"
}
```

---

## Деплой в Airflow

### Docker (рекомендуется)

`make setup` — и готово. Образ из `Dockerfile` в корне репозитория.

Зависимости (`openpyxl`, `python-dateutil`, `tqdm`) устанавливаются при сборке образа — не при каждом старте. Код DAG-ов монтируется через volume `./dags:/opt/airflow/dags` и подхватывается сразу без пересборки.

### Ручной деплой (без Docker)

**pip install не нужен.** Airflow автоматически добавляет `dags/` в `sys.path`.

```bash
cp -r dags/ $AIRFLOW_HOME/dags/
```

Зависимости (`openpyxl`, `python-dateutil`, `tqdm`) должны быть в Python-окружении Airflow. Все они есть в корпоративном Nexus.

---

## Разработка и тесты

### Установка

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Команды

```bash
make test        # все тесты
make test-dag    # только DAG-тесты (не нужен запущенный Airflow)
make coverage    # покрытие с HTML-отчётом в htmlcov/
make lint        # ruff check
make fmt         # ruff format
```

### Что покрывают тесты

DAG-функции (`_validate_params_fn`, `_load_data_fn` и т.д.) вынесены на уровень модуля — именно чтобы их можно было вызвать без поднятия Airflow.

| Файл | Что тестирует |
|---|---|
| `test_validator.py` | Все примитивные валидаторы, get_validator(), граничные значения |
| `test_validation_report.py` | Группировка строк, диапазоны, TXT-файл, логирование |
| `test_loader.py` | Cleanup при ошибке записи, интеграция validation_report_dir |
| `test_inferencer.py` | infer_types() — GP/CH типы, приоритеты, лимит 200 строк |
| `test_ddl_generator.py` | generate_ddl() — структура, Nullable, timestamp_col |
| `test_db_schema.py` | get_table_columns() / table_exists() — маппинг типов, Nullable-unwrap |
| `test_table_manager.py` | prepare() / finalize() — все стратегии GP и CH, откаты |
| `test_db_loader.py` | load_to_db() — батчинг, own vs provided conn, commit/rollback |

### Кодировки

Поддерживаются для `encoding_input` / `encoding_output`:

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
