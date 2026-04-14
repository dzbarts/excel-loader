# excel-loader

Берёт Excel, CSV или SQL — проверяет типы, кричит об ошибках и кладёт данные в **GreenPlum**, **PostgreSQL** или **ClickHouse**.
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
- [Airflow-коннекторы](#airflow-коннекторы)
- [Исключения](#исключения)

---

## Что умеет

| Функция | |
|---|---|
| Чтение Excel (.xlsx, .xls, .xlsm) | ✅ |
| Чтение CSV / TSV | ✅ |
| Чтение SQL INSERT-файлов | ✅ |
| Чтение файлов с SMB-шары (через pysmb) | ✅ |
| Валидация типов данных (GP / PG / CH) | ✅ |
| Отчёт об ошибках — логи + опциональный TXT | ✅ |
| Шаблоны ODS (data + klad_config) | ✅ |
| Инференс типов по данным файла (авто-DDL) | ✅ |
| Автосоздание таблицы если не существует | ✅ |
| Генерация DDL по данным файла | ✅ |
| Выгрузка в SQL-файл | ✅ |
| Выгрузка в CSV-файл | ✅ |
| Прямая запись в GreenPlum (COPY FROM STDIN) | ✅ |
| Прямая запись в PostgreSQL (COPY FROM STDIN) | ✅ |
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
│       │   ├── sql_reader.py          # парсер SQL INSERT-файлов
│       │   └── headers.py             # общая валидация заголовков (Excel + CSV)
│       ├── writers/
│       │   ├── base.py                # BaseWriter (ABC) + FileWriterConfig
│       │   ├── csv_file.py            # запись в CSV
│       │   ├── sql_file.py            # запись в SQL с батчевыми INSERT
│       │   └── db_writer.py           # DbWriter — потоковая вставка в GP/PG/CH
│       ├── _connections.py            # get_gp_conn() / get_pg_conn() / get_ch_client()
│       ├── db_schema.py               # get_table_columns() / table_exists()
│       ├── inferencer.py              # infer_types() — угадывает типы по первым 200 строкам
│       ├── ddl_generator.py           # generate_ddl() — генерирует CREATE TABLE
│       ├── table_manager.py           # prepare() / finalize() — жизненный цикл таблицы
│       ├── enums.py                   # DatabaseType, ErrorMode, DumpType, TimestampField
│       ├── exceptions.py              # иерархия исключений
│       ├── loader.py                  # главный pipeline: read → validate → write
│       ├── models.py                  # LoaderConfig, LoadResult, CellValidationError
│       ├── result.py                  # Ok / Err — result-тип для валидаторов
│       ├── template.py                # парсер шаблонов klad_config
│       ├── validation_report.py       # форматирование и запись отчёта
│       ├── validator.py               # валидаторы типов GP/PG и CH
│       └── ddl.py                     # DDL-парсер CREATE TABLE → dict[col, type]
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
SheetData              FileValidationResult       IO[str] stream (pipe / file)
(headers + rows iter)  (CellValidationError list)       ↓
                               ↓               file-режим: локальный файл
                       validation_report        DAG-режим: OS pipe → SMB
                       (логи + TXT-файл)
                                                →   table_manager.prepare()
                                                         ↓
                                                    DbWriter.write()
                                                         ↓
                                                    table_manager.finalize()
                                                    (append / truncate / backup)
```

**Ридеры** возвращают `SheetData(headers, rows)` — loader не знает, откуда пришли данные.
**Врайтеры** реализуют `BaseWriter.write(headers, rows)` и пишут в переданный `IO[str]` поток — не открывают файл сами.
**Loader** открывает файл (или использует переданный `output_stream`) и передаёт поток в writer. При ошибке — удаляет неполный файл.
**DAG** создаёт OS pipe: writer пишет в write-end, SMB читает из read-end. Локальный файл не создаётся.
**Валидаторы** строятся один раз на колонку и вызываются через Result-тип `Ok / Err` — никаких исключений в горячем пути.
**DB-путь** управляется через `table_manager` → `DbWriter`. Соединения открываются фабриками из Airflow-коннекторов.

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
pip install -e "."
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
    "smb_host":    "10.50.73.65",
    "remote_name": "SPB99-FSN32",
    "smb_share":   "Disk5$",
    "smb_dir":     "reports/2024",
    "smb_file":    "report.xlsx",
    "db_type":     "greenplum",
    "table_name":  "sales_data",
    "scheme_name": "raw",
    "export":      "truncate_load",
    "validation":  "bd",
    "error_mode":  "raise",
    "timestamp":   "load_dttm"
}
```

Файл скачивается с SMB-шары в память через `pysmb` (коннектор `conn_updcc_smb`) — **монтировать шару в ОС не нужно**. `smb_host` — IP-адрес сервера для TCP-соединения, `remote_name` — NetBIOS-имя сервера для аутентификации.

### Параметры DAG

#### 1. Источник файла

| Параметр | Тип | Обязательный | Описание |
|---|---|---|---|
| `smb_host` | string | ✅ | IP-адрес сервера (TCP-соединение) |
| `remote_name` | string | ✅ | NetBIOS-имя сервера (аутентификация NTLM) |
| `smb_share` | string | ✅ | Имя шары (например: `Disk5$`) |
| `smb_dir` | string | — | Путь к папке внутри шары. Пустое — корень шары. |
| `smb_file` | string | ✅ | Имя файла (`.xlsx`, `.xls`, `.xlsm`, `.csv`, `.tsv`, `.sql`) |

#### 2. Чтение файла

| Параметр | Тип | Обязательный | Описание |
|---|---|---|---|
| `sheet_name` | string | — | Лист Excel (default: активный) |
| `skip_rows` | integer | — | Пропустить N строк перед заголовком |
| `skip_cols` | integer | — | Пропустить N столбцов слева |
| `max_row` | integer | — | Ограничить число строк |
| `delimiter` | string | — | Разделитель CSV (default: `,`) |
| `encoding_input` | string | — | Кодировка входящего файла (default: `utf-8`) |
| `is_strip` | boolean | — | Обрезать пробелы в строковых ячейках |

#### 3. Целевая таблица

| Параметр | Тип | Обязательный | Описание |
|---|---|---|---|
| `db_type` | `greenplum` / `postgres` / `clickhouse` | ✅ | Целевая БД |
| `table_name` | string | — | Имя таблицы (default: `table_name`) |
| `scheme_name` | string | — | Схема / база (default: `scheme_name`) |

#### 4. Загрузка

| Параметр | Тип | Обязательный | Описание |
|---|---|---|---|
| `export` | `truncate_load`/`append`/`via_backup`/`to_sql`/`to_csv` | — | Режим выгрузки |
| `batch_size` | integer | — | Размер батча для CH (default: 10000); для GP/PG игнорируется — используется COPY |
| `timestamp` | `none`/`write_ts`/`load_dttm` | — | Добавить колонку с временной меткой |
| `wf_load_idn` | string | — | Добавить колонку с идентификатором потока загрузки |

#### 5. Валидация

| Параметр | Тип | Обязательный | Описание |
|---|---|---|---|
| `validation` | `bd`/`ods_template`/`user_string`/`none` | — | Источник типов для валидации |
| `ddl_string` | string | — | DDL-строка (только при `validation=user_string`) |
| `error_mode` | `raise`/`coerce`/`ignore`/`verify` | — | Реакция на ошибки: `raise` — валидация перед загрузкой, при ошибках отмена; `coerce` — ошибки → NULL; `ignore` — без валидации; `verify` — только проверка |
| `save_validation_report` | boolean | — | Загружать TXT-отчёт об ошибках на шару (default: `false`). При `false` ошибки фиксируются только в логах Airflow |
| `validation_report_include_values` | boolean | — | Включить примеры значений ячеек в TXT-отчёт (default: `false`). Актуально только при `save_validation_report=true` |

#### 6. Вывод (только для `to_sql` / `to_csv`)

| Параметр | Тип | Обязательный | Описание |
|---|---|---|---|
| `output_smb_host` | string | — | IP-адрес сервера для выходного файла. По умолчанию — тот же, что `smb_host`. |
| `output_remote_name` | string | — | NetBIOS-имя сервера для выходного файла. По умолчанию — то же, что `remote_name`. |
| `output_smb_share` | string | — | Шара для выходного файла. По умолчанию — та же, что `smb_share`. |
| `output_smb_dir` | string | — | Папка для выходного файла. По умолчанию — та же папка, где лежит исходный файл. |
| `encoding_output` | string | — | Кодировка выходного файла (default: `utf-8`) |

### Что происходит внутри DAG

```
validate_params  →  resolve_dtypes  →  load_data  →  report
```

- **validate_params** — проверяет обязательные SMB-параметры, расширение файла. Резолвит выходные SMB-параметры (`output_smb_host`, `output_remote_name`, `output_smb_share`, `output_smb_dir`) — если не заданы, подставляет входные значения. Передаёт всё через XCom.
- **resolve_dtypes** — выясняет, откуда брать типы: из БД, из шаблона, из DDL-строки или никуда не ходить. Если таблицы нет — запускает инференс и генерирует DDL для создания.
- **load_data** — запускает pipeline целиком. Для `to_sql`/`to_csv`: SQL/CSV стримится напрямую в SMB через OS pipe без создания локального файла. При ошибке пробрасывает исключение → task failed + retry.
- **report** — логирует итог: строк записано/пропущено, есть ли ошибки.

---

## Параметры LoaderConfig

```python
@dataclass
class LoaderConfig:
    input_file: Path               # путь к файлу (для определения формата и именования вывода)
    db_type: DatabaseType          # GREENPLUM, POSTGRES или CLICKHOUSE
    sheet_name: str | None         # лист Excel (None = активный)
    skip_rows: int = 0             # строк пропустить перед заголовком
    skip_cols: int = 0             # столбцов пропустить слева
    table_name: str = "table_name"
    scheme_name: str = "scheme_name"
    dump_type: DumpType = DumpType.SQL
    error_mode: ErrorMode = ErrorMode.IGNORE
    encoding_input: str = "utf-8"  # только для CSV/TSV/SQL
    encoding_output: str = "utf-8"
    batch_size: int = 10000        # для ClickHouse; GP/PG используют COPY FROM STDIN
    delimiter: str = ","
    timestamp: TimestampField | None = None   # write_ts или load_dttm
    max_row: int | None = None
    wf_load_idn: str | None = None
    is_strip: bool = False
    set_empty_str_to_null: bool = True
    dtypes: dict[str, str] | None = None     # col → type, из parse_ddl()
    show_progress: bool = False              # tqdm, только для терминала

    # Содержимое файла в памяти (bytes). Если задано — ридеры читают из него,
    # не обращаясь к диску. input_file при этом используется только для
    # определения формата по расширению и для именования выходного файла.
    input_stream: bytes | None = None

    # Директория для выходного SQL/CSV файла (локальный режим).
    # None (по умолчанию) — файл создаётся рядом с input_file.
    output_dir: Path | None = None

    # Готовый текстовый поток для записи SQL/CSV.
    # Если задан — load() пишет в него напрямую, локальный файл не создаётся,
    # LoadResult.output_file будет None. Используется DAG'ом для стриминга в SMB.
    output_stream: IO[str] | None = None

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
| `VERIFY` | Только проверка без записи; при ошибках — `DataValidationError` |
| `RAISE` | Сначала полная валидация: если чисто — пишем/грузим; если ошибки — `DataValidationError`, файл/таблица не трогается |

При `VERIFY` и `RAISE` параметр `dtypes` обязателен.

Если запись падает в середине — частично созданный output-файл **удаляется автоматически**. Неполных файлов не остаётся.

Результат валидации **всегда** пишется в логи — независимо от режима.

> **NULL в данных** — пустые ячейки (в том числе состоящие только из пробелов) конвертируются в NULL до валидации. Тип для NULL не проверяется. NULL в ключевом поле (из шаблона `klad_config`) — алерт в отчёте, загрузка продолжается.

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

--- Warnings ---

[text]  column key_id (A)  (3 warning(s))
  Rows: 5, 12, 30
```

Warnings (например, NULL в ключевых колонках) выводятся отдельной секцией и не влияют на статус `Result`.

По умолчанию значения ячеек **не включаются** — они могут быть чувствительными данными. Чтобы добавить:

```python
validation_report_include_values=True
# → Sample values: "2024-13-45" (C21),  "n/a" (C25),  "" (C40)
```

Если ошибок нет — файл не создаётся, `result.error_file == None`.

**В Airflow DAG** TXT-файл на шару загружается только при `save_validation_report=true` (по умолчанию `false`). При `false` все ошибки по-прежнему видны в логах таски — файл просто не пишется.

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

### Поддерживаемые типы GreenPlum / PostgreSQL

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
| `to_sql` | SQL-файл с батчевыми INSERT. При запуске через DAG — стримится напрямую в SMB через OS pipe (локальный файл не создаётся). При вызове через Python API — сохраняется в `output_dir`. |
| `to_csv` | CSV-файл. Аналогично: DAG → SMB-стриминг; Python API → `output_dir`. |

### DB-режимы

Во всех трёх режимах: если таблица не существует — она создаётся автоматически по инференсу типов.

| Режим | GreenPlum / PostgreSQL | ClickHouse |
|---|---|---|
| `append` | INSERT; CREATE TABLE если не существует. | Аналогично. |
| `truncate_load` | TRUNCATE + INSERT в одной транзакции. ROLLBACK при ошибке. | Данные копируются во временную таблицу; при ошибке — восстанавливаются обратно. |
| `via_backup` | RENAME → `table_before_YYMMDD_HHMM`, создать новую, загрузить. При ошибке — DROP новой + RENAME back. | Аналогично через `RENAME TABLE`. |

---

## Источники валидации

Параметр `validation` определяет, откуда брать типы колонок.

| Значение | Поведение |
|---|---|
| `bd` | Запросить схему из БД. Если таблицы нет — инференс типов по первым 200 строкам + авто-DDL + создание таблицы. |
| `ods_template` | Типы из листа `klad_config` (только GP + Excel). Если лист не найден — предупреждение + инференс. |
| `user_string` | Парсинг DDL-строки из параметра `ddl_string`. |
| `none` | Пропустить валидацию, грузить как есть. |

### Инференс типов

Анализируются первые 200 непустых строк каждой колонки. Приоритет (от специфичного к общему):

| Python-тип | GreenPlum / PostgreSQL | ClickHouse |
|---|---|---|
| `str` | `text` | `String` |
| `datetime` | `timestamp` | `DateTime` |
| `date` | `date` | `Date32` |
| `time` | `time` | `String` (нет нативного типа) |
| `bool` | `boolean` | `Bool` |
| `float` | `decimal(18,6)` | `Float64` |
| `int` | `bigint` | `Int64` |
| пустая колонка | `text` | `String` |

---

## Прямая запись в БД

Запись напрямую в БД через `DbWriter`. Соединение открывается автоматически из Airflow-коннекторов:

```python
from manual_excel_loader.writers.db_writer import DbWriter, DbWriterConfig
from manual_excel_loader.enums import DatabaseType

writer = DbWriter(DbWriterConfig(
    db_type=DatabaseType.GREENPLUM,
    scheme_name="my_schema",
    table_name="my_table",
    batch_size=10000,
))
rows_written = writer.write(headers=["id", "name"], rows=[(1, "Alice"), (2, "Bob")])
```

**GP/PG** использует `COPY FROM STDIN` через `cur.copy_expert` — один сетевой roundtrip на весь поток данных. При ошибке — автоматический `ROLLBACK`.

**ClickHouse** передаёт строки кортежами батчами нативного протокола.

При `truncate_load` передайте открытое соединение из `table_manager.prepare()` через `conn` / `client` — это сохраняет транзакцию между TRUNCATE и INSERT:

```python
from manual_excel_loader.table_manager import prepare, finalize

ctx = prepare(scheme, table, db_type, "truncate_load", create_ddl)
writer = DbWriter(DbWriterConfig(
    db_type=db_type,
    scheme_name=scheme,
    table_name=table,
    conn=ctx.get("conn"),
    client=ctx.get("client"),
))
try:
    rows_written = writer.write(headers, rows)
    finalize(ctx, success=True)
except Exception:
    finalize(ctx, success=False)
    raise
```

### Через Airflow DAG

При запуске через DAG прямая запись управляется параметрами `export` и `validation`. Соединения создаются автоматически через Airflow-коннекторы — host/port/password вручную не нужны:

```json
{
    "smb_host":    "10.50.73.65",
    "remote_name": "SPB99-FSN32",
    "smb_share":   "Disk5$",
    "smb_dir":     "uploads",
    "smb_file":    "report.xlsx",
    "db_type":     "greenplum",
    "table_name":  "sales_data",
    "scheme_name": "raw",
    "export":      "truncate_load",
    "validation":  "bd"
}
```

---

## Деплой в Airflow

### Docker (рекомендуется)

`make setup` — и готово. Образ из `Dockerfile` в корне репозитория.

Зависимости (`openpyxl`, `python-dateutil`, `tqdm`, `pysmb`) устанавливаются при сборке образа — не при каждом старте. Код DAG-ов монтируется через volume `./dags:/opt/airflow/dags` и подхватывается сразу без пересборки.

### Ручной деплой (без Docker)

**pip install не нужен.** Airflow автоматически добавляет `dags/` в `sys.path`.

```bash
cp -r dags/ $AIRFLOW_HOME/dags/
```

Зависимости (`openpyxl`, `python-dateutil`, `tqdm`, `pysmb`) должны быть в Python-окружении Airflow. Все они есть в корпоративном Nexus.

### Airflow-коннекторы

DAG использует четыре коннектора. Создайте их через Admin → Connections:

| Conn Id | Тип | Назначение |
|---|---|---|
| `conn_updcc` | `postgres` | GreenPlum |
| `conn_updcc_pg` | `postgres` | PostgreSQL |
| `conn_updcc_ch` | `generic` | ClickHouse (host/port/login/password) |
| `conn_updcc_smb` | `generic` | SMB-шара (login/password, домен `gazprom-neft`) |

---

## Кодировки

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
