# excel-loader: архитектура инструмента

## Назначение

Airflow DAG и Python-пакет для загрузки табличных файлов (Excel, CSV, SQL INSERT)
в GreenPlum, PostgreSQL или ClickHouse. Источник файлов — SMB-шара корпоративной
сети; файл скачивается в память и обрабатывается потоково, без сохранения на диск.

---

## Точка входа: DAG

`dags/excel_loader_dag.py` — параметризованный Airflow DAG.
Пользователь заполняет форму в UI: адрес SMB-шары, имя файла, целевая таблица,
стратегия экспорта, источник типов для валидации.

DAG выполняет три шага:
1. Скачать файл с SMB-шары в `bytes` (через `pysmb`).
2. Собрать `LoaderConfig` из параметров запуска.
3. Вызвать `table_manager.prepare()` → `load_rows()` → `DbWriter.write()` → `table_manager.finalize()`.

---

## Пакет: `manual_excel_loader`

```
manual_excel_loader/
├── loader.py          — публичный API: load() / load_rows()
├── models.py          — LoaderConfig, LoadResult, CellValidationError
├── enums.py           — DatabaseType, ErrorMode, DumpType, TimestampField
├── readers/
│   ├── excel_reader.py — читает .xlsx/.xls/.xlsm через openpyxl (read_only)
│   ├── csv_reader.py   — читает CSV/TSV через csv.reader
│   ├── sql_reader.py   — парсит SQL INSERT-файлы
│   └── headers.py     — нормализация и валидация заголовков
├── writers/
│   ├── db_writer.py   — COPY FROM STDIN (GP/PG) / batched INSERT (CH)
│   ├── sql_file.py    — пишет SQL INSERT-файл
│   └── csv_file.py    — пишет CSV-файл
├── validator.py       — валидаторы ячеек по типам GP и CH
├── table_manager.py   — управление жизненным циклом таблицы (prepare/finalize)
├── template.py        — парсер Excel-шаблонов (листы data + klad_config)
├── ddl.py / ddl_generator.py — парсинг DDL → dtypes, генерация DDL из данных
├── inferencer.py      — инференс типов колонок по содержимому
└── _connections.py    — Airflow-коннекторы к GP, PG, CH
```

---

## Потоковая обработка (streaming)

Ключевая идея: файл никогда не загружается целиком в память как список строк.
Все три ридера возвращают `SheetData(headers, rows: Iterator[tuple])` —
**генератор**, который читает по одной строке по требованию.

```
Файл (.xlsx / .csv)
  ↓  openpyxl read_only / csv.reader
Reader (yield строк)
  ↓
_iter_rows() в loader.py
  ├── _apply_row_transforms()   — strip, empty→None
  ├── _validate_row()           — проверка типов ячеек
  ├── _insert_fixed_values()    — фиксированные значения шаблона
  └── _append_extra_columns()   — timestamp, wf_load_idn
  ↓
Writer
  ├── DbWriter  — COPY FROM STDIN (GP/PG) или батчевый INSERT (CH)
  ├── SqlFileWriter — пишет .sql-файл
  └── CsvFileWriter — пишет .csv-файл
```

В любой момент времени в памяти находится один батч строк (`batch_size`, по умолчанию 10 000).

Если файл передан как `bytes` (из SMB) — ридеры читают из `io.BytesIO(stream)`,
не обращаясь к диску. `input_file` используется только для определения формата
по расширению и именования выходного файла.

---

## Публичный API loader.py

**`load(config: LoaderConfig) → LoadResult`**
Полный pipeline: читает файл → валидирует → пишет SQL/CSV на диск или в поток.
При любой ошибке во время записи частично созданный файл удаляется.

**`load_rows(config) → (headers, Iterator, FileValidationResult)`**
Только чтение и валидация; возвращает итератор для прямой передачи в `DbWriter`.
Используется DAG'ом при загрузке в БД.

---

## Управление таблицей: `table_manager.py`

Три стратегии экспорта:

| Стратегия | GP/PG | ClickHouse |
|---|---|---|
| `append` | INSERT в существующую, создать если нет | аналогично |
| `truncate_load` | TRUNCATE внутри транзакции; откат при ошибке | TRUNCATE + псевдооткат через `*_temp` |
| `via_backup` | RENAME оригинал → `*_before_YYMMDD_HHMM`, создать новую; при ошибке — вернуть backup | аналогично через RENAME |

`prepare()` открывает соединение и возвращает context-словарь.
`finalize(context, success)` коммитит или откатывает, закрывает соединение.
Открытое соединение передаётся в `DbWriter`, чтобы TRUNCATE и INSERT были в одной транзакции.

---

## Валидация данных: `validator.py`

Режимы (`ErrorMode`):
- `IGNORE` — без валидации, строки пишутся как есть.
- `COERCE` — ошибочные ячейки → NULL, запись продолжается.
- `VERIFY` — только проверка без записи; при ошибках поднимает `DataValidationError`.
- `RAISE` — ошибочные ячейки → NULL, при ошибках исключение после записи.

Валидаторы строятся один раз по `dtypes` (dict col→type_str) перед итерацией строк,
затем вызываются на каждую ячейку. Поддерживаются типы GP (smallint..bigserial,
real, double precision, decimal(P,S), varchar(N), text, date, time, timestamp,
interval, boolean, uuid) и CH (Int8..Int256, UInt*, Float32/64, String, DateTime,
Date, Date32, DateTime64, FixedString, Bool, UUID).

Источник `dtypes`: запрос типов из целевой таблицы БД, DDL-строка от пользователя,
лист `klad_config` Excel-шаблона, или автоматический инференс из данных (`inferencer.py`).

---

## Excel-шаблоны: `template.py`

Специальный формат файла с двумя листами:
- `data` — данные с русскими заголовками в строке.
- `klad_config` — метатаблица: русское имя → техническое EN-имя, тип GP,
  признак ключевой колонки, фиксированное значение (берётся из конкретной ячейки
  листа `data`, а не из строк).

`is_template()` проверяет наличие обоих листов. `read_template_config()` разбирает
`klad_config` и возвращает `TemplateConfig` с `headers`, `dtypes`, `key_columns`,
`fixed_values`. Русские заголовки используются только для проверки соответствия
листа `data` конфигу; в выходной SQL/CSV идут технические EN-имена.

---

## Поток данных: полный цикл (DAG → БД)

```
Airflow UI (параметры)
  ↓
DAG task: скачать файл с SMB → bytes
  ↓
table_manager.prepare()  — подготовить таблицу (TRUNCATE / RENAME / создать)
  ↓
load_rows(LoaderConfig)  — вернуть (headers, Iterator[tuple], validation_result)
  ↓
DbWriter.write(headers, rows)
  ├── GP/PG: COPY "{scheme}"."{table}" (...) FROM STDIN WITH (FORMAT CSV, NULL "")
  │          строки сериализуются в CSV побайтово через _CsvStream → cur.copy_expert()
  └── CH:    client.execute("INSERT INTO ... VALUES", batch)  батчами по batch_size
  ↓
table_manager.finalize(context, success=True/False)  — commit или rollback
```
