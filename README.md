# excel-loader

Загрузчик Excel/CSV/SQL → GreenPlum / ClickHouse с валидацией данных.  
Запускается как вручную (Python API), так и через Apache Airflow.

---

## Возможности

| Функция | Статус |
|---|---|
| Чтение Excel (xlsx, xlsm) | ✅ |
| Чтение CSV / TSV | ✅ |
| Чтение SQL INSERT-файлов | ✅ |
| Валидация типов данных (GP / CH) | ✅ |
| Поддержка шаблонов ODS (data + klad_config) | ✅ |
| Генерация DDL | ✅ |
| Выгрузка в SQL-файл | ✅ |
| Выгрузка в CSV-файл | ✅ |
| Прямая запись в GreenPlum | ✅ |
| Прямая запись в ClickHouse | ✅ |
| Airflow DAG с параметрами из UI | ✅ |

---

## Структура проекта

```
excel-loader/
├── dags/
│   └── excel_loader_dag.py       # Airflow DAG
├── src/
│   └── manual_excel_loader/
│       ├── readers/
│       │   ├── csv_reader.py     # Чтение CSV/TSV
│       │   └── sql_reader.py     # Чтение SQL INSERT-файлов
│       ├── writers/
│       │   ├── base.py           # Абстрактный Writer
│       │   ├── csv_file.py       # Запись в CSV
│       │   ├── sql_file.py       # Запись в SQL
│       │   └── database.py       # Прямая запись в GP / CH
│       ├── enums.py
│       ├── exceptions.py
│       ├── loader.py             # Основной API: load()
│       ├── models.py             # LoaderConfig, LoadResult
│       ├── reader.py             # Чтение Excel
│       ├── template.py           # Обработка шаблонов ODS
│       ├── validator.py          # Валидация типов данных
│       └── ddl.py                # Генерация DDL
├── tests/
│   ├── test_dag.py
│   ├── test_csv_reader.py
│   ├── test_sql_reader.py
│   ├── test_database_writers.py
│   ├── test_loader.py
│   ├── test_validator.py
│   ├── test_template.py
│   └── test_writers.py
├── infra/
│   └── compose.yaml              # Docker Compose для локального Airflow
├── pyproject.toml
└── .env.example
```

---

## Быстрый старт (Python API)

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
)

result = load(config)
print(f"Записано строк: {result.rows_written}")
print(f"Выходной файл: {result.output_file}")
```

---

## Запуск через Airflow

### Локально (Docker Compose)

```bash
# Запуск
make up

# Интерфейс Airflow
open http://localhost:8080   # admin / admin
```

### Параметры DAG

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

### Параметры

| Параметр | Тип | Обязательный | Описание |
|---|---|---|---|
| `input_file` | string | ✅ | Абсолютный путь к файлу |
| `db_type` | `gp` / `ch` | ✅ | Целевая БД |
| `table_name` | string | — | Имя таблицы (default: `table_name`) |
| `scheme_name` | string | — | Схема/БД (default: `scheme_name`) |
| `dump_type` | `sql` / `csv` | — | Тип выходного файла |
| `error_mode` | `raise`/`coerce`/`ignore`/`verify` | — | Обработка ошибок валидации |
| `dtypes_ddl` | string | — | DDL или список типов через запятую |
| `timestamp` | `write_ts` / `load_dttm` | — | Добавить временну́ю метку |
| `batch_size` | integer | — | Размер батча (default: 500) |
| `skip_rows` | integer | — | Пропустить N строк заголовка |
| `skip_cols` | integer | — | Пропустить N столбцов слева |
| `notify_email` | string | — | Email для уведомления об ошибках |

### Обработка ошибок в DAG

| `error_mode` | Поведение |
|---|---|
| `raise` | Проверяет данные, при ошибках — task failed, retry по расписанию |
| `coerce` | Ошибочные ячейки → NULL, загрузка продолжается |
| `ignore` | Загрузка без проверки |
| `verify` | Только проверка, выгрузки нет |

---

## Прямая запись в БД

```python
from manual_excel_loader.writers.base import DbWriterConfig
from manual_excel_loader.writers.database import PostgresWriter, ClickHouseWriter

# GreenPlum
pg_writer = PostgresWriter(DbWriterConfig(
    dsn="postgresql://user:pass@host:5432/db",
    table_name="my_table",
    scheme_name="my_schema",
    batch_size=1000,
))
pg_writer.write(headers=["id", "name"], rows=[(1, "Alice"), (2, "Bob")])

# ClickHouse
ch_writer = ClickHouseWriter(DbWriterConfig(
    dsn="clickhouse://user:pass@host:9000/mydb",
    table_name="my_table",
    scheme_name="mydb",
    batch_size=5000,
))
ch_writer.write(headers=["id", "name"], rows=[(1, "Alice")])
```

---

## Тесты

```bash
# Все тесты
make test

# С покрытием
make coverage

# Только DAG-тесты (без Airflow)
pytest tests/test_dag.py -v
```

---

## Разработка

```bash
# Установка зависимостей
pip install -e ".[dev]"

# Линтер
make lint

# Форматирование
make fmt
```