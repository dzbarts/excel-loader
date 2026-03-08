# excel-loader

Загрузчик Excel / CSV / SQL → GreenPlum / ClickHouse с валидацией данных.
Запускается как Python API вручную или через Apache Airflow DAG.

---

## Возможности

| Функция | Статус |
|---|---|
| Чтение Excel (`.xlsx`, `.xlsm`) | ✅ |
| Чтение CSV / TSV | ✅ |
| Чтение SQL INSERT-файлов | ✅ |
| Валидация типов данных (GP / CH) | ✅ |
| Поддержка шаблонов ODS (`data` + `klad_config`) | ✅ |
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
├── dags/                            ← копировать целиком в $AIRFLOW_HOME/dags/
│   ├── excel_loader_dag.py          # Airflow DAG
│   └── manual_excel_loader/        # пакет лежит рядом с DAG — pip install не нужен
│       ├── readers/
│       │   ├── excel_reader.py
│       │   ├── csv_reader.py
│       │   └── sql_reader.py
│       ├── writers/
│       │   ├── base.py
│       │   ├── csv_file.py
│       │   ├── sql_file.py
│       │   └── database.py
│       ├── enums.py
│       ├── exceptions.py
│       ├── loader.py
│       ├── models.py
│       ├── result.py
│       ├── template.py
│       ├── validator.py
│       └── ddl.py
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
├── conftest.py                      # добавляет dags/ в sys.path для тестов
├── pyproject.toml
└── Makefile
```

---

## Деплой в Airflow

Никакого `pip install` не требуется. Airflow автоматически добавляет папку `dags/`
в `sys.path`, поэтому пакет `manual_excel_loader` импортируется напрямую.

**Единственное действие:**
```
Скопировать папку dags/ целиком → $AIRFLOW_HOME/dags/
```

Зависимости (`openpyxl`, `python-dateutil`, `tqdm`) должны быть установлены
в Python-окружении Airflow — они входят в стандартный список разрешённых пакетов
корпоративного Nexus.

---

## Быстрый старт (Python API / ручной запуск)

```bash
pip install -e ".[dev]"
```

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
    show_progress=True,   # tqdm прогресс-бар при ручном запуске
)

result = load(config)
print(f"Записано строк: {result.rows_written}")
print(f"Выходной файл: {result.output_file}")
```

> `show_progress=True` — только для ручного запуска из терминала.
> При запуске через Airflow оставьте `False` (по умолчанию).

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

| Параметр | Тип | Обязательный | Описание |
|---|---|---|---|
| `input_file` | string | ✅ | Абсолютный путь к файлу внутри контейнера |
| `db_type` | `gp` / `ch` | ✅ | Целевая БД |
| `table_name` | string | — | Имя таблицы (default: `table_name`) |
| `scheme_name` | string | — | Схема / БД (default: `scheme_name`) |
| `dump_type` | `sql` / `csv` | — | Тип выходного файла |
| `error_mode` | `raise`/`coerce`/`ignore`/`verify` | — | Обработка ошибок валидации |
| `dtypes_ddl` | string | — | DDL-строка или список типов |
| `timestamp` | `write_ts` / `load_dttm` | — | Добавить временну́ю метку |
| `batch_size` | integer | — | Размер батча (default: 500) |
| `skip_rows` | integer | — | Пропустить N строк заголовка |
| `skip_cols` | integer | — | Пропустить N столбцов слева |
| `notify_email` | string | — | Email для уведомления об ошибках |

### Обработка ошибок в DAG

| `error_mode` | Поведение |
|---|---|
| `raise` | Валидирует; при ошибках — task failed + retry |
| `coerce` | Ошибочные ячейки → NULL, загрузка продолжается |
| `ignore` | Загрузка без проверки |
| `verify` | Только проверка, выгрузки нет |

---

## Прямая запись в БД

```python
from manual_excel_loader.writers.base import DbWriterConfig
from manual_excel_loader.writers.database import PostgresWriter, ClickHouseWriter

pg_writer = PostgresWriter(DbWriterConfig(
    host="localhost", port=5432, database="de_db",
    user="admin", password="...",
    table_name="my_table", scheme_name="my_schema",
    batch_size=1000,
))
pg_writer.write(headers=["id", "name"], rows=[(1, "Alice"), (2, "Bob")])
```

---

## Тесты

```bash
make test          # все тесты
make coverage      # с покрытием (HTML в htmlcov/)
make test-dag      # только DAG-тесты
make lint          # линтер
make fmt           # форматирование
```
