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
├── dags/
│   └── excel_loader_dag.py        # Airflow DAG
├── src/
│   └── manual_excel_loader/
│       ├── readers/
│       │   ├── excel_reader.py    # Чтение Excel
│       │   ├── csv_reader.py      # Чтение CSV/TSV
│       │   └── sql_reader.py      # Чтение SQL INSERT-файлов
│       ├── writers/
│       │   ├── base.py            # Абстракция + конфиги
│       │   ├── csv_file.py        # Запись в CSV
│       │   ├── sql_file.py        # Запись в SQL
│       │   └── database.py        # Прямая запись в GP / CH
│       ├── enums.py
│       ├── exceptions.py
│       ├── loader.py              # Основной API: load()
│       ├── models.py              # LoaderConfig, LoadResult
│       ├── result.py              # Ok / Err result types
│       ├── template.py            # Обработка шаблонов ODS
│       ├── validator.py           # Валидация типов данных
│       └── ddl.py                 # Генерация DDL
├── tests/
│   ├── test_dag.py
│   ├── test_loader.py
│   ├── test_validator.py
│   ├── test_template.py
│   ├── test_writers.py
│   ├── test_csv_reader.py
│   ├── test_sql_reader.py
│   └── test_database_writers.py
├── docs/
├── .env.example
├── .gitignore
├── docker-compose.yml             # Локальный Airflow + GP + CH
├── Makefile
└── pyproject.toml
```

---

## Быстрый старт (Python API)

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
)

result = load(config)
print(f"Записано строк: {result.rows_written}")
print(f"Выходной файл: {result.output_file}")
```

---

## Запуск через Airflow

### Локально (Docker Compose)

```bash
# 1. Создай .env из шаблона и заполни пароли
make init
nano .env

# 2. Запусти окружение
make up

# 3. Открой UI
open http://localhost:8080   # admin / <твой пароль из .env>
```

Docker Compose поднимает: Airflow (webserver + scheduler), PostgreSQL (метаданные Airflow + GreenPlum-совместимый эндпоинт), ClickHouse.

> **Важно:** пакет `manual_excel_loader` устанавливается внутри контейнера через `_PIP_ADDITIONAL_REQUIREMENTS` в `.env`. Если после `make up` видишь `ModuleNotFoundError` — убедись что переменная заполнена (подробнее в разделе [Деплой пакета](#деплой-пакета-в-контейнер)).

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

## Деплой пакета в контейнер

Airflow worker должен иметь доступ к пакету `manual_excel_loader`. Самый простой способ для локальной разработки — переменная окружения в `.env`:

```dotenv
# .env
_PIP_ADDITIONAL_REQUIREMENTS=python-dateutil openpyxl tqdm
```

Пакет при этом монтируется как volume и устанавливается через `pip install -e`:

```yaml
# docker-compose.yml (уже настроено)
volumes:
  - ./src:/opt/airflow/src
environment:
  _PIP_ADDITIONAL_REQUIREMENTS: "..."
```

Для продакшн-деплоя — собери кастомный образ:

```dockerfile
FROM apache/airflow:2.9.1
COPY src/ /opt/airflow/src/
COPY pyproject.toml /opt/airflow/
RUN pip install -e /opt/airflow/.[dev] --no-cache-dir
```

---

## Прямая запись в БД

```python
from manual_excel_loader.writers.base import DbWriterConfig
from manual_excel_loader.writers.database import PostgresWriter, ClickHouseWriter

# GreenPlum
pg_writer = PostgresWriter(DbWriterConfig(
    host="localhost", port=5432, database="de_db",
    user="admin", password="...",
    table_name="my_table", scheme_name="my_schema",
    batch_size=1000,
))
pg_writer.write(headers=["id", "name"], rows=[(1, "Alice"), (2, "Bob")])

# ClickHouse
ch_writer = ClickHouseWriter(DbWriterConfig(
    host="localhost", port=9000, database="default",
    user="admin", password="...",
    table_name="my_table", scheme_name="default",
    batch_size=5000,
))
ch_writer.write(headers=["id", "name"], rows=[(1, "Alice")])
```

---

## Тесты

```bash
# Все тесты
make test

# С покрытием (HTML-отчёт в htmlcov/)
make coverage

# Только DAG-тесты (без запущенного Airflow)
make test-dag

# Линтер
make lint

# Форматирование
make fmt
```

---

## Разработка

```bash
# Клонировать
git clone https://github.com/dzbarts/excel-loader.git
cd excel-loader

# Создать .env и виртуальное окружение
make init
python3 -m venv .venv && source .venv/bin/activate

# Установить зависимости (включая dev)
make install
```