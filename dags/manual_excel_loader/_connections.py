"""
_connections.py
===============
Централизованные фабрики подключений к GP и CH.

Использует Airflow-коннекторы:
    GP  → conn_updcc
    CH  → conn_updcc_ch

Импорты Airflow/драйверов — внутри функций, чтобы модуль безопасно
импортировался вне Airflow-окружения (тесты, CLI).
"""
from __future__ import annotations

_GP_CONN_ID = "conn_updcc"
_CH_CONN_ID = "conn_updcc_ch"


def get_gp_conn():
    """Вернуть открытое psycopg2-соединение к GreenPlum."""
    from airflow.hooks.base import BaseHook
    import psycopg2

    c = BaseHook.get_connection(_GP_CONN_ID)
    return psycopg2.connect(
        host=c.host,
        port=int(c.port or 5432),
        dbname=c.schema,
        user=c.login,
        password=c.password,
    )


def get_ch_client():
    """Вернуть clickhouse_driver.Client к ClickHouse."""
    from airflow.hooks.base import BaseHook
    from clickhouse_driver import Client

    c = BaseHook.get_connection(_CH_CONN_ID)
    return Client(
        host=c.host,
        port=int(c.port or 9000),
        database=c.schema,
        user=c.login,
        password=c.password,
    )
