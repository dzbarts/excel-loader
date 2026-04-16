"""
_connections.py
===============
Централизованные фабрики подключений к GP, PG,CH и SMB.

Использует Airflow-коннекторы:
    GP  → conn_updcc
    PG  → conn_updcc_pg
    CH  → conn_updcc_ch
    SMB → conn_updcc_smb
"""
from __future__ import annotations

_GP_CONN_ID  = "conn_updcc"
_PG_CONN_ID  = "conn_updcc_pg"
_CH_CONN_ID  = "conn_updcc_ch"
_SMB_CONN_ID = "conn_updcc_smb"
_SMB_DOMAIN  = "gazprom-neft"


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


def get_pg_conn():
    """Вернуть открытое psycopg2-соединение к PostgreSQL."""
    from airflow.hooks.base import BaseHook
    import psycopg2

    c = BaseHook.get_connection(_PG_CONN_ID)
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
    extra = c.extra_dejson
    return Client(
        host=c.host,
        port=int(c.port or 9000),
        database=c.schema,
        user=c.login,
        password=c.password,
        secure=extra.get("secure", False),
        verify=extra.get("verify", False),
    )


def write_smb_file_stream(
    host: str,
    remote_name: str,
    share: str,
    smb_dir: str,
    smb_file: str,
    file_obj,
) -> None:
    """Загрузить файл на SMB-шару из бинарного файлового объекта.

    Args:
        host:        IP-адрес сервера (для TCP-соединения, conn.connect).
        remote_name: имя сервера (в конструкторе SMBConnection).
        share:       Имя шары (например Disk5$).
        smb_dir:     Путь к папке внутри шары (пустая строка — корень шары).
        smb_file:    Имя файла.
        file_obj:    Бинарный файловый объект с методом read() (pipe, BytesIO и т.п.).
    """
    import socket

    from airflow.hooks.base import BaseHook
    from smb.SMBConnection import SMBConnection

    c = BaseHook.get_connection(_SMB_CONN_ID)

    conn = SMBConnection(
        username=c.login,
        password=c.password,
        my_name=socket.gethostname(),
        remote_name=remote_name,
        domain=_SMB_DOMAIN,
        use_ntlm_v2=True,
        is_direct_tcp=True,
    )
    conn.connect(host, 445)
    try:
        parts = [p.replace("\\", "/").strip("/") for p in [smb_dir, smb_file] if p and p.strip("/\\")]
        path = "/" + "/".join(parts)
        conn.storeFile(share, path, file_obj)
    finally:
        conn.close()


def get_smb_file_bytes(
    host: str,
    remote_name: str,
    share: str,
    smb_dir: str,
    smb_file: str,
) -> bytes:
    """Скачать файл с SMB-шары в память. Возвращает содержимое файла как bytes.

    Args:
        host:        IP-адрес сервера (для TCP-соединения, conn.connect).
        remote_name: NetBIOS-имя сервера (в конструкторе SMBConnection).
        share:       Имя шары (например Disk5$).
        smb_dir:     Путь к папке внутри шары (пустая строка — корень шары).
        smb_file:    Имя файла.
    """
    import io
    import socket

    from airflow.hooks.base import BaseHook
    from smb.SMBConnection import SMBConnection

    c = BaseHook.get_connection(_SMB_CONN_ID)

    conn = SMBConnection(
        username=c.login,
        password=c.password,
        my_name=socket.gethostname(),
        remote_name=remote_name,
        domain=_SMB_DOMAIN,
        use_ntlm_v2=True,
        is_direct_tcp=True,
    )
    conn.connect(host, 445)
    try:
        parts = [p.replace("\\", "/").strip("/") for p in [smb_dir, smb_file] if p and p.strip("/\\")]
        path = "/" + "/".join(parts)
        buf = io.BytesIO()
        conn.retrieveFile(share, path, buf)
        return buf.getvalue()
    finally:
        conn.close()
