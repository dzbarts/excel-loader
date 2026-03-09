"""
table_manager.py
================
Управление жизненным циклом целевой таблицы перед загрузкой данных.

Стратегии (export_mode):
    append         — добавить в существующую таблицу; создать если нет.
    truncate_load  — GP: TRUNCATE внутри транзакции (откат при ошибке).
                     CH: псевдооткат через временную таблицу *_temp.
    via_backup     — переименовать оригинал в *_before_YYMMDD_HHMM,
                     создать пустую таблицу с оригинальным именем, загрузить.
                     При ошибке: удалить новую, вернуть backup.

Публичный API
-------------
prepare(scheme, table, db_type, export_mode, create_ddl) -> dict
    Подготовить таблицу. Возвращает context-словарь для finalize().

finalize(context, success)
    Зафиксировать (success=True) или откатить (success=False) изменения.
"""
from __future__ import annotations

import logging
from datetime import datetime

from .enums import DatabaseType

log = logging.getLogger(__name__)

EXPORT_MODES = frozenset({"append", "truncate_load", "via_backup"})


# ── Public API ────────────────────────────────────────────────────────────────

def prepare(
    scheme: str,
    table: str,
    db_type: DatabaseType,
    export_mode: str,
    create_ddl: str | None = None,
) -> dict:
    """
    Подготовить таблицу согласно export_mode.

    Args:
        scheme:      схема / база данных.
        table:       имя таблицы.
        db_type:     целевая БД.
        export_mode: 'append' | 'truncate_load' | 'via_backup'.
        create_ddl:  DDL для создания таблицы если её нет.
                     Для via_backup: DDL новой таблицы (обязателен).

    Returns:
        Context-словарь, который нужно передать в finalize().
        Содержит открытое соединение/клиент — не создавай новое.
    """
    if db_type == DatabaseType.GREENPLUM:
        return _prepare_gp(scheme, table, export_mode, create_ddl)
    return _prepare_ch(scheme, table, export_mode, create_ddl)


def finalize(context: dict, success: bool) -> None:
    """Завершить операцию: закоммитить или откатить."""
    db_type = context.get("db_type")
    if db_type == DatabaseType.GREENPLUM:
        _finalize_gp(context, success)
    else:
        _finalize_ch(context, success)


# ── GreenPlum ─────────────────────────────────────────────────────────────────

def _prepare_gp(
    scheme: str,
    table: str,
    export_mode: str,
    create_ddl: str | None,
) -> dict:
    from ._connections import get_gp_conn

    conn = get_gp_conn()
    ctx: dict = {
        "db_type": DatabaseType.GREENPLUM,
        "conn": conn,
        "scheme": scheme,
        "table": table,
        "export_mode": export_mode,
    }

    if export_mode == "append":
        if create_ddl:
            _gp_create_if_not_exists(conn, create_ddl, scheme, table)
        conn.autocommit = True

    elif export_mode == "truncate_load":
        if create_ddl:
            _gp_create_if_not_exists(conn, create_ddl, scheme, table)
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute(f'TRUNCATE TABLE "{scheme}"."{table}"')
        ctx["cursor"] = cur
        log.info("GP: TRUNCATE %s.%s — ожидаем завершения загрузки", scheme, table)

    elif export_mode == "via_backup":
        if not create_ddl:
            raise ValueError("via_backup требует create_ddl для воссоздания таблицы")
        ts = datetime.now().strftime("%y%m%d_%H%M")
        backup = f"{table}_before_{ts}"
        conn.autocommit = False
        with conn.cursor() as cur:
            # Проверяем, что оригинал существует
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema=%s AND table_name=%s",
                (scheme, table),
            )
            if cur.fetchone():
                cur.execute(
                    f'ALTER TABLE "{scheme}"."{table}" RENAME TO "{backup}"'
                )
                log.info("GP: renamed %s.%s → %s.%s", scheme, table, scheme, backup)
                ctx["backup_table"] = backup
            else:
                log.info("GP: таблица %s.%s не существует, backup пропущен", scheme, table)
            cur.execute(create_ddl)
        conn.commit()
        conn.autocommit = True

    return ctx


def _gp_create_if_not_exists(conn, ddl: str, scheme: str, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema=%s AND table_name=%s",
            (scheme, table),
        )
        if not cur.fetchone():
            cur.execute(ddl)
            conn.commit()
            log.info("GP: создана таблица %s.%s", scheme, table)


def _finalize_gp(ctx: dict, success: bool) -> None:
    conn = ctx.get("conn")
    scheme, table = ctx["scheme"], ctx["table"]
    export_mode = ctx["export_mode"]

    try:
        if export_mode == "truncate_load":
            cur = ctx.get("cursor")
            if success:
                conn.commit()
                log.info("GP: truncate_load зафиксирован для %s.%s", scheme, table)
            else:
                conn.rollback()
                log.warning("GP: truncate_load откатан для %s.%s", scheme, table)
            if cur:
                cur.close()

        elif export_mode == "via_backup" and not success:
            backup = ctx.get("backup_table")
            if backup:
                with conn.cursor() as cur:
                    cur.execute(f'DROP TABLE IF EXISTS "{scheme}"."{table}"')
                    cur.execute(
                        f'ALTER TABLE "{scheme}"."{backup}" RENAME TO "{table}"'
                    )
                conn.commit()
                log.warning(
                    "GP: via_backup откатан, восстановлено из %s.%s", scheme, backup
                )
    finally:
        if conn:
            conn.close()


# ── ClickHouse ────────────────────────────────────────────────────────────────

def _prepare_ch(
    scheme: str,
    table: str,
    export_mode: str,
    create_ddl: str | None,
) -> dict:
    from ._connections import get_ch_client

    client = get_ch_client()
    ctx: dict = {
        "db_type": DatabaseType.CLICKHOUSE,
        "client": client,
        "scheme": scheme,
        "table": table,
        "export_mode": export_mode,
    }

    if export_mode == "append":
        if create_ddl:
            _ch_create_if_not_exists(client, create_ddl, scheme, table)

    elif export_mode == "truncate_load":
        if create_ddl:
            _ch_create_if_not_exists(client, create_ddl, scheme, table)
        temp = f"{table}_temp"
        # Создаём копию структуры + данных как псевдобэкап
        client.execute(
            f"CREATE TABLE `{scheme}`.`{temp}` AS `{scheme}`.`{table}`"
        )
        client.execute(
            f"INSERT INTO `{scheme}`.`{temp}` SELECT * FROM `{scheme}`.`{table}`"
        )
        client.execute(f"TRUNCATE TABLE `{scheme}`.`{table}`")
        ctx["temp_table"] = temp
        log.info(
            "CH: truncate_load подготовлен, псевдобэкап: %s.%s", scheme, temp
        )

    elif export_mode == "via_backup":
        if not create_ddl:
            raise ValueError("via_backup требует create_ddl для воссоздания таблицы")
        ts = datetime.now().strftime("%y%m%d_%H%M")
        backup = f"{table}_before_{ts}"
        # Проверяем наличие оригинала
        rows = client.execute(
            "SELECT 1 FROM system.tables WHERE database=%(db)s AND name=%(tbl)s",
            {"db": scheme, "tbl": table},
        )
        if rows:
            client.execute(
                f"RENAME TABLE `{scheme}`.`{table}` TO `{scheme}`.`{backup}`"
            )
            log.info("CH: renamed %s.%s → %s.%s", scheme, table, scheme, backup)
            ctx["backup_table"] = backup
        else:
            log.info("CH: таблица %s.%s не существует, backup пропущен", scheme, table)
        client.execute(create_ddl)

    return ctx


def _ch_create_if_not_exists(client, ddl: str, scheme: str, table: str) -> None:
    rows = client.execute(
        "SELECT 1 FROM system.tables WHERE database=%(db)s AND name=%(tbl)s",
        {"db": scheme, "tbl": table},
    )
    if not rows:
        client.execute(ddl)
        log.info("CH: создана таблица %s.%s", scheme, table)


def _finalize_ch(ctx: dict, success: bool) -> None:
    client = ctx.get("client")
    scheme, table = ctx["scheme"], ctx["table"]
    export_mode = ctx["export_mode"]

    if export_mode == "truncate_load":
        temp = ctx.get("temp_table")
        if success:
            client.execute(f"DROP TABLE IF EXISTS `{scheme}`.`{temp}`")
            log.info("CH: truncate_load завершён, temp-таблица удалена")
        else:
            client.execute(
                f"INSERT INTO `{scheme}`.`{table}` "
                f"SELECT * FROM `{scheme}`.`{temp}`"
            )
            client.execute(f"DROP TABLE IF EXISTS `{scheme}`.`{temp}`")
            log.warning(
                "CH: truncate_load псевдооткат из %s.%s", scheme, temp
            )

    elif export_mode == "via_backup" and not success:
        backup = ctx.get("backup_table")
        if backup:
            client.execute(f"DROP TABLE IF EXISTS `{scheme}`.`{table}`")
            client.execute(
                f"RENAME TABLE `{scheme}`.`{backup}` TO `{scheme}`.`{table}`"
            )
            log.warning(
                "CH: via_backup откатан, восстановлено из %s.%s", scheme, backup
            )
