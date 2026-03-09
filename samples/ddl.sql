-- =============================================================================
-- DDL для тестовых файлов из папки samples/
--
-- Каждая таблица представлена в двух вариантах:
--   • Greenplum (PostgreSQL-совместимый синтаксис)
--   • ClickHouse
-- =============================================================================


-- =============================================================================
-- 1. valid_sales.xlsx / invalid_types.xlsx
--    Колонки: order_id, customer, amount, order_date
-- =============================================================================

-- ── Greenplum ────────────────────────────────────────────────────────────────
CREATE TABLE public.sales (
    order_id    INTEGER      NOT NULL,
    customer    TEXT,
    amount      NUMERIC(10, 2),
    order_date  DATE
)
DISTRIBUTED BY (order_id);

-- ── ClickHouse ───────────────────────────────────────────────────────────────
CREATE TABLE default.sales (
    order_id    Int32,
    customer    String,
    amount      Decimal(10, 2),
    order_date  Date
)
ENGINE = MergeTree()
ORDER BY order_id;


-- =============================================================================
-- 2. skip_rows_example.xlsx  (skip_rows=3)
--    Колонки: product_id, product_name, units_sold, revenue
-- =============================================================================

-- ── Greenplum ────────────────────────────────────────────────────────────────
CREATE TABLE public.products (
    product_id    INTEGER      NOT NULL,
    product_name  TEXT         NOT NULL,
    units_sold    INTEGER,
    revenue       NUMERIC(12, 2)
)
DISTRIBUTED BY (product_id);

-- ── ClickHouse ───────────────────────────────────────────────────────────────
CREATE TABLE default.products (
    product_id    Int32,
    product_name  String,
    units_sold    Int32,
    revenue       Decimal(12, 2)
)
ENGINE = MergeTree()
ORDER BY product_id;


-- =============================================================================
-- 3. large_errors.xlsx
--    Колонки: user_id, score, registered_at
--    score — каждая 3-я строка содержит строку "N/A_N" вместо числа
--    registered_at — каждая 5-я строка содержит "bad-date" вместо даты
-- =============================================================================

-- ── Greenplum ────────────────────────────────────────────────────────────────
CREATE TABLE public.user_scores (
    user_id        INTEGER   NOT NULL,
    score          NUMERIC(6, 2),
    registered_at  DATE
)
DISTRIBUTED BY (user_id);

-- ── ClickHouse ───────────────────────────────────────────────────────────────
CREATE TABLE default.user_scores (
    user_id        Int32,
    score          Decimal(6, 2),
    registered_at  Date
)
ENGINE = MergeTree()
ORDER BY user_id;


-- =============================================================================
-- 4. multisheet.xlsx — лист "orders"
--    Колонки: order_id, amount
-- =============================================================================

-- ── Greenplum ────────────────────────────────────────────────────────────────
CREATE TABLE public.orders (
    order_id  INTEGER        NOT NULL,
    amount    NUMERIC(10, 2)
)
DISTRIBUTED BY (order_id);

-- ── ClickHouse ───────────────────────────────────────────────────────────────
CREATE TABLE default.orders (
    order_id  Int32,
    amount    Decimal(10, 2)
)
ENGINE = MergeTree()
ORDER BY order_id;


-- =============================================================================
-- 4b. multisheet.xlsx — лист "returns"
--     Колонки: return_id, reason, refund_amount
-- =============================================================================

-- ── Greenplum ────────────────────────────────────────────────────────────────
CREATE TABLE public.returns (
    return_id      INTEGER        NOT NULL,
    reason         TEXT,
    refund_amount  NUMERIC(10, 2)
)
DISTRIBUTED BY (return_id);

-- ── ClickHouse ───────────────────────────────────────────────────────────────
CREATE TABLE default.returns (
    return_id      Int32,
    reason         String,
    refund_amount  Decimal(10, 2)
)
ENGINE = MergeTree()
ORDER BY return_id;
