import logging
from typing import Optional
import asyncpg
from app.config import settings

logger = logging.getLogger(__name__)
_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Database pool not initialised — call init_db() first.")
    return _pool


async def init_db() -> asyncpg.Pool:
    global _pool
    dsn = settings.database_url.replace("postgres://", "postgresql://", 1)
    logger.info("Connecting to PostgreSQL…")
    _pool = await asyncpg.create_pool(
        dsn=dsn, min_size=2, max_size=10,
        command_timeout=30, statement_cache_size=0,
    )
    logger.info("PostgreSQL pool created.")
    await _run_migrations(_pool)
    return _pool


async def close_db() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def _run_migrations(pool: asyncpg.Pool) -> None:
    logger.info("Running schema migrations…")
    async with pool.acquire() as conn:
        await conn.execute(_SCHEMA_SQL)
    logger.info("Migrations complete.")


_SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS universe (
    symbol              TEXT        PRIMARY KEY,
    exchange            TEXT        NOT NULL DEFAULT 'NSE',
    fo_eligible         BOOLEAN     NOT NULL DEFAULT TRUE,
    avg_turnover_cr     NUMERIC,
    avg_volume          BIGINT,
    last_backtested_at  TIMESTAMPTZ,
    is_active           BOOLEAN     NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS active_setup (
    setup_id    INT         PRIMARY KEY,
    setup_name  TEXT        NOT NULL,
    is_current  BOOLEAN     NOT NULL DEFAULT FALSE,
    deployed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_active_setup_current
    ON active_setup (is_current) WHERE is_current = TRUE;

CREATE TABLE IF NOT EXISTS watchlist (
    id              SERIAL      PRIMARY KEY,
    symbol          TEXT        NOT NULL,
    active_setup_id INT         REFERENCES active_setup (setup_id) ON DELETE SET NULL,
    added_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_watchlist_symbol ON watchlist (symbol);

CREATE TABLE IF NOT EXISTS daily_candidates (
    id                  SERIAL      PRIMARY KEY,
    date                DATE        NOT NULL,
    symbol              TEXT        NOT NULL,
    day_type            TEXT        NOT NULL,
    bias                TEXT        NOT NULL,
    score               INT         NOT NULL,
    key_level           NUMERIC,
    invalidation_level  NUMERIC,
    reasoning           TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (date, symbol)
);
CREATE INDEX IF NOT EXISTS ix_daily_candidates_date ON daily_candidates (date);

CREATE TABLE IF NOT EXISTS open_trades (
    trade_id        UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    date            DATE        NOT NULL DEFAULT CURRENT_DATE,
    symbol          TEXT        NOT NULL,
    direction       TEXT        NOT NULL,
    setup_id        INT,
    entry_order_id  TEXT,
    sl_order_id     TEXT,
    t1_order_id     TEXT,
    t2_order_id     TEXT,
    entry_price     NUMERIC,
    stop_price      NUMERIC,
    target_1        NUMERIC,
    target_2        NUMERIC,
    quantity        INT         NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'ENTRY_PENDING',
    opened_at       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_open_trades_date ON open_trades (date);
CREATE INDEX IF NOT EXISTS ix_open_trades_symbol_date ON open_trades (symbol, date);

CREATE TABLE IF NOT EXISTS trade_history (
    trade_id        UUID        PRIMARY KEY,
    date            DATE        NOT NULL,
    symbol          TEXT        NOT NULL,
    direction       TEXT        NOT NULL,
    setup_id        INT,
    entry_price     NUMERIC     NOT NULL,
    exit_price      NUMERIC     NOT NULL,
    quantity        INT         NOT NULL,
    gross_pnl       NUMERIC     NOT NULL,
    net_pnl         NUMERIC     NOT NULL,
    r_multiple      NUMERIC,
    exit_reason     TEXT,
    opened_at       TIMESTAMPTZ,
    closed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_trade_history_date     ON trade_history (date);
CREATE INDEX IF NOT EXISTS ix_trade_history_symbol   ON trade_history (symbol);
CREATE INDEX IF NOT EXISTS ix_trade_history_setup_id ON trade_history (setup_id);

CREATE TABLE IF NOT EXISTS daily_pnl (
    date            DATE        PRIMARY KEY,
    opening_capital NUMERIC     NOT NULL,
    closing_capital NUMERIC     NOT NULL,
    gross_pnl       NUMERIC     NOT NULL DEFAULT 0,
    net_pnl         NUMERIC     NOT NULL DEFAULT 0,
    num_trades      INT         NOT NULL DEFAULT 0,
    num_wins        INT         NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS backtest_results (
    id                  SERIAL      PRIMARY KEY,
    symbol              TEXT        NOT NULL,
    setup_id            INT         NOT NULL,
    setup_name          TEXT        NOT NULL,
    num_trades          INT         NOT NULL,
    win_rate            NUMERIC     NOT NULL,
    avg_r               NUMERIC     NOT NULL,
    sharpe              NUMERIC     NOT NULL,
    max_dd              NUMERIC     NOT NULL,
    oos_sharpe          NUMERIC,
    passed_walkforward  BOOLEAN     NOT NULL DEFAULT FALSE,
    backtested_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_backtest_results_symbol    ON backtest_results (symbol);
CREATE INDEX IF NOT EXISTS ix_backtest_results_setup_id  ON backtest_results (setup_id);
"""
