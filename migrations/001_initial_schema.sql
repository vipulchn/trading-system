-- 001_initial_schema.sql
-- Run once at first deploy (app auto-runs this via database.py init_db).
-- All tables are idempotent (CREATE TABLE IF NOT EXISTS).

-- ── Extensions ────────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "pgcrypto";   -- gen_random_uuid()

-- ── universe ──────────────────────────────────────────────────────────────────
-- Symbols that passed the backtester universe filter.
CREATE TABLE IF NOT EXISTS universe (
    symbol              TEXT        PRIMARY KEY,
    exchange            TEXT        NOT NULL DEFAULT 'NSE',
    fo_eligible         BOOLEAN     NOT NULL DEFAULT TRUE,
    avg_turnover_cr     NUMERIC,
    avg_volume          BIGINT,
    last_backtested_at  TIMESTAMPTZ,
    is_active           BOOLEAN     NOT NULL DEFAULT TRUE
);

-- ── active_setup ──────────────────────────────────────────────────────────────
-- The one setup currently deployed for live trading.
CREATE TABLE IF NOT EXISTS active_setup (
    setup_id    INT         PRIMARY KEY,
    setup_name  TEXT        NOT NULL,
    is_current  BOOLEAN     NOT NULL DEFAULT FALSE,
    deployed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Only one row can be current at a time.
CREATE UNIQUE INDEX IF NOT EXISTS ux_active_setup_current
    ON active_setup (is_current) WHERE is_current = TRUE;

-- ── watchlist ─────────────────────────────────────────────────────────────────
-- Symbols that survived the backtest and are scanned each pre-market.
CREATE TABLE IF NOT EXISTS watchlist (
    id              SERIAL      PRIMARY KEY,
    symbol          TEXT        NOT NULL,
    active_setup_id INT         REFERENCES active_setup (setup_id) ON DELETE SET NULL,
    added_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_watchlist_symbol
    ON watchlist (symbol);

-- ── daily_candidates ──────────────────────────────────────────────────────────
-- LLM-scored shortlist written by Agent 2 each morning at 08:30.
CREATE TABLE IF NOT EXISTS daily_candidates (
    id                  SERIAL      PRIMARY KEY,
    date                DATE        NOT NULL,
    symbol              TEXT        NOT NULL,
    day_type            TEXT        NOT NULL,   -- Normal | Trend | Gap | Neutral
    bias                TEXT        NOT NULL,   -- Long | Short
    score               INT         NOT NULL,
    key_level           NUMERIC,
    invalidation_level  NUMERIC,
    reasoning           TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (date, symbol)
);

CREATE INDEX IF NOT EXISTS ix_daily_candidates_date
    ON daily_candidates (date);

-- ── open_trades ───────────────────────────────────────────────────────────────
-- Active intraday positions managed by Agent 5.
CREATE TABLE IF NOT EXISTS open_trades (
    trade_id        UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    date            DATE        NOT NULL DEFAULT CURRENT_DATE,
    symbol          TEXT        NOT NULL,
    direction       TEXT        NOT NULL,   -- Long | Short
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
    -- ENTRY_PENDING | OPEN | T1_HIT | CLOSED | CANCELLED
    opened_at       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_open_trades_date
    ON open_trades (date);
CREATE INDEX IF NOT EXISTS ix_open_trades_symbol_date
    ON open_trades (symbol, date);

-- ── trade_history ─────────────────────────────────────────────────────────────
-- Immutable record of every completed trade.
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
    exit_reason     TEXT,       -- T1_HIT | T2_HIT | SL_HIT | BE_SL_HIT | FORCE_EXIT
    opened_at       TIMESTAMPTZ,
    closed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_trade_history_date
    ON trade_history (date);
CREATE INDEX IF NOT EXISTS ix_trade_history_symbol
    ON trade_history (symbol);
CREATE INDEX IF NOT EXISTS ix_trade_history_setup_id
    ON trade_history (setup_id);

-- ── daily_pnl ─────────────────────────────────────────────────────────────────
-- End-of-day summary written by Agent 6 at 15:30.
-- Used by Agent 2 at 08:00 to detect capital mismatches.
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

-- ── backtest_results ──────────────────────────────────────────────────────────
-- Per-symbol, per-setup metrics from Agent 1 walk-forward validation.
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

CREATE INDEX IF NOT EXISTS ix_backtest_results_symbol
    ON backtest_results (symbol);
CREATE INDEX IF NOT EXISTS ix_backtest_results_setup_id
    ON backtest_results (setup_id);
-- Latest result per symbol+setup (for watchlist population)
CREATE INDEX IF NOT EXISTS ix_backtest_results_symbol_setup_ts
    ON backtest_results (symbol, setup_id, backtested_at DESC);
