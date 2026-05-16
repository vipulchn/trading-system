"""
Agent 1 — Universe Backtester

Schedule: Sunday 06:00 IST (APScheduler cron)
Also triggerable via /backtest Telegram command.

Data source: Angel One SmartAPI — 6-month 15-min OHLCV
Universe: ~180 NSE F&O-eligible stocks (turnover >50Cr, volume >5L shares)

Output:
  PostgreSQL watchlist table    — top-50 stocks with scores
  PostgreSQL active_setup table — winning setup ID for the coming week
  Telegram report               — summary with action required
"""

import logging
from datetime import datetime, timedelta
from decimal import Decimal

import pytz

from app.constants import (
    IST_TIMEZONE,
    SETUP_NAMES,
    DISABLED_SETUPS,
    VRVP_LOOKBACK_SESSIONS,
    VRVP_ROWS,
    VRVP_VALUE_AREA_PCT,
    MIN_AVG_DAILY_TURNOVER_CR,
    MIN_AVG_DAILY_VOLUME,
    UNIVERSE_TOP_N,
    MIN_BACKTEST_TRADES,
    WALKFORWARD_TRAIN_MONTHS,
    WALKFORWARD_OOS_MONTHS,
    WALKFORWARD_VALIDATION_RATIO,
    SCORE_WEIGHT_SHARPE,
    SCORE_WEIGHT_WIN_RATE,
    SCORE_WEIGHT_AVG_R,
    SUSPICIOUS_WIN_RATE,
    BROKERAGE_PCT,
    STT_PCT,
    GST_ON_BROKERAGE,
    EXCHANGE_CHARGE_PER_ORDER,
    SEBI_CHARGE_PCT,
)
from app.database import get_pool
from app.telegram import send_message, fmt_backtest_complete

logger = logging.getLogger(__name__)
IST = pytz.timezone(IST_TIMEZONE)


async def run_universe_backtest() -> None:
    """Entry point — called by APScheduler and /backtest command."""
    logger.info("Agent 1: Universe backtest starting.")
    started_at = datetime.now(IST)

    try:
        symbols = await _fetch_universe()
        logger.info("Agent 1: Universe size = %d symbols.", len(symbols))

        results = []
        for symbol in symbols:
            data = await _fetch_ohlcv(symbol)
            if data is None:
                continue
            result = await _backtest_symbol(symbol, data)
            if result:
                results.append(result)

        if not results:
            await send_message("⚠️ Agent 1: No symbols passed backtest thresholds. Watchlist unchanged.")
            return

        ranked = _rank_and_select(results)
        active_setup_id = _select_active_setup(ranked)
        active_setup_name = SETUP_NAMES[active_setup_id]

        await _write_watchlist(ranked)
        await _write_active_setup(active_setup_id, active_setup_name, ranked)

        top5 = [r["symbol"] for r in ranked[:5]]
        new_entries, dropped = await _diff_watchlist(ranked)
        weakest = [r["symbol"] for r in ranked[-5:]]

        msg = fmt_backtest_complete(
            date=started_at.strftime("%Y-%m-%d"),
            setup_name=active_setup_name,
            setup_id=active_setup_id,
            top5=top5,
            new_entries=new_entries,
            dropped=dropped,
            weakest=weakest,
        )
        await send_message(msg)
        logger.info("Agent 1: Backtest complete. Active setup: %s.", active_setup_name)

    except Exception as exc:
        logger.exception("Agent 1: Backtest failed: %s", exc)
        await send_message(f"🚨 Agent 1 BACKTEST FAILED: {exc}")


# ---------------------------------------------------------------------------
# TODO: implement these stubs
# ---------------------------------------------------------------------------

async def _fetch_universe() -> list[str]:
    """
    TODO: Pull F&O-eligible NSE stocks from Angel One SmartAPI.
    Filter: avg daily turnover >50Cr AND avg daily volume >5L shares.
    Returns list of symbol strings e.g. ['RELIANCE', 'INFY', ...].
    """
    raise NotImplementedError("_fetch_universe: connect Angel One SmartAPI")


async def _fetch_ohlcv(symbol: str) -> list[dict] | None:
    """
    TODO: Fetch 6 months of 15-min OHLCV from Angel One SmartAPI.
    Returns list of candles: [{timestamp, open, high, low, close, volume}, ...]
    Returns None if data unavailable or insufficient.
    """
    raise NotImplementedError(f"_fetch_ohlcv({symbol}): connect Angel One SmartAPI")


async def _backtest_symbol(symbol: str, candles: list[dict]) -> dict | None:
    """
    TODO: Run Setups 1–5 (never 6) on historical candles.

    For each setup:
      1. Compute rolling VRVP (20-session lookback, 200 rows, 70% VA).
      2. Compute SVP per session from 09:15 IST.
      3. Detect setup trigger conditions.
      4. Simulate entries, exits (T1/T2/stop), apply transaction costs.
      5. Compute: win_rate, avg_r, sharpe, max_dd, trade_count.
      6. Walk-forward: train months 1–4, validate months 5–6.
         Reject if validation_sharpe < 0.7 × backtest_sharpe.
      7. Reject if win_rate > 70% (look-ahead bias indicator).
      8. Reject if trade_count < MIN_BACKTEST_TRADES.

    Returns best-setup dict per symbol, or None if all setups fail thresholds.

    Return schema:
    {
        "symbol": str,
        "best_setup_id": int,
        "composite_score": float,
        "win_rate": float,
        "avg_r": float,
        "sharpe": float,
        "max_dd": float,
        "backtest_sharpe": float,
        "validation_sharpe": float,
        "trade_count": int,
    }
    """
    raise NotImplementedError(f"_backtest_symbol({symbol}): implement VRVP backtester")


def _rank_and_select(results: list[dict]) -> list[dict]:
    """
    Composite score = (Sharpe × 0.4) + (win_rate × 0.3) + (avg_r × 0.3).
    Return top-UNIVERSE_TOP_N ranked by composite_score descending.
    """
    for r in results:
        r["composite_score"] = (
            r["sharpe"] * SCORE_WEIGHT_SHARPE
            + r["win_rate"] * SCORE_WEIGHT_WIN_RATE
            + r["avg_r"] * SCORE_WEIGHT_AVG_R
        )
    return sorted(results, key=lambda x: x["composite_score"], reverse=True)[:UNIVERSE_TOP_N]


def _select_active_setup(ranked: list[dict]) -> int:
    """
    Return the setup_id with the highest aggregate composite score
    across all top-UNIVERSE_TOP_N ranked stocks.
    """
    from collections import defaultdict
    scores: dict[int, float] = defaultdict(float)
    for r in ranked:
        scores[r["best_setup_id"]] += r["composite_score"]
    return max(scores, key=lambda k: scores[k])


async def _write_watchlist(ranked: list[dict]) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            for r in ranked:
                await conn.execute(
                    """
                    INSERT INTO watchlist
                        (symbol, composite_score, win_rate, avg_r, sharpe, max_dd,
                         active_setup_id, validation_passed, updated_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
                    """,
                    r["symbol"],
                    r["composite_score"],
                    r["win_rate"],
                    r["avg_r"],
                    r["sharpe"],
                    r["max_dd"],
                    r["best_setup_id"],
                    True,
                )


async def _write_active_setup(setup_id: int, setup_name: str, ranked: list[dict]) -> None:
    pool = await get_pool()
    # Find aggregate backtest/validation Sharpe for the winning setup
    matching = [r for r in ranked if r["best_setup_id"] == setup_id]
    bt_sharpe = sum(r["backtest_sharpe"] for r in matching) / max(len(matching), 1)
    val_sharpe = sum(r["validation_sharpe"] for r in matching) / max(len(matching), 1)

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("UPDATE active_setup SET is_current = FALSE")
            await conn.execute(
                """
                INSERT INTO active_setup (setup_id, setup_name, backtest_sharpe,
                    validation_sharpe, is_current, activated_at)
                VALUES ($1,$2,$3,$4,TRUE,NOW())
                """,
                setup_id, setup_name, bt_sharpe, val_sharpe,
            )


async def _diff_watchlist(ranked: list[dict]) -> tuple[list[str], list[str]]:
    """Return (new_entries, dropped) vs previous watchlist snapshot."""
    pool = await get_pool()
    new_symbols = {r["symbol"] for r in ranked}
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT symbol FROM watchlist
            WHERE updated_at < NOW() - INTERVAL '6 days'
            """
        )
    old_symbols = {row["symbol"] for row in rows}
    new_entries = sorted(new_symbols - old_symbols)
    dropped = sorted(old_symbols - new_symbols)
    return new_entries, dropped
