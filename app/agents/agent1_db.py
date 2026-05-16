"""
agent1_db.py — Database write helpers for Agent 1 (backtester).

Kept in a separate module so agent1_backtester.py stays under the file-size
limit that applies to the mounted workspace.
"""

import logging
from app.constants import SETUP_NAMES
from app.database import get_pool

logger = logging.getLogger(__name__)


async def write_backtest_results(results: list[dict]) -> None:
    """Persist per-symbol best-setup metrics to backtest_results."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            for r in results:
                sid = r["best_setup_id"]
                await conn.execute(
                    """INSERT INTO backtest_results
                       (symbol, setup_id, setup_name, num_trades, win_rate, avg_r,
                        sharpe, max_dd, oos_sharpe, passed_walkforward)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,TRUE)""",
                    r["symbol"], sid, SETUP_NAMES[sid],
                    r["trade_count"], r["win_rate"], r["avg_r"],
                    r["sharpe"], r["max_dd"], r["validation_sharpe"],
                )
    logger.info("Agent 1 DB: backtest_results written for %d symbols.", len(results))


async def write_universe(results: list[dict]) -> None:
    """Upsert symbols that passed all filters into the universe table."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            for r in results:
                await conn.execute(
                    """INSERT INTO universe
                           (symbol, fo_eligible, last_backtested_at, is_active)
                       VALUES ($1, TRUE, NOW(), TRUE)
                       ON CONFLICT (symbol) DO UPDATE
                         SET last_backtested_at = NOW(), is_active = TRUE""",
                    r["symbol"],
                )
    logger.info("Agent 1 DB: universe upserted for %d symbols.", len(results))


async def get_current_watchlist_symbols() -> set[str]:
    """Return the set of symbols currently in the watchlist (for diff reporting)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT symbol FROM watchlist")
    return {r["symbol"] for r in rows}


async def write_watchlist(ranked: list[dict], active_setup_id: int) -> None:
    """Replace watchlist with the new ranked set. UPSERT-safe for re-runs."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            new_symbols = [r["symbol"] for r in ranked]
            # Deactivate symbols that dropped out of the ranked list
            await conn.execute(
                "UPDATE universe SET is_active = FALSE "
                "WHERE symbol != ALL($1::text[])",
                new_symbols,
            )
            await conn.execute("DELETE FROM watchlist")
            for r in ranked:
                await conn.execute(
                    """INSERT INTO watchlist (symbol, active_setup_id, updated_at)
                       VALUES ($1, $2, NOW())
                       ON CONFLICT (symbol) DO UPDATE
                         SET active_setup_id = $2, updated_at = NOW()""",
                    r["symbol"], active_setup_id,
                )
    logger.info("Agent 1 DB: watchlist updated — %d symbols.", len(ranked))


async def write_active_setup(setup_id: int, setup_name: str) -> None:
    """Set the winning setup as is_current=TRUE; clear all others."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("UPDATE active_setup SET is_current = FALSE")
            await conn.execute(
                """INSERT INTO active_setup
                       (setup_id, setup_name, is_current, deployed_at)
                   VALUES ($1, $2, TRUE, NOW())
                   ON CONFLICT (setup_id) DO UPDATE
                     SET setup_name  = EXCLUDED.setup_name,
                         is_current  = TRUE,
                         deployed_at = NOW()""",
                setup_id, setup_name,
            )
    logger.info("Agent 1 DB: active_setup → %d (%s).", setup_id, setup_name)
