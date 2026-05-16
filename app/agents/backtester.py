"""
backtester.py — Agent 1 orchestration (replaces agent1_backtester.py).

Schedule: Sunday 06:00 IST via APScheduler.
Trigger:  POST /backtest (system router).

Setup simulators  → agent1_setups.py
DB write helpers  → agent1_db.py
"""

import asyncio
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import pytz

from app.constants import (
    IST_TIMEZONE, SETUP_NAMES,
    VRVP_LOOKBACK_SESSIONS,
    MIN_AVG_DAILY_TURNOVER_CR, MIN_AVG_DAILY_VOLUME,
    UNIVERSE_TOP_N, MIN_BACKTEST_TRADES,
    WALKFORWARD_TRAIN_MONTHS, WALKFORWARD_OOS_MONTHS, WALKFORWARD_VALIDATION_RATIO,
    SCORE_WEIGHT_SHARPE, SCORE_WEIGHT_WIN_RATE, SCORE_WEIGHT_AVG_R,
    SUSPICIOUS_WIN_RATE,
    BROKERAGE_PCT, STT_PCT, GST_ON_BROKERAGE,
    EXCHANGE_CHARGE_PER_ORDER, SEBI_CHARGE_PCT,
)
from app.telegram import send_message, fmt_backtest_complete
from app.vrvp import split_into_sessions, rolling_vrvp
from app.agents.agent1_setups import (
    setup1_poc_magnet, setup2_va_breakout, setup3_hvn_rejection,
    setup4_lvn_acceleration, setup5_open_drive,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone(IST_TIMEZONE)
TOTAL_MONTHS = WALKFORWARD_TRAIN_MONTHS + WALKFORWARD_OOS_MONTHS


# ── Entry point ───────────────────────────────────────────────────────────────

async def run_universe_backtest() -> None:
    logger.info("Agent 1: Universe backtest starting.")
    started_at = datetime.now(IST)
    try:
        from app.symbol_master import load_symbol_masters, is_loaded
        if not is_loaded():
            await load_symbol_masters()

        symbols = await _build_universe()
        logger.info("Agent 1: Universe = %d symbols.", len(symbols))

        results: list[dict] = []
        sem = asyncio.Semaphore(5)

        async def process_one(sym: str) -> None:
            async with sem:
                try:
                    candles = await _fetch_ohlcv(sym)
                    if candles is None:
                        return
                    result = _backtest_symbol(sym, candles)
                    if result:
                        results.append(result)
                except Exception as exc:
                    logger.warning("Agent 1: %s failed: %s", sym, exc)

        await asyncio.gather(*[process_one(s) for s in symbols])

        if not results:
            await send_message("Agent 1: No symbols passed thresholds. Watchlist unchanged.")
            return

        ranked = _rank_and_select(results)
        active_setup_id = _select_active_setup(ranked)
        active_setup_name = SETUP_NAMES[active_setup_id]

        from app.agents.agent1_db import (
            write_backtest_results, write_universe,
            write_watchlist, write_active_setup, get_current_watchlist_symbols,
        )
        await write_backtest_results(results)
        await write_universe(results)
        await write_active_setup(active_setup_id, active_setup_name)  # must precede watchlist (FK)
        prev = await get_current_watchlist_symbols()
        await write_watchlist(ranked, active_setup_id)

        new_syms = {r["symbol"] for r in ranked}
        await send_message(fmt_backtest_complete(
            date=started_at.strftime("%Y-%m-%d"),
            setup_name=active_setup_name,
            setup_id=active_setup_id,
            top5=[r["symbol"] for r in ranked[:5]],
            new_entries=sorted(new_syms - prev),
            dropped=sorted(prev - new_syms),
            weakest=[r["symbol"] for r in ranked[-5:]],
        ))
        logger.info("Agent 1: Complete. Active setup: %s.", active_setup_name)

    except Exception as exc:
        logger.exception("Agent 1: Backtest failed: %s", exc)
        await send_message(f"Agent 1 BACKTEST FAILED: {exc}")


# ── Universe building ─────────────────────────────────────────────────────────

async def _build_universe() -> list[str]:
    from app.symbol_master import get_fo_eligible_symbols, angel_token
    from app.angel_client import get_candle_data

    fo_symbols = get_fo_eligible_symbols()
    today = date.today()
    from_date = (today - timedelta(days=30)).strftime("%Y-%m-%d") + " 09:15"
    to_date = today.strftime("%Y-%m-%d") + " 15:30"
    passing: list[str] = []
    sem = asyncio.Semaphore(10)

    async def check_one(symbol: str) -> None:
        token = angel_token(symbol)
        if not token:
            return
        async with sem:
            try:
                daily = await get_candle_data(token, "ONE_DAY", from_date, to_date)
                if len(daily) < 10:
                    return
                df = pd.DataFrame(daily)
                df["turnover_cr"] = df["close"] * df["volume"] / 1e7
                if (df["turnover_cr"].mean() >= MIN_AVG_DAILY_TURNOVER_CR
                        and df["volume"].mean() >= MIN_AVG_DAILY_VOLUME):
                    passing.append(symbol)
                await asyncio.sleep(0.1)
            except Exception as exc:
                logger.debug("Agent 1: turnover check %s: %s", symbol, exc)

    await asyncio.gather(*[check_one(s) for s in fo_symbols])
    logger.info("Agent 1: %d symbols pass universe filters.", len(passing))
    return passing


async def _fetch_ohlcv(symbol: str) -> Optional[list[dict]]:
    from app.symbol_master import angel_token
    from app.angel_client import get_six_months_15min
    token = angel_token(symbol)
    if not token:
        return None
    candles = await get_six_months_15min(token)
    if len(candles) < MIN_BACKTEST_TRADES * 2:
        return None
    return candles


# ── Backtester core ───────────────────────────────────────────────────────────

def _backtest_symbol(symbol: str, raw_candles: list[dict]) -> Optional[dict]:
    df = pd.DataFrame(raw_candles)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(IST)
    df = df.sort_values("timestamp").reset_index(drop=True)

    all_dates = sorted(df["timestamp"].dt.date.unique())
    if len(all_dates) < 40:
        return None

    cutoff_idx = int(len(all_dates) * (WALKFORWARD_TRAIN_MONTHS / TOTAL_MONTHS))
    train_cutoff = all_dates[cutoff_idx]
    train_df = df[df["timestamp"].dt.date < train_cutoff].copy()
    oos_df = df[df["timestamp"].dt.date >= train_cutoff].copy()

    best_result: Optional[dict] = None
    best_score = -999.0

    for setup_id in SETUP_NAMES:
        train_trades = _simulate_setup(setup_id, train_df)
        if len(train_trades) < MIN_BACKTEST_TRADES:
            continue
        bt = _compute_metrics(train_trades)
        if bt["win_rate"] > SUSPICIOUS_WIN_RATE:
            continue
        oos_trades = _simulate_setup(setup_id, oos_df)
        if len(oos_trades) < 5:
            continue
        oos = _compute_metrics(oos_trades)
        if oos["sharpe"] < float(WALKFORWARD_VALIDATION_RATIO) * bt["sharpe"]:
            continue
        composite = (bt["sharpe"] * SCORE_WEIGHT_SHARPE
                     + bt["win_rate"] * SCORE_WEIGHT_WIN_RATE
                     + bt["avg_r"] * SCORE_WEIGHT_AVG_R)
        if composite > best_score:
            best_score = composite
            best_result = {
                "symbol": symbol, "best_setup_id": setup_id,
                "composite_score": composite,
                "win_rate": bt["win_rate"], "avg_r": bt["avg_r"],
                "sharpe": bt["sharpe"], "max_dd": bt["max_dd"],
                "backtest_sharpe": bt["sharpe"],
                "validation_sharpe": oos["sharpe"],
                "trade_count": len(train_trades),
            }
    return best_result


def _simulate_setup(setup_id: int, df: pd.DataFrame) -> list[dict]:
    if len(df) < 50:
        return []
    sessions = split_into_sessions(df)
    vrvps = rolling_vrvp(sessions, lookback=VRVP_LOOKBACK_SESSIONS)
    dispatch = {
        1: setup1_poc_magnet, 2: setup2_va_breakout,
        3: setup3_hvn_rejection, 4: setup4_lvn_acceleration,
        5: setup5_open_drive,
    }
    fn = dispatch.get(setup_id)
    if fn is None:
        return []
    trades: list[dict] = []
    for day_idx, (session_df, vrvp) in enumerate(zip(sessions, vrvps)):
        if vrvp is None:
            continue
        prev = sessions[max(0, day_idx - 10):day_idx]
        avg_vol = pd.concat(prev)["volume"].mean() if prev else 0
        candles = session_df.to_dict("records")
        for t in fn(candles, vrvp, avg_vol):
            t["pnl_after_costs"] = _apply_costs(t["entry"], t["exit"], t["quantity"])
            trades.append(t)
    return trades


# ── Metrics and costs ─────────────────────────────────────────────────────────

def _compute_metrics(trades: list[dict]) -> dict:
    if not trades:
        return {"win_rate": 0, "avg_r": 0, "sharpe": 0, "max_dd": 0}
    r = np.array([t["r_multiple"] for t in trades])
    win_rate = float(np.mean(r > 0))
    avg_r = float(np.mean(r))
    sharpe = float(avg_r / np.std(r) * np.sqrt(min(len(r), 252))) if np.std(r) > 0 else 0.0
    cum = np.cumsum(r)
    max_dd = float(abs((cum - np.maximum.accumulate(cum)).min())) if len(cum) > 0 else 0.0
    return {"win_rate": win_rate, "avg_r": avg_r, "sharpe": sharpe, "max_dd": max_dd}


def _apply_costs(entry: float, exit_price: float, qty: int) -> float:
    brokerage = float(BROKERAGE_PCT) * (entry + exit_price) * qty
    stt = float(STT_PCT) * exit_price * qty
    gst = brokerage * float(GST_ON_BROKERAGE)
    exchange = float(EXCHANGE_CHARGE_PER_ORDER) * 2
    sebi = float(SEBI_CHARGE_PCT) * (entry + exit_price) * qty
    return -(brokerage + stt + gst + exchange + sebi)


# ── Ranking ───────────────────────────────────────────────────────────────────

def _rank_and_select(results: list[dict]) -> list[dict]:
    for r in results:
        r["composite_score"] = (r["sharpe"] * SCORE_WEIGHT_SHARPE
                                + r["win_rate"] * SCORE_WEIGHT_WIN_RATE
                                + r["avg_r"] * SCORE_WEIGHT_AVG_R)
    return sorted(results, key=lambda x: x["composite_score"], reverse=True)[:UNIVERSE_TOP_N]


def _select_active_setup(ranked: list[dict]) -> int:
    scores: dict[int, float] = defaultdict(float)
    for r in ranked:
        scores[r["best_setup_id"]] += r["composite_score"]
    return max(scores, key=lambda k: scores[k])
