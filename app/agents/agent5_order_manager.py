"""
Agent 5 — Order Manager

BLPOP loop on queue:execution (populated by Agent 4).
Places entry LIMIT order → polls for fill → places SL + T1 orders → monitors to exit.
Force-exit at 15:15 IST is called directly by the scheduler.
"""

import asyncio
import json
import logging
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

import pytz

from app.constants import (
    IST_TIMEZONE, REDIS_QUEUE_EXECUTION, SETUP_NAMES,
    DAILY_LOSS_LIMIT_PCT, FORCE_EXIT_HOUR, FORCE_EXIT_MINUTE,
)
from app.database import get_pool
from app.models.signals import ExecutionSignal
from app.redis_client import get_redis, get_capital, set_capital, set_halt, is_halted
from app.telegram import (
    send_message,
    fmt_entry, fmt_t1_hit, fmt_trade_closed, fmt_stop_hit,
    fmt_force_exit, fmt_daily_loss_limit,
)
from app.dhan_client import (
    place_limit_order, place_sl_order, place_market_order, cancel_order,
    get_order_status, get_fill_price,
    get_open_intraday_positions, cancel_open_orders_for_symbol,
    STATUS_TRADED, STATUS_CANCELLED, STATUS_REJECTED, TERMINAL_STATUSES,
)
from app.symbol_master import dhan_security_id, dhan_trading_symbol

logger = logging.getLogger(__name__)
IST = pytz.timezone(IST_TIMEZONE)

_running = False
_ENTRY_POLL_INTERVAL_S = 2
_ENTRY_TIMEOUT_S = 180        # 3 minutes to fill entry
_MONITOR_INTERVAL_S = 30      # check open trades every 30s


# ── Background tasks ──────────────────────────────────────────────────────────

async def run_order_manager() -> None:
    """BLPOP loop for new execution signals. Runs as an asyncio task."""
    global _running
    _running = True
    logger.info("Agent 5: Order Manager started.")
    r = await get_redis()

    while _running:
        try:
            result = await r.blpop(REDIS_QUEUE_EXECUTION, timeout=5)
            if result is None:
                continue
            _, raw = result
            asyncio.create_task(_execute_signal(raw))
        except asyncio.CancelledError:
            break
        except TimeoutError:
            continue  # BLPOP socket timeout — queue is empty, just retry
        except Exception as exc:
            logger.error("Agent 5: BLPOP loop error: %s", exc)
            await asyncio.sleep(2)

    logger.info("Agent 5: Order Manager stopped.")


async def run_position_monitor() -> None:
    """
    Periodic monitor for all open trades.
    Checks SL/T1/T2 fill status and handles exits.
    Runs as a separate asyncio task.
    """
    global _running
    logger.info("Agent 5: Position Monitor started.")

    while _running:
        try:
            await _monitor_open_trades()
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error("Agent 5: Monitor loop error: %s", exc)
        await asyncio.sleep(_MONITOR_INTERVAL_S)

    logger.info("Agent 5: Position Monitor stopped.")


async def stop_order_manager() -> None:
    global _running
    _running = False


# ── Entry execution ───────────────────────────────────────────────────────────

async def _execute_signal(raw: str) -> None:
    try:
        data = json.loads(raw)
        signal = ExecutionSignal(**data)
    except Exception as exc:
        logger.error("Agent 5: Failed to deserialize ExecutionSignal: %s", exc)
        return

    if await is_halted():
        logger.info("Agent 5: System halted — signal for %s dropped.", signal.symbol)
        return

    symbol = signal.symbol
    sec_id = dhan_security_id(symbol)
    trading_sym = dhan_trading_symbol(symbol)

    if not sec_id or not trading_sym:
        logger.error("Agent 5: No Dhan mapping for %s — signal dropped.", symbol)
        return

    tx_type = "BUY" if signal.direction == "Long" else "SELL"
    entry_price = float(signal.entry_price)

    logger.info("Agent 5: Placing entry LIMIT %s %s qty=%d @ ₹%.2f.",
                tx_type, symbol, signal.quantity, entry_price)

    order_id = await place_limit_order(
        security_id=sec_id,
        trading_symbol=trading_sym,
        transaction_type=tx_type,
        quantity=signal.quantity,
        price=entry_price,
    )
    if not order_id:
        logger.error("Agent 5: Entry order placement failed for %s.", symbol)
        return

    # Write ENTRY_PENDING record immediately
    trade_id = await _insert_open_trade(signal, order_id)
    if not trade_id:
        logger.error("Agent 5: Failed to insert open_trade for %s.", symbol)
        await cancel_order(order_id)
        return

    # Poll for fill
    fill_price = await _poll_fill(order_id)
    if fill_price is None:
        logger.warning("Agent 5: Entry order %s not filled within timeout — cancelling.", order_id)
        await cancel_order(order_id)
        await _cancel_open_trade(trade_id)
        return

    logger.info("Agent 5: %s entry FILLED @ ₹%.2f (trade %s).", symbol, fill_price, trade_id)
    await _update_trade_entry_filled(trade_id, fill_price)

    # Place SL and T1 orders
    sl_order_id, t1_order_id = await _place_protective_orders(
        signal, sec_id, trading_sym, fill_price
    )

    await _update_trade_orders(trade_id, sl_order_id, t1_order_id)

    # Telegram confirmation
    capital = await get_capital()
    setup_name = SETUP_NAMES.get(signal.setup_id, f"Setup {signal.setup_id}")
    risk_pct = float(signal.risk_amount_inr) / capital * 100 if capital > 0 else 0
    await send_message(fmt_entry(
        symbol=symbol,
        direction=signal.direction,
        entry=fill_price,
        qty=signal.quantity,
        stop=float(signal.stop_price),
        t1=float(signal.target_1),
        t2=float(signal.target_2),
        risk_inr=float(signal.risk_amount_inr),
        risk_pct=risk_pct,
        setup_name=setup_name,
    ))


async def _poll_fill(order_id: str) -> float | None:
    """Poll order status every 2s for up to 3 minutes. Returns fill price or None."""
    iterations = _ENTRY_TIMEOUT_S // _ENTRY_POLL_INTERVAL_S
    for _ in range(iterations):
        await asyncio.sleep(_ENTRY_POLL_INTERVAL_S)
        status = await get_order_status(order_id)
        if status == STATUS_TRADED:
            return await get_fill_price(order_id)
        if status in (STATUS_CANCELLED, STATUS_REJECTED):
            return None
    return None


async def _place_protective_orders(
    signal: ExecutionSignal,
    sec_id: str,
    trading_sym: str,
    fill_price: float,
) -> tuple[str | None, str | None]:
    """Place SL stop-loss and T1 limit orders after entry is filled."""
    # SL: opposite direction to entry
    sl_tx = "SELL" if signal.direction == "Long" else "BUY"
    t1_tx = "SELL" if signal.direction == "Long" else "BUY"

    sl_order_id = await place_sl_order(
        security_id=sec_id,
        trading_symbol=trading_sym,
        transaction_type=sl_tx,
        quantity=signal.quantity,
        trigger_price=float(signal.stop_price),
    )

    t1_order_id = await place_limit_order(
        security_id=sec_id,
        trading_symbol=trading_sym,
        transaction_type=t1_tx,
        quantity=signal.quantity,
        price=float(signal.target_1),
    )

    logger.info(
        "Agent 5: Protective orders placed — SL=%s T1=%s.",
        sl_order_id, t1_order_id,
    )
    return sl_order_id, t1_order_id


# ── Position monitoring ───────────────────────────────────────────────────────

async def _monitor_open_trades() -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT trade_id, symbol, direction, setup_id, entry_price, stop_price, "
            "target_1, target_2, quantity, status, "
            "entry_order_id, sl_order_id, t1_order_id, t2_order_id "
            "FROM open_trades "
            "WHERE date = CURRENT_DATE AND status NOT IN ('CLOSED', 'CANCELLED', 'ENTRY_PENDING')"
        )

    for row in rows:
        trade = dict(row)
        try:
            await _check_trade(trade)
        except Exception as exc:
            logger.error("Agent 5: Monitor error for trade %s: %s", trade["trade_id"], exc)


async def _check_trade(trade: dict) -> None:
    trade_id = str(trade["trade_id"])
    symbol = trade["symbol"]
    status = trade["status"]
    setup_name = SETUP_NAMES.get(trade["setup_id"], f"Setup {trade['setup_id']}")

    if status == "OPEN":
        # Check SL
        if trade["sl_order_id"]:
            sl_status = await get_order_status(trade["sl_order_id"])
            if sl_status == STATUS_TRADED:
                fill = await get_fill_price(trade["sl_order_id"])
                exit_price = fill or float(trade["stop_price"])
                # Cancel T1 if still open
                if trade["t1_order_id"]:
                    await cancel_order(trade["t1_order_id"])
                pnl = _calc_pnl(trade, exit_price)
                r_mult = _calc_r(trade, exit_price)
                await _close_trade(trade_id, exit_price, pnl, "SL_HIT")
                await send_message(fmt_stop_hit(
                    symbol=symbol,
                    stop_price=exit_price,
                    loss_inr=abs(pnl),
                    daily_pnl=await _get_daily_pnl(),
                    remaining_pct=await _daily_loss_remaining_pct(),
                ))
                await _check_daily_loss_limit()
                return

        # Check T1
        if trade["t1_order_id"]:
            t1_status = await get_order_status(trade["t1_order_id"])
            if t1_status == STATUS_TRADED:
                fill = await get_fill_price(trade["t1_order_id"])
                t1_price = fill or float(trade["target_1"])
                entry_price = float(trade["entry_price"])

                # Cancel original SL, place breakeven SL + T2
                if trade["sl_order_id"]:
                    await cancel_order(trade["sl_order_id"])

                sec_id = dhan_security_id(symbol)
                trading_sym = dhan_trading_symbol(symbol)
                sl_tx = "SELL" if trade["direction"] == "Long" else "BUY"

                # Breakeven SL
                be_sl_id = await place_sl_order(
                    security_id=sec_id,
                    trading_symbol=trading_sym,
                    transaction_type=sl_tx,
                    quantity=trade["quantity"],
                    trigger_price=entry_price,
                )
                # T2 limit order
                t2_tx = "SELL" if trade["direction"] == "Long" else "BUY"
                t2_order_id = await place_limit_order(
                    security_id=sec_id,
                    trading_symbol=trading_sym,
                    transaction_type=t2_tx,
                    quantity=trade["quantity"],
                    price=float(trade["target_2"]),
                )
                await _update_trade_t1_hit(trade_id, be_sl_id, t2_order_id)
                await send_message(fmt_t1_hit(symbol, t1_price, entry_price))
                return

    elif status == "T1_HIT":
        # Check T2
        if trade["t2_order_id"]:
            t2_status = await get_order_status(trade["t2_order_id"])
            if t2_status == STATUS_TRADED:
                fill = await get_fill_price(trade["t2_order_id"])
                exit_price = fill or float(trade["target_2"])
                if trade["sl_order_id"]:
                    await cancel_order(trade["sl_order_id"])
                pnl = _calc_pnl(trade, exit_price)
                r_mult = _calc_r(trade, exit_price)
                await _close_trade(trade_id, exit_price, pnl, "T2_HIT")
                await send_message(fmt_trade_closed(symbol, exit_price, pnl, r_mult, setup_name))
                return

        # Check breakeven SL
        if trade["sl_order_id"]:
            sl_status = await get_order_status(trade["sl_order_id"])
            if sl_status == STATUS_TRADED:
                fill = await get_fill_price(trade["sl_order_id"])
                exit_price = fill or float(trade["entry_price"])
                if trade["t2_order_id"]:
                    await cancel_order(trade["t2_order_id"])
                pnl = _calc_pnl(trade, exit_price)
                r_mult = _calc_r(trade, exit_price)
                await _close_trade(trade_id, exit_price, pnl, "BE_SL_HIT")
                await send_message(fmt_trade_closed(symbol, exit_price, pnl, r_mult, setup_name))


# ── Force exit at 15:15 ───────────────────────────────────────────────────────

async def run_force_exit() -> None:
    """
    Called by scheduler at 15:15 IST.
    Cancels all pending orders, market-closes all open intraday positions,
    and marks all open_trades as CLOSED in the DB.
    """
    logger.info("Agent 5: Force exit triggered.")

    # Get open positions from Dhan
    open_positions = await get_open_intraday_positions()
    if not open_positions:
        logger.info("Agent 5: No open positions at force exit.")
        return

    summaries = []
    total_pnl = 0.0

    for pos in open_positions:
        symbol_raw = pos.get("tradingSymbol", "")
        symbol = symbol_raw.replace("-EQ", "").upper()
        qty = pos["quantity"]
        direction = pos["direction"]
        unrealized = pos.get("unrealized_pnl", 0.0)

        # Cancel all pending orders for this symbol first
        await cancel_open_orders_for_symbol(symbol)

        # Place market exit
        sec_id = dhan_security_id(symbol)
        trading_sym = dhan_trading_symbol(symbol)
        exit_tx = "SELL" if direction == "Long" else "BUY"

        if sec_id and trading_sym:
            exit_order_id = await place_market_order(
                security_id=sec_id,
                trading_symbol=trading_sym,
                transaction_type=exit_tx,
                quantity=qty,
            )
            logger.info("Agent 5: Force exit %s %s qty=%d → order %s.",
                        exit_tx, symbol, qty, exit_order_id)
        else:
            logger.error("Agent 5: No Dhan mapping for %s — cannot force exit.", symbol)

        summaries.append({"symbol": symbol, "pnl": unrealized})
        total_pnl += unrealized

    # Mark all open DB trades as CLOSED
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE open_trades SET status = 'CLOSED' "
            "WHERE date = CURRENT_DATE AND status NOT IN ('CLOSED', 'CANCELLED')"
        )

    await send_message(fmt_force_exit(summaries, total_pnl))
    logger.info("Agent 5: Force exit complete. Session P&L ≈ ₹%.2f.", total_pnl)


# ── DB helpers ────────────────────────────────────────────────────────────────

async def _insert_open_trade(signal: ExecutionSignal, entry_order_id: str) -> str | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO open_trades "
            "(date, symbol, direction, setup_id, entry_order_id, "
            " entry_price, stop_price, target_1, target_2, quantity, status) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'ENTRY_PENDING') "
            "RETURNING trade_id",
            date.today(), signal.symbol, signal.direction, signal.setup_id, entry_order_id,
            float(signal.entry_price), float(signal.stop_price),
            float(signal.target_1), float(signal.target_2), signal.quantity,
        )
    return str(row["trade_id"]) if row else None


async def _update_trade_entry_filled(trade_id: str, fill_price: float) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE open_trades SET status='OPEN', entry_price=$2, opened_at=NOW() WHERE trade_id=$1",
            trade_id, fill_price,
        )


async def _update_trade_orders(trade_id: str, sl_order_id: str | None, t1_order_id: str | None) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE open_trades SET sl_order_id=$2, t1_order_id=$3 WHERE trade_id=$1",
            trade_id, sl_order_id, t1_order_id,
        )


async def _update_trade_t1_hit(trade_id: str, be_sl_order_id: str | None, t2_order_id: str | None) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE open_trades SET status='T1_HIT', sl_order_id=$2, t2_order_id=$3 WHERE trade_id=$1",
            trade_id, be_sl_order_id, t2_order_id,
        )


async def _cancel_open_trade(trade_id: str) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE open_trades SET status='CANCELLED' WHERE trade_id=$1", trade_id
        )


async def _close_trade(trade_id: str, exit_price: float, pnl: float, reason: str) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE open_trades SET status='CLOSED' WHERE trade_id=$1 "
            "RETURNING symbol, direction, setup_id, entry_price, quantity, opened_at",
            trade_id,
        )
        if row:
            await conn.execute(
                "INSERT INTO trade_history "
                "(trade_id, date, symbol, direction, setup_id, entry_price, exit_price, "
                " quantity, gross_pnl, net_pnl, exit_reason, opened_at) "
                "VALUES ($1, CURRENT_DATE, $2, $3, $4, $5, $6, $7, $8, $8, $9, $10)",
                trade_id, row["symbol"], row["direction"], row["setup_id"],
                float(row["entry_price"]), exit_price, int(row["quantity"]),
                round(pnl, 2), reason, row["opened_at"],
            )


async def _get_daily_pnl() -> float:
    pool = await get_pool()
    async with pool.acquire() as conn:
        val = await conn.fetchval(
            "SELECT COALESCE(SUM(net_pnl), 0) FROM trade_history WHERE date = CURRENT_DATE"
        )
    return float(val or 0)


async def _daily_loss_remaining_pct() -> float:
    capital = await get_capital()
    if capital == 0:
        return 0.0
    daily_pnl = await _get_daily_pnl()
    limit = capital * float(DAILY_LOSS_LIMIT_PCT)
    used = max(-daily_pnl, 0)
    return max((limit - used) / limit * 100, 0)


async def _check_daily_loss_limit() -> None:
    """Halt trading if daily loss limit is reached."""
    capital = await get_capital()
    if capital == 0:
        return
    daily_pnl = await _get_daily_pnl()
    loss_pct = -daily_pnl / capital if daily_pnl < 0 else 0
    if Decimal(str(loss_pct)) >= DAILY_LOSS_LIMIT_PCT:
        await set_halt(True)
        await send_message(fmt_daily_loss_limit())
        logger.warning("Agent 5: Daily loss limit hit — system halted.")


# ── P&L helpers ───────────────────────────────────────────────────────────────

def _calc_pnl(trade: dict, exit_price: float) -> float:
    entry = float(trade["entry_price"])
    qty = int(trade["quantity"])
    if trade["direction"] == "Long":
        return round((exit_price - entry) * qty, 2)
    return round((entry - exit_price) * qty, 2)


def _calc_r(trade: dict, exit_price: float) -> float:
    entry = float(trade["entry_price"])
    stop = float(trade["stop_price"])
    per_share_risk = abs(entry - stop)
    if per_share_risk == 0:
        return 0.0
    pnl_per_share = (exit_price - entry) if trade["direction"] == "Long" else (entry - exit_price)
    return round(pnl_per_share / per_share_risk, 2)
