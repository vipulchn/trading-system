"""
Agent 4 — Risk Guardian

BLPOP loop on queue:signals (populated by Agent 3).
Validates R:R, position limits, capital-at-risk, and sizes the position.
Approved signals → queue:execution (Agent 5).
Rejected signals → Telegram alert.
"""

import asyncio
import json
import logging
from decimal import Decimal

from app.constants import (
    MIN_RR_RATIO, MAX_RISK_PER_TRADE_PCT, MAX_OPEN_POSITIONS,
    MAX_TOTAL_CAPITAL_AT_RISK_PCT, MAX_STOP_DISTANCE_PCT,
    REDIS_QUEUE_SIGNALS, SETUP_NAMES,
)
from app.database import get_pool
from app.models.signals import EnrichedSignal, ExecutionSignal
from redis.exceptions import TimeoutError as RedisTimeoutError
from app.redis_client import get_redis, push_execution, get_capital, is_halted
from app.telegram import send_message, fmt_signal_rejected

logger = logging.getLogger(__name__)

_running = False


async def run_risk_guardian() -> None:
    """
    Blocking BLPOP loop. Call once at startup as an asyncio task.
    Exits cleanly when the process shuts down (_running is set to False).
    """
    global _running
    _running = True
    logger.info("Agent 4: Risk Guardian started.")
    r = await get_redis()

    while _running:
        try:
            result = await r.blpop(REDIS_QUEUE_SIGNALS, timeout=5)
            if result is None:
                continue  # timeout — loop to check _running
            _, raw = result
            await _process_signal(raw)
        except asyncio.CancelledError:
            break
        except (TimeoutError, RedisTimeoutError):
            continue  # BLPOP socket timeout — queue is empty, just retry
        except Exception as exc:
            logger.error("Agent 4: Unexpected error in BLPOP loop: %s", exc)
            await asyncio.sleep(2)

    logger.info("Agent 4: Risk Guardian stopped.")


async def stop_risk_guardian() -> None:
    global _running
    _running = False


# ── Signal processing ─────────────────────────────────────────────────────────

async def _process_signal(raw: str) -> None:
    try:
        data = json.loads(raw)
        signal = EnrichedSignal(**data)
    except Exception as exc:
        logger.error("Agent 4: Failed to deserialize signal: %s", exc)
        return

    if await is_halted():
        logger.info("Agent 4: System halted — signal for %s dropped.", signal.symbol)
        return

    rejection = await _validate(signal)
    if rejection:
        logger.info("Agent 4: %s rejected — %s.", signal.symbol, rejection)
        await send_message(fmt_signal_rejected(signal.symbol, signal.direction, rejection))
        return

    # Position sizing
    entry = float(signal.entry_price)
    stop = float(signal.stop_price)
    capital = float(signal.capital_at_signal)

    per_share_risk = abs(entry - stop)
    if per_share_risk == 0:
        await send_message(fmt_signal_rejected(signal.symbol, signal.direction, "zero per-share risk"))
        return

    risk_inr = capital * float(MAX_RISK_PER_TRADE_PCT)
    quantity = int(risk_inr / per_share_risk)

    if quantity < 1:
        await send_message(
            fmt_signal_rejected(signal.symbol, signal.direction,
                                 f"qty=0 (risk ₹{risk_inr:.0f}, per-share ₹{per_share_risk:.2f})")
        )
        return

    risk_amount_inr = Decimal(str(round(quantity * per_share_risk, 2)))

    exec_signal = ExecutionSignal(
        signal_id=signal.signal_id,
        received_at=signal.received_at,
        symbol=signal.symbol,
        direction=signal.direction,
        setup_id=signal.setup_id,
        entry_price=signal.entry_price,
        stop_price=signal.stop_price,
        target_1=signal.target_1,
        target_2=signal.target_2,
        vrvp_level_used=signal.vrvp_level_used,
        svp_alignment=signal.svp_alignment,
        day_type=signal.day_type,
        volume_ratio=signal.volume_ratio,
        rr_ratio=signal.rr_ratio,
        capital_at_signal=signal.capital_at_signal,
        quantity=quantity,
        risk_amount_inr=risk_amount_inr,
    )

    await push_execution(exec_signal.model_dump_json())
    logger.info(
        "Agent 4: %s %s APPROVED — qty=%d risk=₹%.2f.",
        signal.symbol, signal.direction, quantity, float(risk_amount_inr),
    )


async def _validate(signal: EnrichedSignal) -> str | None:
    """
    Run all pre-execution checks. Returns a rejection reason string, or None if approved.
    """
    entry = float(signal.entry_price)
    stop = float(signal.stop_price)
    capital = float(signal.capital_at_signal)

    # R:R check
    if signal.rr_ratio < MIN_RR_RATIO:
        return f"R:R {float(signal.rr_ratio):.2f} < {float(MIN_RR_RATIO):.1f}"

    # Stop distance check (max 1% from entry)
    stop_distance_pct = abs(entry - stop) / entry
    if Decimal(str(stop_distance_pct)) > MAX_STOP_DISTANCE_PCT:
        return f"stop distance {stop_distance_pct*100:.2f}% > {float(MAX_STOP_DISTANCE_PCT)*100:.0f}%"

    pool = await get_pool()
    async with pool.acquire() as conn:
        # Open positions count
        open_count = await conn.fetchval(
            "SELECT COUNT(*) FROM open_trades WHERE date = CURRENT_DATE AND status NOT IN ('CLOSED', 'CANCELLED')"
        )
        if open_count >= MAX_OPEN_POSITIONS:
            return f"max concurrent positions ({MAX_OPEN_POSITIONS}) reached"

        # Duplicate symbol check
        existing = await conn.fetchval(
            "SELECT COUNT(*) FROM open_trades WHERE date = CURRENT_DATE "
            "AND symbol = $1 AND status NOT IN ('CLOSED', 'CANCELLED')",
            signal.symbol,
        )
        if existing > 0:
            return f"already have an open trade in {signal.symbol}"

        # Aggregate capital already at risk
        risk_rows = await conn.fetch(
            "SELECT entry_price, stop_price, quantity, direction FROM open_trades "
            "WHERE date = CURRENT_DATE AND status NOT IN ('CLOSED', 'CANCELLED')"
        )

    committed_risk = sum(
        abs(float(r["entry_price"]) - float(r["stop_price"])) * int(r["quantity"])
        for r in risk_rows
    )
    new_risk = abs(entry - stop) * int(capital * float(MAX_RISK_PER_TRADE_PCT) / max(abs(entry - stop), 0.01))
    total_risk_pct = (committed_risk + new_risk) / capital if capital > 0 else 1.0
    if Decimal(str(total_risk_pct)) > MAX_TOTAL_CAPITAL_AT_RISK_PCT:
        return f"total capital at risk {total_risk_pct*100:.2f}% would exceed {float(MAX_TOTAL_CAPITAL_AT_RISK_PCT)*100:.0f}%"

    return None
