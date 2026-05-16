"""
Agent 3 — TradingView Webhook Receiver

POST /webhook from TradingView Pine strategy alert.
Flow: validate secret → dedup → check daily_candidates → enrich → push to queue:signals.
"""

import json
import logging
from datetime import date, datetime

import pytz
from fastapi import APIRouter, HTTPException, Request, status

from app.constants import (
    IST_TIMEZONE, REDIS_KEY_LAST_TV_WEBHOOK,
)
from app.config import settings
from app.database import get_pool
from app.models.signals import TVWebhookPayload, EnrichedSignal
from app.redis_client import (
    get_redis, is_halted, is_trading_active,
    check_dedup, set_dedup, push_signal, get_capital,
)
from app.telegram import send_message, fmt_webhook_rejected

logger = logging.getLogger(__name__)
IST = pytz.timezone(IST_TIMEZONE)

router = APIRouter()


@router.post("/webhook", status_code=status.HTTP_200_OK)
async def receive_webhook(request: Request):
    """
    Entry point for TradingView Pine strategy alerts.
    Validates, deduplicates, and routes to Agent 4 via Redis queue.
    """
    client_ip = request.client.host if request.client else "unknown"

    # Parse body
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body.")

    # Validate secret
    if body.get("secret") != settings.tv_webhook_secret:
        ts = datetime.now(IST).strftime("%H:%M:%S IST")
        await send_message(fmt_webhook_rejected(ts, client_ip))
        logger.warning("Agent 3: Invalid webhook secret from %s.", client_ip)
        raise HTTPException(status_code=401, detail="Unauthorized.")

    # Parse payload
    try:
        payload = TVWebhookPayload(**body)
    except Exception as exc:
        logger.warning("Agent 3: Malformed payload: %s", exc)
        raise HTTPException(status_code=422, detail=str(exc))

    # Update last-seen timestamp for watchdog
    r = await get_redis()
    await r.set(REDIS_KEY_LAST_TV_WEBHOOK, datetime.now(IST).isoformat())

    # Guard: system halted
    if await is_halted():
        logger.info("Agent 3: Signal for %s dropped — system halted.", payload.symbol)
        return {"status": "dropped", "reason": "system_halted"}

    # Guard: trading not active (pre-market scan not complete)
    if not await is_trading_active():
        logger.info("Agent 3: Signal for %s dropped — trading not active.", payload.symbol)
        return {"status": "dropped", "reason": "trading_not_active"}

    # Guard: deduplication (15-min window per symbol)
    if await check_dedup(payload.symbol):
        logger.info("Agent 3: Duplicate signal for %s within dedup window.", payload.symbol)
        return {"status": "dropped", "reason": "dedup"}

    # Guard: symbol must be in today's daily_candidates with matching bias
    candidate = await _get_daily_candidate(payload.symbol, str(payload.direction))
    if candidate is None:
        logger.info(
            "Agent 3: %s %s not in today's candidates — dropped.",
            payload.symbol, payload.direction,
        )
        return {"status": "dropped", "reason": "not_in_candidates"}

    # Enrich signal with live capital
    capital = await get_capital()
    if capital == 0.0:
        logger.warning("Agent 3: Capital is zero — signal dropped.")
        return {"status": "dropped", "reason": "zero_capital"}

    signal = EnrichedSignal(
        symbol=payload.symbol,
        direction=payload.direction,
        setup_id=payload.setup_id,
        entry_price=payload.entry_price,
        stop_price=payload.stop_price,
        target_1=payload.target_1,
        target_2=payload.target_2,
        vrvp_level_used=payload.vrvp_level_used,
        svp_alignment=payload.svp_alignment,
        day_type=payload.day_type,
        volume_ratio=payload.volume_ratio,
        rr_ratio=payload.rr_ratio,
        capital_at_signal=capital,
    )

    # Push to risk queue and mark dedup
    await push_signal(signal.model_dump_json())
    await set_dedup(payload.symbol)

    logger.info(
        "Agent 3: Signal queued — %s %s entry=%.2f rr=%.2f.",
        payload.symbol, payload.direction,
        float(payload.entry_price), float(payload.rr_ratio),
    )
    return {"status": "queued", "signal_id": str(signal.signal_id)}


async def _get_daily_candidate(symbol: str, direction: str) -> dict | None:
    """
    Check today's daily_candidates for symbol with matching bias.
    Returns the row dict or None.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT symbol, bias, score FROM daily_candidates "
            "WHERE date = $1 AND symbol = $2 AND bias = $3",
            date.today(), symbol.upper(), direction,
        )
    return dict(row) if row else None
