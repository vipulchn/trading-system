"""
System control router — /health, /status, /resume, /halt.
All endpoints except /health require the TV webhook secret as Bearer token.
"""

import logging
from datetime import date, datetime

import pytz
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.config import settings
from app.constants import IST_TIMEZONE
from app.database import get_pool
from app.redis_client import (
    get_capital, is_halted, is_trading_active,
    set_halt, set_trading_active,
)
from app.telegram import send_message

logger = logging.getLogger(__name__)
IST = pytz.timezone(IST_TIMEZONE)
router = APIRouter()
bearer = HTTPBearer()


def _require_secret(creds: HTTPAuthorizationCredentials = Depends(bearer)):
    if creds.credentials != settings.tv_webhook_secret:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden.")
    return creds


@router.get("/health")
async def health():
    """Public liveness probe — used by Railway health checks."""
    return {"status": "ok", "ts": datetime.now(IST).isoformat()}


@router.get("/status", dependencies=[Depends(_require_secret)])
async def system_status():
    """Full system status snapshot."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        open_trades = await conn.fetch(
            "SELECT symbol, direction, status FROM open_trades "
            "WHERE date = $1 AND status NOT IN ('CLOSED', 'CANCELLED')",
            date.today(),
        )
        today_pnl = await conn.fetchval(
            "SELECT COALESCE(SUM(net_pnl), 0) FROM trade_history WHERE date = $1",
            date.today(),
        )
        candidates = await conn.fetchval(
            "SELECT COUNT(*) FROM daily_candidates WHERE date = $1", date.today()
        )

    return {
        "halted": await is_halted(),
        "trading_active": await is_trading_active(),
        "capital_inr": await get_capital(),
        "today_pnl_inr": float(today_pnl or 0),
        "open_trades": [dict(r) for r in open_trades],
        "daily_candidates": candidates,
        "ts": datetime.now(IST).isoformat(),
    }


@router.post("/halt", dependencies=[Depends(_require_secret)])
async def halt_system():
    """Manually halt all trading."""
    await set_halt(True)
    await send_message("🛑 System manually <b>HALTED</b> via /halt.")
    logger.warning("System halted via /halt endpoint.")
    return {"status": "halted"}


@router.post("/resume", dependencies=[Depends(_require_secret)])
async def resume_system():
    """Clear the halt flag and re-enable trading."""
    await set_halt(False)
    await send_message("✅ System manually <b>RESUMED</b> via /resume.")
    logger.info("System resumed via /resume endpoint.")
    return {"status": "resumed"}


@router.post("/trading-active", dependencies=[Depends(_require_secret)])
async def set_active(active: bool):
    """Manually set trading:active flag."""
    await set_trading_active(active)
    state = "ACTIVE" if active else "INACTIVE"
    logger.info("trading:active manually set to %s.", state)
    return {"trading_active": active}
