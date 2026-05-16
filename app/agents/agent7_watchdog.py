"""
Agent 7 — Health Watchdog

Runs every 5 minutes via APScheduler.
Checks Redis, PostgreSQL, Dhan, capital sync freshness, memory,
and open-position/DB consistency.
"""

import logging
import os
import time
from datetime import datetime, timedelta

import pytz

from app.constants import (
    IST_TIMEZONE,
    WATCHDOG_TV_STALE_MINUTES,
    WATCHDOG_RETRY_INTERVAL_SECONDS,
    WATCHDOG_MAX_RETRIES,
    WATCHDOG_REDIS_TIMEOUT_S,
    WATCHDOG_PG_TIMEOUT_S,
    WATCHDOG_DHAN_TIMEOUT_S,
    WATCHDOG_MEMORY_LIMIT_MB,
    REDIS_KEY_LAST_TV_WEBHOOK,
)
from app.telegram import send_message, fmt_critical, fmt_component_restored, fmt_component_unrecoverable

logger = logging.getLogger(__name__)
IST = pytz.timezone(IST_TIMEZONE)

# Track whether a component was previously in error so we can send a "restored" alert
_component_errors: dict[str, int] = {}   # component → consecutive failure count
_component_alerted: set[str] = set()     # components with outstanding alert sent


async def run_health_watchdog() -> None:
    logger.debug("Agent 7: Watchdog tick.")
    now = datetime.now(IST)
    is_trading_day = now.weekday() < 5   # Mon-Fri

    checks = [
        ("Redis",      _check_redis),
        ("PostgreSQL", _check_postgres),
        ("Dhan",       _check_dhan),
    ]
    if is_trading_day:
        checks.append(("CapitalSync", _check_capital_sync))
        checks.append(("TVWebhook",   _check_tv_staleness))
        checks.append(("Positions",   _check_position_consistency))

    checks.append(("Memory", _check_memory))

    for name, check_fn in checks:
        try:
            ok, detail = await check_fn()
        except Exception as exc:
            ok, detail = False, str(exc)

        if ok:
            if name in _component_alerted:
                _component_alerted.discard(name)
                _component_errors[name] = 0
                await send_message(fmt_component_restored(name))
                logger.info("Agent 7: %s restored.", name)
        else:
            _component_errors[name] = _component_errors.get(name, 0) + 1
            logger.warning("Agent 7: %s check FAILED (%dx): %s",
                           name, _component_errors[name], detail)

            if _component_errors[name] >= WATCHDOG_MAX_RETRIES and name not in _component_alerted:
                open_positions = await _get_open_position_symbols()
                ts = now.strftime("%H:%M IST")
                await send_message(fmt_critical(name, ts, open_positions))
                _component_alerted.add(name)
                if _component_errors[name] > WATCHDOG_MAX_RETRIES * 2:
                    await send_message(fmt_component_unrecoverable(name))


# ── Individual checks ─────────────────────────────────────────────────────────

async def _check_redis() -> tuple[bool, str]:
    import asyncio
    from app.redis_client import get_redis
    try:
        r = await get_redis()
        await asyncio.wait_for(r.ping(), timeout=WATCHDOG_REDIS_TIMEOUT_S)
        return True, ""
    except Exception as exc:
        return False, str(exc)


async def _check_postgres() -> tuple[bool, str]:
    import asyncio
    from app.database import get_pool
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await asyncio.wait_for(conn.fetchval("SELECT 1"), timeout=WATCHDOG_PG_TIMEOUT_S)
        return True, ""
    except Exception as exc:
        return False, str(exc)


async def _check_dhan() -> tuple[bool, str]:
    import asyncio
    from app.dhan_client import get_fund_limit
    try:
        balance = await asyncio.wait_for(get_fund_limit(), timeout=WATCHDOG_DHAN_TIMEOUT_S)
        if balance is None:
            return False, "get_fund_limit returned None"
        return True, ""
    except Exception as exc:
        return False, str(exc)


async def _check_capital_sync() -> tuple[bool, str]:
    """
    Verify capital was synced within the last 2 hours during trading hours.
    Only meaningful between 08:00 and 16:00 IST on weekdays.
    """
    from app.redis_client import get_redis
    now = datetime.now(IST)
    if not (8 <= now.hour < 16):
        return True, "outside sync window"
    try:
        r = await get_redis()
        last_sync_str = await r.get("system:capital_last_sync")
        if not last_sync_str:
            return False, "system:capital_last_sync not set"
        last_sync = datetime.fromisoformat(last_sync_str)
        age_minutes = (now - last_sync).total_seconds() / 60
        if age_minutes > 120:
            return False, f"capital not synced for {age_minutes:.0f} min"
        return True, ""
    except Exception as exc:
        return False, str(exc)


async def _check_tv_staleness() -> tuple[bool, str]:
    """
    During trading hours (09:15–15:00) warn if no TradingView webhook
    has arrived in the last WATCHDOG_TV_STALE_MINUTES minutes.
    """
    from app.redis_client import get_redis
    from app.redis_client import is_trading_active
    now = datetime.now(IST)
    if not (9 <= now.hour < 15):
        return True, "outside trading window"
    if not await is_trading_active():
        return True, "trading not active"
    try:
        r = await get_redis()
        last_str = await r.get(REDIS_KEY_LAST_TV_WEBHOOK)
        if not last_str:
            return True, "no webhooks expected yet"  # first day / not yet received any
        last = datetime.fromisoformat(last_str)
        age_min = (now - last).total_seconds() / 60
        if age_min > WATCHDOG_TV_STALE_MINUTES:
            await send_message(
                f"⚠️ No TradingView webhook in {age_min:.0f} min. Check Pine Script alerts."
            )
            return False, f"stale {age_min:.0f}min"
        return True, ""
    except Exception as exc:
        return False, str(exc)


async def _check_position_consistency() -> tuple[bool, str]:
    """
    Compare Dhan open positions with open_trades in DB.
    Alert if Dhan shows a position that isn't tracked in DB (orphan).
    """
    from app.dhan_client import get_open_intraday_positions
    from app.database import get_pool

    try:
        dhan_positions = await get_open_intraday_positions()
        dhan_symbols = {
            p.get("tradingSymbol", "").replace("-EQ", "").upper()
            for p in dhan_positions
        }

        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT symbol FROM open_trades "
                "WHERE date = CURRENT_DATE AND status NOT IN ('CLOSED', 'CANCELLED')"
            )
        db_symbols = {r["symbol"].upper() for r in rows}

        orphans = dhan_symbols - db_symbols
        if orphans:
            return False, f"Dhan has untracked positions: {', '.join(orphans)}"
        return True, ""
    except Exception as exc:
        return False, str(exc)


async def _check_memory() -> tuple[bool, str]:
    """Check process RSS memory usage."""
    try:
        import resource
        rss_bytes = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # On Linux, ru_maxrss is in kilobytes
        rss_mb = rss_bytes / 1024
        if rss_mb > WATCHDOG_MEMORY_LIMIT_MB:
            return False, f"RSS {rss_mb:.0f} MB > limit {WATCHDOG_MEMORY_LIMIT_MB} MB"
        return True, ""
    except Exception as exc:
        return False, str(exc)


async def _get_open_position_symbols() -> list[str]:
    try:
        from app.database import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT symbol FROM open_trades "
                "WHERE date = CURRENT_DATE AND status NOT IN ('CLOSED', 'CANCELLED')"
            )
        return [r["symbol"] for r in rows]
    except Exception:
        return []
