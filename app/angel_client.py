"""
angel_client.py — Async Angel One SmartAPI HTTP client.

Handles TOTP-based auth, JWT token caching in Redis, and historical
candle data fetch. Called only by Agent 1 (backtester, Sunday) and
Agent 2 (pre-market data, weekday 08:30).

Never called during market hours — no latency concern.
"""

import asyncio
import json
import logging
from datetime import datetime, date, timedelta
from typing import Optional

import httpx
import pyotp
import pytz

from app.config import settings

logger = logging.getLogger(__name__)

BASE_URL = "https://apiconnect.angelbroking.com"
SCRIP_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

IST = pytz.timezone("Asia/Kolkata")

# Redis keys for token caching
_REDIS_KEY_JWT = "angel:jwt_token"
_REDIS_KEY_JWT_TTL = 82800  # 23 hours (tokens valid 24h)


def _base_headers() -> dict:
    """Common headers required by Angel One API for every request."""
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": "192.168.1.1",
        "X-ClientPublicIP": "1.1.1.1",
        "X-MACAddress": "00:00:00:00:00:00",
        "X-PrivateKey": settings.angel_one_api_key,
    }


async def _get_cached_jwt() -> Optional[str]:
    try:
        from app.redis_client import get_redis
        r = await get_redis()
        return await r.get(_REDIS_KEY_JWT)
    except Exception:
        return None


async def _cache_jwt(token: str) -> None:
    try:
        from app.redis_client import get_redis
        r = await get_redis()
        await r.set(_REDIS_KEY_JWT, token, ex=_REDIS_KEY_JWT_TTL)
    except Exception as exc:
        logger.warning("Angel One: Failed to cache JWT: %s", exc)


async def login() -> str:
    """
    Authenticate with Angel One using password + TOTP.
    Returns the JWT token string (without 'Bearer ' prefix).
    Caches in Redis for 23 hours.
    """
    cached = await _get_cached_jwt()
    if cached:
        return cached

    totp_code = pyotp.TOTP(settings.angel_one_totp_secret).now()
    payload = {
        "clientcode": settings.angel_one_client_id,
        "password": settings.angel_one_password,
        "totp": totp_code,
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            f"{BASE_URL}/rest/auth/angelbroking/user/v1/loginByPassword",
            headers=_base_headers(),
            json=payload,
        )
        resp.raise_for_status()
        data = resp.json()

    if not data.get("status"):
        raise RuntimeError(f"Angel One login failed: {data.get('message', 'unknown error')}")

    jwt_token = data["data"]["jwtToken"]
    # Strip "Bearer " prefix if present — we add it ourselves in auth headers
    if jwt_token.startswith("Bearer "):
        jwt_token = jwt_token[7:]

    await _cache_jwt(jwt_token)
    logger.info("Angel One: Login successful.")
    return jwt_token


async def _auth_headers() -> dict:
    token = await login()
    headers = _base_headers()
    headers["Authorization"] = f"Bearer {token}"
    return headers


async def get_candle_data(
    symbol_token: str,
    interval: str,
    from_date: str,
    to_date: str,
    exchange: str = "NSE",
) -> list[dict]:
    """
    Fetch OHLCV candles from Angel One historical API.

    Args:
        symbol_token: Angel One internal token (e.g. "3045" for SBIN)
        interval: "FIFTEEN_MINUTE" | "ONE_DAY" | etc.
        from_date: "YYYY-MM-DD HH:MM" (IST)
        to_date:   "YYYY-MM-DD HH:MM" (IST)
        exchange: "NSE" (default)

    Returns:
        List of dicts: [{timestamp, open, high, low, close, volume}, ...]
    """
    headers = await _auth_headers()
    payload = {
        "exchange": exchange,
        "symboltoken": symbol_token,
        "interval": interval,
        "fromdate": from_date,
        "todate": to_date,
    }

    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.post(
            f"{BASE_URL}/rest/secure/angelbroking/historical/v1/getCandleData",
            headers=headers,
            json=payload,
        )
        resp.raise_for_status()
        data = resp.json()

    if not data.get("status") or data.get("data") is None:
        raise RuntimeError(
            f"getCandleData failed for token={symbol_token}: {data.get('message', 'no data')}"
        )

    candles = []
    for row in data["data"]:
        # Angel One returns: [timestamp_str, open, high, low, close, volume]
        ts_str, o, h, l, c, v = row
        candles.append({
            "timestamp": datetime.fromisoformat(ts_str).astimezone(IST),
            "open": float(o),
            "high": float(h),
            "low": float(l),
            "close": float(c),
            "volume": int(v),
        })
    return candles


async def get_six_months_15min(symbol_token: str, exchange: str = "NSE") -> list[dict]:
    """
    Fetch 6 months of 15-min OHLCV. Angel One allows up to 60 days per call
    for 15-min interval, so we split into chunks.
    Returns list sorted by timestamp ascending.
    """
    today = date.today()
    end_date = today
    start_date = today - timedelta(days=180)  # ~6 months

    all_candles: list[dict] = []
    chunk_start = start_date
    chunk_days = 55  # stay under 60-day limit with margin

    while chunk_start < end_date:
        chunk_end = min(chunk_start + timedelta(days=chunk_days), end_date)
        from_str = f"{chunk_start.strftime('%Y-%m-%d')} 09:15"
        to_str = f"{chunk_end.strftime('%Y-%m-%d')} 15:30"

        try:
            candles = await get_candle_data(
                symbol_token=symbol_token,
                interval="FIFTEEN_MINUTE",
                from_date=from_str,
                to_date=to_str,
                exchange=exchange,
            )
            all_candles.extend(candles)
        except Exception as exc:
            logger.warning(
                "Angel One: Chunk %s–%s for token %s failed: %s",
                chunk_start, chunk_end, symbol_token, exc,
            )

        chunk_start = chunk_end + timedelta(days=1)
        await asyncio.sleep(0.3)  # respect rate limits

    # Deduplicate by timestamp and sort
    seen = set()
    deduped = []
    for c in all_candles:
        key = c["timestamp"].isoformat()
        if key not in seen:
            seen.add(key)
            deduped.append(c)
    deduped.sort(key=lambda x: x["timestamp"])
    return deduped


async def get_scrip_master() -> list[dict]:
    """
    Download the full Angel One scrip master JSON.
    Returns list of instrument dicts with fields:
      token, symbol, name, exch_seg, instrumenttype, lotsize, tick_size
    """
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.get(SCRIP_MASTER_URL)
        resp.raise_for_status()
        return resp.json()


async def invalidate_token() -> None:
    """Force re-login on next request (call after auth error)."""
    try:
        from app.redis_client import get_redis
        r = await get_redis()
        await r.delete(_REDIS_KEY_JWT)
    except Exception:
        pass
