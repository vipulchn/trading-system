"""
tv_client.py — TradingView historical data via tvdatafeed.

Replaces Angel One OHLCV fetches in the backtester.
No API key or auth required. NSE symbols fetched directly.

tvDatafeed is synchronous and uses a single WebSocket — all calls
are serialized through a threading.Lock to avoid race conditions.
"""

import asyncio
import logging
import threading
from functools import partial
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

_tv = None
_tv_lock = threading.Lock()


def _get_tv():
    global _tv
    if _tv is None:
        from tvDatafeed import TvDatafeed
        _tv = TvDatafeed()
    return _tv


def _sanitize(symbol: str) -> str:
    """TradingView rejects hyphens in NSE symbols — strip them."""
    return symbol.replace("-", "")


def _fetch_daily_sync(symbol: str, n_bars: int) -> Optional[pd.DataFrame]:
    from tvDatafeed import Interval
    with _tv_lock:
        tv = _get_tv()
        return tv.get_hist(_sanitize(symbol), "NSE", interval=Interval.in_daily, n_bars=n_bars)


def _fetch_15min_sync(symbol: str, n_bars: int) -> Optional[pd.DataFrame]:
    from tvDatafeed import Interval
    with _tv_lock:
        tv = _get_tv()
        return tv.get_hist(_sanitize(symbol), "NSE", interval=Interval.in_15_minute, n_bars=n_bars)


async def get_daily_bars(symbol: str, n_bars: int = 60) -> Optional[list[dict]]:
    """
    Fetch n_bars of daily OHLCV for an NSE symbol.
    Returns list of {timestamp, open, high, low, close, volume} dicts, or None on failure.
    """
    try:
        loop = asyncio.get_event_loop()
        df = await loop.run_in_executor(None, partial(_fetch_daily_sync, symbol, n_bars))
        if df is None or df.empty:
            return None
        df = df.reset_index().rename(columns={"datetime": "timestamp"})
        return df[["timestamp", "open", "high", "low", "close", "volume"]].to_dict("records")
    except Exception as exc:
        logger.debug("TV daily fetch failed for %s: %s", symbol, exc)
        return None


async def get_15min_bars(symbol: str, n_bars: int = 5000) -> Optional[list[dict]]:
    """
    Fetch n_bars of 15-minute OHLCV for an NSE symbol.
    5000 bars ≈ 200 trading days (6+ months at 25 bars/day).
    Returns list of {timestamp, open, high, low, close, volume} dicts, or None on failure.
    """
    try:
        loop = asyncio.get_event_loop()
        df = await loop.run_in_executor(None, partial(_fetch_15min_sync, symbol, n_bars))
        if df is None or df.empty:
            return None
        df = df.reset_index().rename(columns={"datetime": "timestamp"})
        return df[["timestamp", "open", "high", "low", "close", "volume"]].to_dict("records")
    except Exception as exc:
        logger.debug("TV 15min fetch failed for %s: %s", symbol, exc)
        return None
