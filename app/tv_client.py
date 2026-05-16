"""
tv_client.py — TradingView historical data via tvdatafeed.

Replaces Angel One OHLCV fetches in the backtester.
No API key or auth required. NSE symbols fetched directly.

tvdatafeed is synchronous — all calls are wrapped in run_in_executor
so they don't block the asyncio event loop.
"""

import asyncio
import logging
from datetime import date, timedelta
from functools import partial
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

_tv = None


def _get_tv():
    global _tv
    if _tv is None:
        from tvdatafeed import TvDatafeed
        _tv = TvDatafeed()  # anonymous — no login needed for NSE data
    return _tv


async def _run(fn, *args, **kwargs):
    """Run a synchronous tvdatafeed call in a thread pool."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, partial(fn, *args, **kwargs))


async def get_daily_bars(symbol: str, n_bars: int = 60) -> Optional[list[dict]]:
    """
    Fetch n_bars of daily OHLCV for an NSE symbol.
    Returns list of {timestamp, open, high, low, close, volume} dicts, or None on failure.
    """
    try:
        from tvdatafeed import Interval
        tv = _get_tv()
        df: pd.DataFrame = await _run(
            tv.get_hist, symbol, "NSE",
            interval=Interval.in_daily,
            n_bars=n_bars,
        )
        if df is None or df.empty:
            return None
        df = df.reset_index()
        df = df.rename(columns={"datetime": "timestamp"})
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
        from tvdatafeed import Interval
        tv = _get_tv()
        df: pd.DataFrame = await _run(
            tv.get_hist, symbol, "NSE",
            interval=Interval.in_15_minute,
            n_bars=n_bars,
        )
        if df is None or df.empty:
            return None
        df = df.reset_index()
        df = df.rename(columns={"datetime": "timestamp"})
        return df[["timestamp", "open", "high", "low", "close", "volume"]].to_dict("records")
    except Exception as exc:
        logger.debug("TV 15min fetch failed for %s: %s", symbol, exc)
        return None
