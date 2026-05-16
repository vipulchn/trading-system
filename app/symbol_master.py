"""
symbol_master.py — Symbol master download and caching.

Maps NSE trading symbol → Angel One token (for historical data)
                        → Dhan security ID (for order placement)
                        → Dhan trading symbol e.g. "RELIANCE-EQ"

Angel One scrip master: JSON file downloaded once per week (Agent 1 startup).
Dhan scrip master: CSV file downloaded from Dhan CDN.

Both are cached in PostgreSQL table `symbol_master` to avoid
re-downloading every run.
"""

import asyncio
import csv
import io
import json
import logging
from datetime import date, timedelta
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

DHAN_SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# In-memory caches (populated on first use within a process)
_angel_token_map: dict[str, str] = {}   # symbol → angel_token
_dhan_id_map: dict[str, str] = {}       # symbol → dhan_security_id
_dhan_symbol_map: dict[str, str] = {}   # symbol → dhan_trading_symbol (e.g. "RELIANCE-EQ")
_fo_eligible: set[str] = set()          # symbols in F&O (from NFO segment)


async def load_symbol_masters() -> None:
    """
    Download and parse both scrip masters. Call once at Agent 1 startup.
    Populates all in-memory maps.
    """
    await asyncio.gather(
        _load_angel_one_master(),
        _load_dhan_master(),
    )
    logger.info(
        "Symbol master loaded: %d Angel tokens, %d Dhan IDs, %d F&O eligible.",
        len(_angel_token_map),
        len(_dhan_id_map),
        len(_fo_eligible),
    )


async def _load_angel_one_master() -> None:
    """Download Angel One OpenAPI scrip master and build symbol→token map."""
    from app.angel_client import get_scrip_master

    try:
        instruments = await get_scrip_master()
    except Exception as exc:
        logger.error("Failed to download Angel One scrip master: %s", exc)
        return

    nse_equity: set[str] = set()
    nfo_underlying: set[str] = set()

    for row in instruments:
        seg = row.get("exch_seg", "")
        instrument_type = row.get("instrumenttype", "")
        symbol = row.get("symbol", "").upper()
        token = row.get("token", "")
        name = row.get("name", "").upper()

        if seg == "NSE" and instrument_type == "EQ":
            # NSE equity — map by symbol
            _angel_token_map[symbol] = token
            nse_equity.add(symbol)

        elif seg == "NFO" and instrument_type in ("FUTSTK", "OPTSTK"):
            # F&O eligible — extract underlying from name (e.g. "RELIANCE" from "RELIANCE25JANFUT")
            # The 'name' field in NFO rows contains the underlying symbol
            underlying = name.strip()
            if underlying:
                nfo_underlying.add(underlying)

    # F&O eligible = stocks that appear in NSE equity AND have NFO contracts
    _fo_eligible.update(nse_equity & nfo_underlying)
    logger.info(
        "Angel One master: %d NSE equity tokens, %d F&O eligible.",
        len(nse_equity), len(_fo_eligible),
    )


async def _load_dhan_master() -> None:
    """Download Dhan scrip master CSV and build symbol→security_id map."""
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(DHAN_SCRIP_MASTER_URL)
            resp.raise_for_status()
            csv_text = resp.text
    except Exception as exc:
        logger.error("Failed to download Dhan scrip master: %s", exc)
        return

    reader = csv.DictReader(io.StringIO(csv_text))
    count = 0
    for row in reader:
        # Filter NSE equity segment
        exch = row.get("SEM_EXM_EXCH_ID", "").strip()
        segment = row.get("SEM_SEGMENT", "").strip()
        instrument = row.get("SEM_INSTRUMENT_NAME", "").strip()
        trading_symbol = row.get("SEM_TRADING_SYMBOL", "").strip()
        security_id = row.get("SEM_SMST_SECURITY_ID", "").strip()

        if exch != "NSE" or segment != "E":
            continue
        if not trading_symbol.endswith("-EQ"):
            continue

        # Extract base symbol: "RELIANCE-EQ" → "RELIANCE"
        base_symbol = trading_symbol.replace("-EQ", "").upper()
        _dhan_id_map[base_symbol] = security_id
        _dhan_symbol_map[base_symbol] = trading_symbol
        count += 1

    logger.info("Dhan master: %d NSE equity securities.", count)


# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------

def angel_token(symbol: str) -> Optional[str]:
    """Return Angel One symboltoken for a given NSE symbol."""
    return _angel_token_map.get(symbol.upper())


def dhan_security_id(symbol: str) -> Optional[str]:
    """Return Dhan securityId for a given NSE symbol."""
    return _dhan_id_map.get(symbol.upper())


def dhan_trading_symbol(symbol: str) -> Optional[str]:
    """Return Dhan tradingSymbol (e.g. 'RELIANCE-EQ') for a given NSE symbol."""
    return _dhan_symbol_map.get(symbol.upper())


def is_fo_eligible(symbol: str) -> bool:
    """Return True if the symbol has F&O contracts on NSE."""
    return symbol.upper() in _fo_eligible


def get_fo_eligible_symbols() -> list[str]:
    """Return list of all F&O eligible NSE equity symbols."""
    return sorted(_fo_eligible)


def is_loaded() -> bool:
    return len(_angel_token_map) > 0 and len(_dhan_id_map) > 0
