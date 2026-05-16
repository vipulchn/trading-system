"""
dhan_client.py — Async Dhan API v2 HTTP client.

All order placement, status polling, and position queries go through here.
Called by Agent 5 (Order Manager) and Agent 2 (capital reconciliation).

Dhan API v2 base: https://api.dhan.co/v2
Auth: access-token + client-id headers on every request.
"""

import logging
from typing import Literal, Optional

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

BASE_URL = "https://api.dhan.co/v2"

# Dhan exchange segment constants
EXCHANGE_NSE_EQ = "NSE_EQ"
EXCHANGE_BSE_EQ = "BSE_EQ"

# Product type for intraday
PRODUCT_INTRADAY = "INTRADAY"

# Order status constants
STATUS_TRANSIT = "TRANSIT"
STATUS_PENDING = "PENDING"
STATUS_PART_TRADED = "PART_TRADED"
STATUS_TRADED = "TRADED"
STATUS_CANCELLED = "CANCELLED"
STATUS_REJECTED = "REJECTED"

TERMINAL_STATUSES = {STATUS_TRADED, STATUS_CANCELLED, STATUS_REJECTED}


def _headers() -> dict:
    return {
        "access-token": settings.dhan_access_token,
        "client-id": settings.dhan_client_id,
        "Content-Type": "application/json",
    }


# ---------------------------------------------------------------------------
# Order placement
# ---------------------------------------------------------------------------

async def place_limit_order(
    security_id: str,
    trading_symbol: str,
    transaction_type: Literal["BUY", "SELL"],
    quantity: int,
    price: float,
    exchange_segment: str = EXCHANGE_NSE_EQ,
) -> Optional[str]:
    """
    Place a DAY LIMIT order.
    Returns the Dhan orderId string, or None on failure.
    """
    payload = {
        "dhanClientId": settings.dhan_client_id,
        "transactionType": transaction_type,
        "exchangeSegment": exchange_segment,
        "productType": PRODUCT_INTRADAY,
        "orderType": "LIMIT",
        "validity": "DAY",
        "securityId": security_id,
        "tradingSymbol": trading_symbol,
        "quantity": quantity,
        "disclosedQuantity": 0,
        "price": round(price, 2),
        "triggerPrice": 0,
        "afterMarketOrder": False,
    }
    return await _post_order(payload)


async def place_sl_order(
    security_id: str,
    trading_symbol: str,
    transaction_type: Literal["BUY", "SELL"],
    quantity: int,
    trigger_price: float,
    exchange_segment: str = EXCHANGE_NSE_EQ,
) -> Optional[str]:
    """
    Place a STOP_LOSS order.
    Limit price = trigger_price * 0.995 (0.5% slippage buffer on sell-side SL).
    Returns the Dhan orderId string, or None on failure.
    """
    # For long SL (sell): limit slightly below trigger to ensure fill
    # For short SL (buy): limit slightly above trigger
    if transaction_type == "SELL":
        limit_price = round(trigger_price * 0.995, 2)
    else:
        limit_price = round(trigger_price * 1.005, 2)

    payload = {
        "dhanClientId": settings.dhan_client_id,
        "transactionType": transaction_type,
        "exchangeSegment": exchange_segment,
        "productType": PRODUCT_INTRADAY,
        "orderType": "STOP_LOSS",
        "validity": "DAY",
        "securityId": security_id,
        "tradingSymbol": trading_symbol,
        "quantity": quantity,
        "disclosedQuantity": 0,
        "price": limit_price,
        "triggerPrice": round(trigger_price, 2),
        "afterMarketOrder": False,
    }
    return await _post_order(payload)


async def place_market_order(
    security_id: str,
    trading_symbol: str,
    transaction_type: Literal["BUY", "SELL"],
    quantity: int,
    exchange_segment: str = EXCHANGE_NSE_EQ,
) -> Optional[str]:
    """
    Place a MARKET order (used for force exit at 15:15).
    Returns the Dhan orderId string, or None on failure.
    """
    payload = {
        "dhanClientId": settings.dhan_client_id,
        "transactionType": transaction_type,
        "exchangeSegment": exchange_segment,
        "productType": PRODUCT_INTRADAY,
        "orderType": "MARKET",
        "validity": "DAY",
        "securityId": security_id,
        "tradingSymbol": trading_symbol,
        "quantity": quantity,
        "disclosedQuantity": 0,
        "price": 0,
        "triggerPrice": 0,
        "afterMarketOrder": False,
    }
    return await _post_order(payload)


async def _post_order(payload: dict) -> Optional[str]:
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{BASE_URL}/orders",
                headers=_headers(),
                json=payload,
            )
            resp.raise_for_status()
            data = resp.json()
            order_id = data.get("orderId")
            if not order_id:
                logger.error("Dhan place_order: no orderId in response: %s", data)
                return None
            logger.info(
                "Dhan: Order placed — id=%s symbol=%s type=%s qty=%d",
                order_id,
                payload.get("tradingSymbol"),
                payload.get("transactionType"),
                payload.get("quantity"),
            )
            return str(order_id)
    except httpx.HTTPStatusError as exc:
        logger.error(
            "Dhan place_order HTTP %s: %s", exc.response.status_code, exc.response.text
        )
        return None
    except Exception as exc:
        logger.error("Dhan place_order failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Order management
# ---------------------------------------------------------------------------

async def cancel_order(order_id: str) -> bool:
    """
    Cancel an open order. Returns True on success.
    Dhan returns 200 with confirmation; returns 400 if already terminal.
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.delete(
                f"{BASE_URL}/orders/{order_id}",
                headers=_headers(),
            )
            if resp.status_code in (200, 202):
                logger.info("Dhan: Order %s cancelled.", order_id)
                return True
            logger.warning(
                "Dhan cancel_order %s → HTTP %s: %s",
                order_id, resp.status_code, resp.text,
            )
            return False
    except Exception as exc:
        logger.error("Dhan cancel_order %s failed: %s", order_id, exc)
        return False


async def get_order(order_id: str) -> Optional[dict]:
    """
    Fetch full order detail by ID.
    Returns the order dict or None on failure.

    Key fields:
      orderStatus: TRANSIT | PENDING | PART_TRADED | TRADED | CANCELLED | REJECTED
      averageTradedPrice: float
      filledQty: int
      remainingQuantity: int
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{BASE_URL}/orders/{order_id}",
                headers=_headers(),
            )
            resp.raise_for_status()
            return resp.json()
    except Exception as exc:
        logger.error("Dhan get_order %s failed: %s", order_id, exc)
        return None


async def get_order_status(order_id: str) -> Optional[str]:
    """Return the orderStatus string, or None on failure."""
    order = await get_order(order_id)
    if order is None:
        return None
    return order.get("orderStatus")


async def get_fill_price(order_id: str) -> Optional[float]:
    """Return the averageTradedPrice for a TRADED order, or None."""
    order = await get_order(order_id)
    if order is None:
        return None
    price = order.get("averageTradedPrice")
    return float(price) if price else None


# ---------------------------------------------------------------------------
# Positions and capital
# ---------------------------------------------------------------------------

async def get_positions() -> list[dict]:
    """
    Fetch all intraday positions from Dhan /positions.

    Returns list of position dicts. Key fields:
      tradingSymbol, securityId, exchangeSegment, productType,
      netQty, buyAvg, sellAvg, unrealizedProfit, realizedProfit,
      dayBuyQty, daySellQty
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{BASE_URL}/positions",
                headers=_headers(),
            )
            resp.raise_for_status()
            data = resp.json()
            # Dhan returns a list directly or wrapped in a key
            if isinstance(data, list):
                return data
            return data.get("data", [])
    except Exception as exc:
        logger.error("Dhan get_positions failed: %s", exc)
        return []


async def get_open_intraday_positions() -> list[dict]:
    """
    Return only INTRADAY positions with non-zero netQty.
    Enriched with derived fields: direction, quantity.
    """
    all_positions = await get_positions()
    open_pos = []
    for p in all_positions:
        if p.get("productType") != PRODUCT_INTRADAY:
            continue
        net_qty = int(p.get("netQty", 0))
        if net_qty == 0:
            continue
        direction = "Long" if net_qty > 0 else "Short"
        open_pos.append({
            **p,
            "direction": direction,
            "quantity": abs(net_qty),
            "unrealized_pnl": float(p.get("unrealizedProfit", 0)),
        })
    return open_pos


async def get_fund_limit() -> Optional[float]:
    """
    Fetch available cash balance from Dhan /fundlimit.
    Returns float or None on failure.
    Note: Dhan misspells the field as 'availabelBalance' — keep as-is.
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{BASE_URL}/fundlimit",
                headers=_headers(),
            )
            resp.raise_for_status()
            data = resp.json()
            return float(data.get("availabelBalance", 0))
    except Exception as exc:
        logger.error("Dhan get_fund_limit failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Cancel all orders for a symbol (force exit helper)
# ---------------------------------------------------------------------------

async def cancel_open_orders_for_symbol(symbol: str) -> int:
    """
    Cancel all non-terminal orders for a given tradingSymbol.
    Returns the count of successfully cancelled orders.
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(f"{BASE_URL}/orders", headers=_headers())
            resp.raise_for_status()
            data = resp.json()
            all_orders = data if isinstance(data, list) else data.get("data", [])
    except Exception as exc:
        logger.error("Dhan: Failed to list orders for symbol cancel: %s", exc)
        return 0

    cancelled = 0
    for order in all_orders:
        order_symbol = order.get("tradingSymbol", "").replace("-EQ", "").upper()
        if order_symbol != symbol.upper():
            continue
        if order.get("orderStatus") in TERMINAL_STATUSES:
            continue
        order_id = order.get("orderId")
        if order_id and await cancel_order(str(order_id)):
            cancelled += 1

    return cancelled
