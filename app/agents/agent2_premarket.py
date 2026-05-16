"""
Agent 2 — Pre-Market Analyst + Capital Reconciliation

08:00 IST: Pull Dhan balance → write Redis system:capital_inr
           Validate vs prior day; halt if >2% unexplained delta.
08:30 IST: Pull prior-session data for watchlist stocks via Angel One.
           Call Claude to score each stock for today's active setup.
           Write daily_candidates; set trading:active flag.
09:00 IST: Send Telegram brief.
"""

import json
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal

import pandas as pd
import pytz

from app.constants import (
    IST_TIMEZONE, AGENT2_MODEL, AGENT2_MAX_TOKENS,
    CAPITAL_MISMATCH_PCT, MAX_DAILY_CANDIDATES, MIN_DAILY_CANDIDATES,
    CANDIDATE_MIN_SCORE, VRVP_LOOKBACK_SESSIONS, VRVP_ROWS, VRVP_VALUE_AREA_PCT,
)
from app.config import settings
from app.database import get_pool
from app.redis_client import get_capital, set_capital, set_trading_active
from app.models.signals import DailyCandidate
from app.telegram import send_message, fmt_capital_mismatch, fmt_premarket_brief

logger = logging.getLogger(__name__)
IST = pytz.timezone(IST_TIMEZONE)


# ── Step 1: Capital reconciliation (08:00) ───────────────────────────────────

async def run_capital_reconcile() -> bool:
    logger.info("Agent 2: Capital reconciliation.")
    from app.dhan_client import get_fund_limit

    balance = await get_fund_limit()
    if balance is None:
        await send_message("🚨 Agent 2: Dhan /fundlimit failed. Pre-market halted.")
        await set_trading_active(False)
        return False

    prev = await _get_previous_day_capital()
    if prev and prev > 0:
        delta = abs(balance - prev) / prev
        if delta > float(CAPITAL_MISMATCH_PCT):
            await send_message(fmt_capital_mismatch(balance, prev, delta * 100))
            await set_trading_active(False)
            return False

    await set_capital(balance)
    # Record sync timestamp for watchdog
    from app.redis_client import get_redis
    r = await get_redis()
    await r.set("system:capital_last_sync", datetime.now(IST).isoformat())
    logger.info("Agent 2: Capital = ₹%.2f.", balance)
    return True


async def _get_previous_day_capital() -> float | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT closing_capital FROM daily_pnl ORDER BY date DESC LIMIT 1"
        )
    return float(row["closing_capital"]) if row and row["closing_capital"] else None


# ── Step 2: LLM pre-market analysis (08:30) ──────────────────────────────────

async def run_premarket_analyst() -> None:
    logger.info("Agent 2: Pre-market LLM analysis.")
    capital = await get_capital()
    if capital == 0.0:
        await send_message("⚠️ Agent 2: Capital zero — pre-market skipped.")
        await set_trading_active(False)
        return

    watchlist = await _fetch_watchlist()
    if not watchlist:
        await send_message("⚠️ Agent 2: Empty watchlist — run /backtest first.")
        await set_trading_active(False)
        return

    active_setup = await _fetch_active_setup()
    if not active_setup:
        await send_message("⚠️ Agent 2: No active_setup — run /backtest first.")
        await set_trading_active(False)
        return

    stock_data = await _fetch_prior_session_data(watchlist)
    market_ctx = await _fetch_market_context()

    system_prompt = _build_system_prompt(active_setup, capital, market_ctx)
    candidates = await _call_llm(system_prompt, json.dumps(stock_data, default=str))

    today = date.today()
    if len(candidates) < MIN_DAILY_CANDIDATES:
        await set_trading_active(False)
        await send_message(
            "⚠️ Pre-market scan: fewer than 3 candidates qualify. System on standby."
        )
        return

    candidates = candidates[:MAX_DAILY_CANDIDATES]
    await _write_candidates(today, candidates)
    await set_trading_active(True)

    msg = fmt_premarket_brief(
        date=today.isoformat(),
        capital=capital,
        setup_name=active_setup["setup_name"],
        candidates=[{"symbol": c.symbol, "bias": c.bias} for c in candidates],
    )
    await send_message(msg)
    logger.info("Agent 2: %d candidates written.", len(candidates))


# ── Prior-session data fetch ──────────────────────────────────────────────────

async def _fetch_prior_session_data(watchlist: list[dict]) -> list[dict]:
    """
    For each watchlist symbol, fetch the last 25 sessions of 15-min OHLCV
    from Angel One, compute VRVP + prior-session SVP, and package into the
    schema the LLM prompt expects.
    """
    import asyncio
    from app.angel_client import get_candle_data
    from app.symbol_master import angel_token
    from app.vrvp import compute_vrvp, compute_svp, split_into_sessions

    today = date.today()
    # Pull ~28 calendar days to guarantee 20+ trading sessions
    from_date = (today - timedelta(days=40)).strftime("%Y-%m-%d") + " 09:15"
    to_date = (today - timedelta(days=1)).strftime("%Y-%m-%d") + " 15:30"

    results: list[dict] = []
    semaphore = asyncio.Semaphore(8)

    async def process_one(entry: dict) -> dict | None:
        symbol = entry["symbol"]
        token = angel_token(symbol)
        if not token:
            return None
        async with semaphore:
            try:
                raw = await get_candle_data(token, "FIFTEEN_MINUTE", from_date, to_date)
                await asyncio.sleep(0.15)
            except Exception as exc:
                logger.warning("Agent 2: OHLCV fetch failed for %s: %s", symbol, exc)
                return None

        if len(raw) < 50:
            return None

        df = pd.DataFrame(raw)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(IST)
        df = df.sort_values("timestamp").reset_index(drop=True)

        sessions = split_into_sessions(df)
        if len(sessions) < 5:
            return None

        # VRVP on last 20 completed sessions
        lookback = sessions[-VRVP_LOOKBACK_SESSIONS:]
        combined = pd.concat(lookback, ignore_index=True)
        vrvp = compute_vrvp(combined, rows=VRVP_ROWS, value_area_pct=VRVP_VALUE_AREA_PCT)
        if vrvp is None:
            return None

        # Prior session SVP (last complete day)
        prior_session = sessions[-1]
        svp = compute_svp(prior_session, rows=100)
        if svp is None:
            return None

        # Prior session stats
        prior_close = float(prior_session["close"].iloc[-1])
        prior_high = float(prior_session["high"].max())
        prior_low = float(prior_session["low"].min())
        prior_volume = int(prior_session["volume"].sum())
        avg_volume_10s = sum(
            s["volume"].sum() for s in sessions[-11:-1]
        ) / 10 if len(sessions) >= 11 else prior_volume
        prior_volume_ratio = prior_volume / avg_volume_10s if avg_volume_10s > 0 else 1.0

        # Pre-open price proxy — use prior close (actual pre-open from NSE is separate)
        pre_open_price = prior_close

        return {
            "symbol": symbol,
            "pre_open_price": round(pre_open_price, 2),
            "prior_close": round(prior_close, 2),
            "prior_high": round(prior_high, 2),
            "prior_low": round(prior_low, 2),
            "prior_volume_ratio": round(prior_volume_ratio, 2),
            "prior_day_move_pct": round(
                abs(prior_close - float(prior_session["open"].iloc[0])) /
                float(prior_session["open"].iloc[0]) * 100, 2
            ),
            # VRVP levels
            "vrvp_poc": round(vrvp.poc, 2),
            "vrvp_vah": round(vrvp.vah, 2),
            "vrvp_val": round(vrvp.val, 2),
            "vrvp_hvns": [round(h, 2) for h in vrvp.hvns[:5]],
            "vrvp_lvns": [round(l, 2) for l in vrvp.lvns[:5]],
            # Prior session SVP levels
            "prior_svp_poc": round(svp.poc, 2),
            "prior_svp_vah": round(svp.vah, 2),
            "prior_svp_val": round(svp.val, 2),
            # Disqualification flags (earnings/corporate actions require external data)
            "earnings_today": False,        # TODO: wire NSE earnings calendar
            "corporate_action_today": False, # TODO: wire NSE corporate actions
        }

    tasks = [process_one(e) for e in watchlist]
    raw_results = await asyncio.gather(*tasks)
    results = [r for r in raw_results if r is not None]
    logger.info("Agent 2: %d/%d symbols have usable pre-market data.", len(results), len(watchlist))
    return results


async def _fetch_market_context() -> dict:
    """
    Pull Nifty/BankNifty pre-open levels + PCR + max pain from NSE.
    Uses the NSE API (no auth required for public endpoints).
    Falls back to safe defaults on failure.
    """
    import httpx

    defaults = {"nifty_pre_open": "N/A", "banknifty_pre_open": "N/A",
                "pcr": "N/A", "max_pain": "N/A"}
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Referer": "https://www.nseindia.com",
    }
    try:
        async with httpx.AsyncClient(timeout=10.0, headers=headers) as client:
            # NSE pre-open market data
            resp = await client.get(
                "https://www.nseindia.com/api/market-data-pre-open?key=NIFTY"
            )
            if resp.status_code == 200:
                data = resp.json()
                nifty_pre = data.get("data", [{}])[0].get("metadata", {}).get("lastPrice", "N/A")
                defaults["nifty_pre_open"] = nifty_pre

            # NSE option chain for PCR and max pain
            resp2 = await client.get(
                "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
            )
            if resp2.status_code == 200:
                oc = resp2.json()
                filtered = oc.get("filtered", {}).get("data", [])
                total_ce_oi = sum(r.get("CE", {}).get("openInterest", 0) for r in filtered)
                total_pe_oi = sum(r.get("PE", {}).get("openInterest", 0) for r in filtered)
                defaults["pcr"] = round(total_pe_oi / total_ce_oi, 2) if total_ce_oi > 0 else "N/A"
    except Exception as exc:
        logger.warning("Agent 2: Market context fetch failed (non-fatal): %s", exc)

    return defaults


# ── LLM call ─────────────────────────────────────────────────────────────────

def _build_system_prompt(active_setup: dict, capital: float, market_ctx: dict) -> str:
    return (
        f"You are a pre-market analyst for an NSE intraday VRVP+SVP system.\n"
        f"Active setup: {active_setup['setup_name']} (Setup {active_setup['setup_id']})\n"
        f"Capital: ₹{capital:,.2f} | Date: {date.today().isoformat()}\n"
        f"Nifty pre-open: {market_ctx['nifty_pre_open']} | "
        f"BankNifty: {market_ctx['banknifty_pre_open']}\n"
        f"PCR: {market_ctx['pcr']} | Max pain: {market_ctx['max_pain']}\n\n"
        f"For each stock in the input array:\n"
        f"1. Classify day type (Normal/Trend/Gap/Neutral) from pre_open_price vs "
        f"prior_svp_vah/val.\n"
        f"2. Assess whether {active_setup['setup_name']} has a structurally favorable "
        f"configuration given VRVP levels.\n"
        f"3. Score 1-10. Include only stocks scoring >={CANDIDATE_MIN_SCORE}.\n\n"
        f"Unconditionally disqualify if: earnings_today=true, corporate_action_today=true, "
        f"abs(pre_open_price-vrvp_poc)/vrvp_poc<0.003, "
        f"prior_day_move_pct>4 AND prior_volume_ratio>1.0\n\n"
        f"Return a JSON array only — no text outside JSON. Max {MAX_DAILY_CANDIDATES} elements. "
        f"Return [] if fewer than {MIN_DAILY_CANDIDATES} qualify.\n"
        f"Each element: {{\"symbol\":\"\",\"day_type\":\"\",\"bias\":\"\","
        f"\"setup_probability_score\":0,\"key_level\":0.0,"
        f"\"invalidation_level\":0.0,\"reasoning\":\"\"}}\n"
        f"reasoning: max 15 words. Max tokens: {AGENT2_MAX_TOKENS}."
    )


async def _call_llm(system_prompt: str, user_content: str) -> list[DailyCandidate]:
    import anthropic
    client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
    try:
        response = await client.messages.create(
            model=AGENT2_MODEL,
            max_tokens=AGENT2_MAX_TOKENS,
            system=system_prompt,
            messages=[{"role": "user", "content": user_content}],
        )
        raw = response.content[0].text.strip()
        items = json.loads(raw)
        candidates = []
        for item in items:
            try:
                c = DailyCandidate(
                    symbol=item["symbol"],
                    day_type=item["day_type"],
                    bias=item["bias"],
                    setup_probability_score=item["setup_probability_score"],
                    key_level=item.get("key_level"),
                    invalidation_level=item.get("invalidation_level"),
                    reasoning=item.get("reasoning"),
                )
                if c.setup_probability_score >= CANDIDATE_MIN_SCORE:
                    candidates.append(c)
            except Exception as e:
                logger.warning("Agent 2: Bad candidate item: %s", e)
        return candidates
    except Exception as exc:
        logger.error("Agent 2: LLM call failed: %s", exc)
        return []


# ── DB helpers ────────────────────────────────────────────────────────────────

async def _write_candidates(today: date, candidates: list[DailyCandidate]) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DELETE FROM daily_candidates WHERE date = $1", today)
            for c in candidates:
                await conn.execute(
                    "INSERT INTO daily_candidates "
                    "(date,symbol,day_type,bias,score,key_level,invalidation_level,reasoning) "
                    "VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                    today, c.symbol, c.day_type, c.bias,
                    c.setup_probability_score, c.key_level,
                    c.invalidation_level, c.reasoning,
                )


async def _fetch_watchlist() -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT ON (symbol) symbol, active_setup_id "
            "FROM watchlist ORDER BY symbol, updated_at DESC"
        )
    return [dict(r) for r in rows]


async def _fetch_active_setup() -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT setup_id, setup_name FROM active_setup WHERE is_current=TRUE LIMIT 1"
        )
    return dict(row) if row else None
