"""
Agent 6 — Auditor

run_eod_report()      — called by scheduler at 15:30 IST Mon-Fri.
run_weekly_csv_export() — called by scheduler at 05:55 IST Sunday.
"""

import csv
import io
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytz

from app.constants import IST_TIMEZONE, SETUP_NAMES
from app.database import get_pool
from app.redis_client import get_capital, set_trading_active
from app.telegram import send_message, fmt_eod_report

logger = logging.getLogger(__name__)
IST = pytz.timezone(IST_TIMEZONE)


# ── EOD report (15:30) ────────────────────────────────────────────────────────

async def run_eod_report() -> None:
    logger.info("Agent 6: Generating EOD report.")
    today = date.today()
    pool = await get_pool()

    async with pool.acquire() as conn:
        # Today's trades
        trades = await conn.fetch(
            "SELECT symbol, direction, setup_id, entry_price, exit_price, "
            "net_pnl, exit_reason "
            "FROM trade_history WHERE date = $1",
            today,
        )
        # Active setup
        setup_row = await conn.fetchrow(
            "SELECT setup_id, setup_name FROM active_setup WHERE is_current = TRUE LIMIT 1"
        )
        # Weekly P&L (Mon-today)
        week_start = today - timedelta(days=today.weekday())
        weekly = await conn.fetchrow(
            "SELECT COALESCE(SUM(net_pnl), 0) AS weekly_pnl "
            "FROM trade_history WHERE date >= $1 AND date <= $2",
            week_start, today,
        )
        # Win rate over last 20 trades for active setup
        wr_row = await conn.fetchrow(
            "SELECT COUNT(*) FILTER (WHERE net_pnl > 0) AS wins, COUNT(*) AS total "
            "FROM trade_history WHERE setup_id = $1 ORDER BY date DESC LIMIT 20",
            setup_row["setup_id"] if setup_row else None,
        )

    num_trades = len(trades)
    wins = sum(1 for t in trades if float(t["net_pnl"]) > 0)
    losses = num_trades - wins
    session_pnl = sum(float(t["net_pnl"]) for t in trades)
    capital = await get_capital()
    session_pnl_pct = session_pnl / capital * 100 if capital > 0 else 0.0
    weekly_pnl = float(weekly["weekly_pnl"]) if weekly else 0.0
    weekly_pnl_pct = weekly_pnl / capital * 100 if capital > 0 else 0.0

    # Best / worst trade by R
    def r_mult(t):
        entry = float(t["entry_price"])
        exit_ = float(t["exit_price"])
        pnl = float(t["net_pnl"])
        risk = abs(entry * 0.005)   # fallback: 0.5% of entry as risk proxy
        return pnl / (risk * int(1)) if risk > 0 else 0.0

    if trades:
        sorted_trades = sorted(trades, key=r_mult)
        worst = sorted_trades[0]
        best = sorted_trades[-1]
        best_symbol, best_r = best["symbol"], round(r_mult(best), 1)
        worst_symbol, worst_r = worst["symbol"], round(r_mult(worst), 1)
    else:
        best_symbol, best_r = "—", 0.0
        worst_symbol, worst_r = "—", 0.0

    opening_capital = capital - session_pnl
    daily_loss_limit = opening_capital * 0.02
    daily_loss_used = max(-session_pnl, 0)
    daily_loss_used_pct = (daily_loss_used / daily_loss_limit * 100) if daily_loss_limit > 0 else 0.0

    setup_name = setup_row["setup_name"] if setup_row else "Unknown"
    setup_wr_20 = (float(wr_row["wins"]) / float(wr_row["total"]) * 100
                   if wr_row and wr_row["total"] > 0 else 0.0)

    msg = fmt_eod_report(
        date=today.isoformat(),
        capital=capital,
        trades=num_trades,
        wins=wins,
        losses=losses,
        session_pnl=session_pnl,
        session_pnl_pct=session_pnl_pct,
        best_symbol=best_symbol,
        best_r=best_r,
        worst_symbol=worst_symbol,
        worst_r=worst_r,
        daily_loss_used_pct=daily_loss_used_pct,
        weekly_pnl=weekly_pnl,
        weekly_pnl_pct=weekly_pnl_pct,
        setup_name=setup_name,
        setup_win_rate_20=setup_wr_20,
    )
    await send_message(msg)

    # Persist EOD capital and P&L to daily_pnl table
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO daily_pnl (date, opening_capital, closing_capital, gross_pnl, net_pnl,
               num_trades, num_wins)
               VALUES ($1, $2, $3, $4, $4, $5, $6)
               ON CONFLICT (date) DO UPDATE SET
                 closing_capital = EXCLUDED.closing_capital,
                 gross_pnl       = EXCLUDED.gross_pnl,
                 net_pnl         = EXCLUDED.net_pnl,
                 num_trades      = EXCLUDED.num_trades,
                 num_wins        = EXCLUDED.num_wins""",
            today,
            round(opening_capital, 2),
            round(capital, 2),
            round(session_pnl, 2),
            num_trades,
            wins,
        )

    # Deactivate trading flag until next pre-market
    await set_trading_active(False)
    logger.info("Agent 6: EOD report sent. trading:active → false.")


# ── Weekly CSV export (Sunday 05:55) ─────────────────────────────────────────

async def run_weekly_csv_export() -> None:
    """
    Export last week's trade history to CSV and send via Telegram document.
    """
    import httpx
    from app.config import settings

    logger.info("Agent 6: Weekly CSV export.")
    today = date.today()
    week_start = today - timedelta(days=7)

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT date, symbol, direction, setup_id, entry_price, exit_price, "
            "quantity, gross_pnl, net_pnl, exit_reason, opened_at, closed_at "
            "FROM trade_history WHERE date >= $1 AND date < $2 ORDER BY closed_at",
            week_start, today,
        )

    if not rows:
        await send_message(f"📁 Weekly export ({week_start} → {today}): no trades.")
        return

    # Build CSV in memory
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=[
        "date", "symbol", "direction", "setup_id", "entry_price", "exit_price",
        "quantity", "gross_pnl", "net_pnl", "exit_reason", "opened_at", "closed_at",
    ])
    writer.writeheader()
    for r in rows:
        writer.writerow({
            "date": r["date"].isoformat(),
            "symbol": r["symbol"],
            "direction": r["direction"],
            "setup_id": r["setup_id"],
            "entry_price": r["entry_price"],
            "exit_price": r["exit_price"],
            "quantity": r["quantity"],
            "gross_pnl": r["gross_pnl"],
            "net_pnl": r["net_pnl"],
            "exit_reason": r["exit_reason"],
            "opened_at": r["opened_at"].isoformat() if r["opened_at"] else "",
            "closed_at": r["closed_at"].isoformat() if r["closed_at"] else "",
        })

    csv_bytes = buf.getvalue().encode()
    filename = f"trades_{week_start}_{today}.csv"

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendDocument"
            resp = await client.post(url, data={
                "chat_id": settings.telegram_chat_id,
                "caption": f"📊 Weekly trade export: {week_start} → {today} ({len(rows)} trades)",
            }, files={"document": (filename, csv_bytes, "text/csv")})
            if resp.status_code == 200:
                logger.info("Agent 6: CSV exported — %d rows.", len(rows))
            else:
                logger.error("Agent 6: Telegram document upload failed: %s", resp.text)
    except Exception as exc:
        logger.error("Agent 6: Weekly CSV export failed: %s", exc)
        await send_message(f"⚠️ Weekly CSV export failed: {exc}")
