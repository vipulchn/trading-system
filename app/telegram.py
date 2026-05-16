import logging
import httpx
from app.config import settings

logger = logging.getLogger(__name__)
_BASE = "https://api.telegram.org/bot{token}/sendMessage"


async def send_message(text: str, parse_mode: str = "HTML") -> bool:
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        logger.warning("Telegram not configured.")
        return False
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(
                _BASE.format(token=settings.telegram_bot_token),
                json={"chat_id": settings.telegram_chat_id, "text": text,
                      "parse_mode": parse_mode, "disable_web_page_preview": True},
            )
            return r.status_code == 200
    except Exception as exc:
        logger.error("Telegram send failed: %s", exc)
        return False


async def send_alert(text: str) -> bool:
    return await send_message(text)


# ── Message formatters ────────────────────────────────────────────────────────

def fmt_backtest_complete(date, setup_name, setup_id, top5, new_entries, dropped, weakest):
    return (f"📈 <b>BACKTEST COMPLETE — {date}</b>\n"
            f"Active setup: <b>{setup_name}</b>\nTop 5: {', '.join(top5)}\n"
            f"New: {', '.join(new_entries) or 'none'} | Dropped: {', '.join(dropped) or 'none'}\n"
            f"⚠️ Update ACTIVE_SETUP in TradingView Pine Script to Setup {setup_id} before Monday 09:00.")

def fmt_capital_mismatch(dhan_amount, expected, delta_pct):
    return (f"⚠️ <b>CAPITAL MISMATCH</b>\nDhan: ₹{dhan_amount:,.2f} | Expected: ₹{expected:,.2f} | "
            f"Delta: {delta_pct:.2f}%\nAgent 2 halted. Send /resume after confirming.")

def fmt_premarket_brief(date, capital, setup_name, candidates):
    parts = [f"{c['symbol']} {c['bias']}" for c in candidates]
    return (f"🔍 <b>PRE-MARKET BRIEF — {date}</b>\nCapital: ₹{capital:,.2f}\n"
            f"Active setup: {setup_name}\nCandidates ({len(candidates)}): {', '.join(parts)}\n"
            f"⚠️ Ensure TradingView alerts active before 09:15.")

def fmt_signal_rejected(symbol, direction, reason):
    return f"❌ Signal <b>{symbol} {direction}</b> REJECTED: {reason}"

def fmt_entry(symbol, direction, entry, qty, stop, t1, t2, risk_inr, risk_pct, setup_name):
    return (f"✅ <b>ENTRY: {symbol} {direction}</b>\nPrice: ₹{entry:.2f} | Qty: {qty}\n"
            f"Stop: ₹{stop:.2f} | T1: ₹{t1:.2f} | T2: ₹{t2:.2f}\n"
            f"Risk: ₹{risk_inr:,.2f} ({risk_pct:.2f}%) | Setup: {setup_name}")

def fmt_t1_hit(symbol, t1_price, entry_price):
    return f"📊 <b>T1 HIT: {symbol}</b> @ ₹{t1_price:.2f}\nStop → breakeven ₹{entry_price:.2f}."

def fmt_trade_closed(symbol, exit_price, pnl, r_multiple, setup_name):
    s = "+" if pnl >= 0 else ""
    return (f"🏁 <b>TRADE CLOSED: {symbol}</b>\nExit: ₹{exit_price:.2f} | "
            f"P&L: {s}₹{pnl:,.2f}\nR: {s}{r_multiple:.2f}R | {setup_name}")

def fmt_stop_hit(symbol, stop_price, loss_inr, daily_pnl, remaining_pct):
    return (f"🛑 <b>STOP HIT: {symbol}</b> @ ₹{stop_price:.2f}\n"
            f"Loss: -₹{abs(loss_inr):,.2f} | Daily P&L: ₹{daily_pnl:,.2f} | "
            f"Limit remaining: {remaining_pct:.1f}%")

def fmt_force_exit(positions_summary, session_pnl):
    s = "+" if session_pnl >= 0 else ""
    lines = [f"⏰ <b>FORCE EXIT 15:15 — {len(positions_summary)} position(s)</b>"]
    for p in positions_summary:
        ps = "+" if p["pnl"] >= 0 else ""
        lines.append(f"{p['symbol']}: {ps}₹{p['pnl']:,.2f}")
    lines.append(f"Session P&L: {s}₹{session_pnl:,.2f}")
    return "\n".join(lines)

def fmt_daily_loss_limit():
    return ("🚨 <b>DAILY LOSS LIMIT HIT</b>: -2% reached.\n"
            "All trading halted. Positions NOT auto-closed. Send /resume tomorrow.")

def fmt_critical(component, timestamp, open_positions):
    pos = ", ".join(open_positions) if open_positions else "none"
    return (f"🚨 <b>CRITICAL: {component} unreachable at {timestamp}</b>\n"
            f"Open positions: {pos}\nAuto-retry every 60s. Send /resume after fix.")

def fmt_component_restored(component):
    return f"✅ <b>{component}</b> restored. Trading resumed."

def fmt_component_unrecoverable(component):
    return f"🚨 <b>{component}</b> unrecoverable. Manual restart required."

def fmt_tv_stale(minutes):
    return f"⚠️ No TradingView webhook in {minutes} min. Check Pine Script alerts."

def fmt_webhook_rejected(timestamp, ip):
    return f"🚨 <b>REJECTED WEBHOOK</b>: Invalid secret at {timestamp}. IP: {ip}"

def fmt_eod_report(date, capital, trades, wins, losses, session_pnl, session_pnl_pct,
                   best_symbol, best_r, worst_symbol, worst_r, daily_loss_used_pct,
                   weekly_pnl, weekly_pnl_pct, setup_name, setup_win_rate_20):
    wr = (wins / trades * 100) if trades else 0.0
    ss, ws = ("+" if session_pnl >= 0 else ""), ("+" if weekly_pnl >= 0 else "")
    return (f"📊 <b>SESSION SUMMARY — {date}</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f"Capital: ₹{capital:,.2f}\n"
            f"Trades: {trades} | Wins: {wins} | Losses: {losses} | WR: {wr:.0f}%\n"
            f"Session P&L: {ss}₹{session_pnl:,.2f} ({ss}{session_pnl_pct:.2f}%)\n"
            f"Best: {best_symbol} +{best_r:.1f}R | Worst: {worst_symbol} {worst_r:.1f}R\n"
            f"Daily limit used: {daily_loss_used_pct:.1f}% of 2%\n━━━━━━━━━━━━━━━━━━━━\n"
            f"Weekly P&L: {ws}₹{weekly_pnl:,.2f} ({ws}{weekly_pnl_pct:.2f}%)\n"
            f"Active setup: {setup_name} | WR (last 20): {setup_win_rate_20:.0f}%\n"
            f"━━━━━━━━━━━━━━━━━━━━\nNext backtest: Sunday 06:00 IST")
