"""
agent1_setups.py — VRVP+SVP setup simulators for Agent 1 backtester.

Imported by agent1_backtester._simulate_setup().
Each function receives: candles (list[dict]), vrvp (VolumeProfileResult), avg_vol (float).
Returns list of trade dicts: {entry, exit, direction, quantity, r_multiple, exit_reason}.
"""

import pandas as pd
from app.vrvp import VolumeProfileResult, compute_svp

TICK_SIZE = 0.05


# ── Setup 1: POC Magnet Pull ──────────────────────────────────────────────────

def setup1_poc_magnet(candles, vrvp, avg_vol):
    trades = []
    svp_candles = []
    for i, bar in enumerate(candles):
        svp_candles.append(bar)
        if len(svp_candles) < 3 or i + 1 >= len(candles):
            continue
        svp = compute_svp(pd.DataFrame(svp_candles))
        if svp is None:
            continue
        poc = svp.poc
        price = bar["close"]
        dist_pct = abs(price - poc) / poc
        if dist_pct < 0.005 or bar["volume"] >= avg_vol:
            continue
        direction = "Long" if price < poc else "Short"
        is_rej = ((direction == "Long" and bar["close"] > bar["open"])
                  or (direction == "Short" and bar["close"] < bar["open"]))
        if not is_rej:
            continue
        entry = candles[i + 1]["open"]
        if direction == "Long":
            stop, t1, t2 = bar["low"] - 5 * TICK_SIZE, poc, svp.vah
        else:
            stop, t1, t2 = bar["high"] + 5 * TICK_SIZE, poc, svp.val
        sd = abs(entry - stop)
        if sd < TICK_SIZE or abs(t2 - entry) / sd < 2.0:
            continue
        ep, er = simulate_exit(candles[i + 2:], direction, entry, stop, t1, t2)
        trades.append({"entry": entry, "exit": ep, "direction": direction, "quantity": 1,
                       "r_multiple": calc_r(direction, entry, ep, stop), "exit_reason": er})
    return trades


# ── Setup 2: Value Area Breakout ──────────────────────────────────────────────

def setup2_va_breakout(candles, vrvp, avg_vol):
    trades = []
    for i, bar in enumerate(candles):
        if i + 1 >= len(candles):
            continue
        vah, val = vrvp.vah, vrvp.val
        if (bar["close"] > vah and bar["volume"] >= 1.5 * avg_vol
                and vrvp.has_lvn_immediately_above(vah)):
            direction = "Long"
            entry = bar["high"] + TICK_SIZE
            stop = vah - 10 * TICK_SIZE
            t1 = vrvp.nearest_hvn_above(vah) or (vah + (vah - val) * 0.5)
            t2 = vrvp.nearest_hvn_above(t1) or (t1 + abs(t1 - entry))
        elif (bar["close"] < val and bar["volume"] >= 1.5 * avg_vol
              and vrvp.has_lvn_immediately_below(val)):
            direction = "Short"
            entry = bar["low"] - TICK_SIZE
            stop = val + 10 * TICK_SIZE
            t1 = vrvp.nearest_hvn_below(val) or (val - (vah - val) * 0.5)
            t2 = vrvp.nearest_hvn_below(t1) or (t1 - abs(t1 - entry))
        else:
            continue
        sd = abs(entry - stop)
        if sd < TICK_SIZE or abs(t2 - entry) / sd < 2.0:
            continue
        ep, er = simulate_exit(candles[i + 1:], direction, entry, stop, t1, t2)
        trades.append({"entry": entry, "exit": ep, "direction": direction, "quantity": 1,
                       "r_multiple": calc_r(direction, entry, ep, stop), "exit_reason": er})
    return trades


# ── Setup 3: HVN Rejection ────────────────────────────────────────────────────

def setup3_hvn_rejection(candles, vrvp, avg_vol):
    trades = []
    for i, bar in enumerate(candles):
        if not vrvp.hvns or i + 1 >= len(candles):
            continue
        nearest = min(vrvp.hvns, key=lambda h: abs(h - bar["close"]))
        if abs(bar["close"] - nearest) / nearest > 0.005 or bar["volume"] >= avg_vol:
            continue
        rng = bar["high"] - bar["low"]
        if rng < TICK_SIZE:
            continue
        if bar["high"] >= nearest > bar["close"] and (bar["high"] - bar["close"]) / rng >= 0.60:
            direction = "Short"
            entry = candles[i + 1]["open"]
            stop = bar["high"] + 5 * TICK_SIZE
            t2 = vrvp.poc
        elif bar["low"] <= nearest < bar["close"] and (bar["close"] - bar["low"]) / rng >= 0.60:
            direction = "Long"
            entry = candles[i + 1]["open"]
            stop = bar["low"] - 5 * TICK_SIZE
            t2 = vrvp.poc
        else:
            continue
        sd = abs(entry - stop)
        if sd < TICK_SIZE or abs(t2 - entry) / sd < 1.5:
            continue
        ep, er = simulate_exit(candles[i + 1:], direction, entry, stop, t2, t2)
        trades.append({"entry": entry, "exit": ep, "direction": direction, "quantity": 1,
                       "r_multiple": calc_r(direction, entry, ep, stop), "exit_reason": er})
    return trades


# ── Setup 4: LVN Acceleration ─────────────────────────────────────────────────

def setup4_lvn_acceleration(candles, vrvp, avg_vol):
    trades = []
    for i, bar in enumerate(candles):
        if not vrvp.lvns or i + 1 >= len(candles):
            continue
        lvn = min(vrvp.lvns, key=lambda l: abs(l - bar["close"]))
        if abs(bar["close"] - lvn) / lvn >= 0.003 or bar["volume"] < avg_vol * 0.8:
            continue
        direction = "Long" if bar["close"] >= lvn else "Short"
        entry = bar["close"]
        if direction == "Long":
            stop = vrvp.nearest_hvn_below(lvn) or (lvn - 10 * TICK_SIZE)
            t2 = vrvp.nearest_hvn_above(lvn) or (lvn + abs(lvn - stop) * 2)
        else:
            stop = vrvp.nearest_hvn_above(lvn) or (lvn + 10 * TICK_SIZE)
            t2 = vrvp.nearest_hvn_below(lvn) or (lvn - abs(lvn - stop) * 2)
        sd = abs(entry - stop)
        if sd < TICK_SIZE or abs(t2 - entry) / sd < 2.0:
            continue
        ep, er = simulate_exit(candles[i:], direction, entry, stop, t2, t2)
        trades.append({"entry": entry, "exit": ep, "direction": direction, "quantity": 1,
                       "r_multiple": calc_r(direction, entry, ep, stop), "exit_reason": er})
    return trades


# ── Setup 5: Open Drive ───────────────────────────────────────────────────────

def setup5_open_drive(candles, vrvp, avg_vol):
    if len(candles) < 4:
        return []
    bar = candles[0]
    rng = bar["high"] - bar["low"]
    if rng < TICK_SIZE:
        return []
    body = abs(bar["close"] - bar["open"])
    if (1 - body / rng) > 0.20 or bar["volume"] < 2 * avg_vol:
        return []
    if bar["open"] > vrvp.vah:
        direction, boundary = "Long", vrvp.vah
    elif bar["open"] < vrvp.val:
        direction, boundary = "Short", vrvp.val
    else:
        return []
    cum_v = cum_vp = 0.0
    for b in candles[:5]:
        typ = (b["high"] + b["low"] + b["close"]) / 3
        cum_v += b["volume"]
        cum_vp += typ * b["volume"]
    vwap = cum_vp / cum_v if cum_v > 0 else bar["open"]
    entry, stop = vwap, boundary
    sd = abs(entry - stop)
    if sd < TICK_SIZE:
        return []
    if direction == "Long":
        t1 = vrvp.nearest_hvn_above(entry) or (entry + sd * 1.5)
        t2 = vrvp.poc if vrvp.poc > entry else (entry + sd * 3)
    else:
        t1 = vrvp.nearest_hvn_below(entry) or (entry - sd * 1.5)
        t2 = vrvp.poc if vrvp.poc < entry else (entry - sd * 3)
    if abs(t2 - entry) / sd < 2.0:
        return []
    ep, er = simulate_exit(candles[1:], direction, entry, stop, t1, t2)
    return [{"entry": entry, "exit": ep, "direction": direction, "quantity": 1,
             "r_multiple": calc_r(direction, entry, ep, stop), "exit_reason": er}]


# ── Shared helpers ────────────────────────────────────────────────────────────

def simulate_exit(bars, direction, entry, stop, t1, t2):
    t1_hit = False
    for bar in bars:
        h, l = bar["high"], bar["low"]
        if direction == "Long":
            if not t1_hit and l <= stop:
                return stop, "STOP_HIT"
            if not t1_hit and h >= t1:
                t1_hit = True
            if t1_hit and l <= entry:
                return entry, "STOP_BREAKEVEN"
            if h >= t2:
                return t2, "TARGET_2"
        else:
            if not t1_hit and h >= stop:
                return stop, "STOP_HIT"
            if not t1_hit and l <= t1:
                t1_hit = True
            if t1_hit and h >= entry:
                return entry, "STOP_BREAKEVEN"
            if l <= t2:
                return t2, "TARGET_2"
    return (bars[-1]["close"] if bars else entry), "FORCE_EXIT"


def calc_r(direction, entry, exit_price, stop):
    sd = abs(entry - stop)
    if sd < 1e-9:
        return 0.0
    return (exit_price - entry) / sd if direction == "Long" else (entry - exit_price) / sd
