"""
vrvp.py — Volume Profile computation (VRVP and SVP).

Used ONLY by Agent 1 (Sunday backtester). Never called during market hours.
TradingView Pine Script handles all live VRVP/SVP computation.

VRVP — Visible Range Volume Profile
  Computed over the last N completed sessions (default 20).
  Returns POC, VAH, VAL, HVNs, LVNs.

SVP — Session Volume Profile
  Computed over the bars of a single session from 09:15 onward.
  Resets each day. Used for T1/T2 target computation in backtester.
"""

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class VolumeProfileResult:
    poc: float                      # Price Of Control — highest volume bucket
    vah: float                      # Value Area High (upper bound of 70% zone)
    val: float                      # Value Area Low  (lower bound of 70% zone)
    hvns: list[float] = field(default_factory=list)  # High Volume Nodes
    lvns: list[float] = field(default_factory=list)  # Low Volume Nodes
    bucket_centers: np.ndarray = field(default_factory=lambda: np.array([]))
    volumes: np.ndarray = field(default_factory=lambda: np.array([]))

    def nearest_hvn_above(self, price: float) -> Optional[float]:
        above = [h for h in self.hvns if h > price]
        return min(above) if above else None

    def nearest_hvn_below(self, price: float) -> Optional[float]:
        below = [h for h in self.hvns if h < price]
        return max(below) if below else None

    def nearest_lvn_above(self, price: float) -> Optional[float]:
        above = [l for l in self.lvns if l > price]
        return min(above) if above else None

    def nearest_lvn_below(self, price: float) -> Optional[float]:
        below = [l for l in self.lvns if l < price]
        return max(below) if below else None

    def has_lvn_immediately_above(self, price: float, tolerance_pct: float = 0.005) -> bool:
        """Return True if there is an LVN within `tolerance_pct` above price."""
        threshold = price * (1 + tolerance_pct)
        return any(price < l <= threshold for l in self.lvns)

    def has_lvn_immediately_below(self, price: float, tolerance_pct: float = 0.005) -> bool:
        threshold = price * (1 - tolerance_pct)
        return any(threshold <= l < price for l in self.lvns)


def compute_vrvp(
    candles_df: pd.DataFrame,
    rows: int = 200,
    value_area_pct: float = 0.70,
    hvn_min_volume_pct: float = 0.015,  # HVN must be at least 1.5% of total volume
) -> Optional[VolumeProfileResult]:
    """
    Compute VRVP from a DataFrame of candles.

    Args:
        candles_df: DataFrame with columns [high, low, close, volume].
                    Rows = 15-min bars over the lookback window.
        rows: number of price buckets (default 200 as per strategy spec)
        value_area_pct: value area size (default 0.70 = 70%)
        hvn_min_volume_pct: minimum volume fraction for HVN classification

    Returns:
        VolumeProfileResult or None if insufficient data.
    """
    if candles_df is None or len(candles_df) < 10:
        return None

    df = candles_df.copy()
    price_min = df["low"].min()
    price_max = df["high"].max()

    if price_max <= price_min:
        return None

    # Build price buckets
    bucket_edges = np.linspace(price_min, price_max, rows + 1)
    bucket_size = bucket_edges[1] - bucket_edges[0]
    bucket_centers = (bucket_edges[:-1] + bucket_edges[1:]) / 2
    volume_profile = np.zeros(rows)

    # Distribute volume into overlapping buckets (vectorised per candle)
    for _, row in df.iterrows():
        h = float(row["high"])
        l = float(row["low"])
        v = float(row["volume"])
        candle_range = h - l

        if candle_range < 1e-9:
            # Zero-range bar — assign all volume to the close bucket
            idx = np.searchsorted(bucket_edges[1:], float(row["close"]), side="left")
            idx = min(idx, rows - 1)
            volume_profile[idx] += v
            continue

        # Vectorised: compute overlap of candle with each bucket
        bucket_lows = bucket_edges[:-1]
        bucket_highs = bucket_edges[1:]
        overlap = (np.minimum(h, bucket_highs) - np.maximum(l, bucket_lows)).clip(min=0)
        volume_profile += v * overlap / candle_range

    # POC
    poc_idx = int(np.argmax(volume_profile))
    poc = float(bucket_centers[poc_idx])

    # Value Area — expand from POC outward, always adding the higher adjacent bucket
    total_volume = volume_profile.sum()
    if total_volume == 0:
        return None

    target = total_volume * value_area_pct
    va_low_idx = poc_idx
    va_high_idx = poc_idx
    accumulated = volume_profile[poc_idx]

    while accumulated < target:
        can_up = va_high_idx + 1 < rows
        can_dn = va_low_idx - 1 >= 0

        if can_up and can_dn:
            vol_up = volume_profile[va_high_idx + 1]
            vol_dn = volume_profile[va_low_idx - 1]
            if vol_up >= vol_dn:
                va_high_idx += 1
                accumulated += volume_profile[va_high_idx]
            else:
                va_low_idx -= 1
                accumulated += volume_profile[va_low_idx]
        elif can_up:
            va_high_idx += 1
            accumulated += volume_profile[va_high_idx]
        elif can_dn:
            va_low_idx -= 1
            accumulated += volume_profile[va_low_idx]
        else:
            break

    vah = float(bucket_edges[va_high_idx + 1])
    val = float(bucket_edges[va_low_idx])

    # HVNs — local maxima that exceed hvn_min_volume_pct of total
    min_hvn_vol = total_volume * hvn_min_volume_pct
    hvns: list[float] = []
    lvns: list[float] = []
    for i in range(1, rows - 1):
        if (volume_profile[i] > volume_profile[i - 1]
                and volume_profile[i] > volume_profile[i + 1]
                and volume_profile[i] >= min_hvn_vol):
            hvns.append(float(bucket_centers[i]))
        if (volume_profile[i] < volume_profile[i - 1]
                and volume_profile[i] < volume_profile[i + 1]):
            lvns.append(float(bucket_centers[i]))

    return VolumeProfileResult(
        poc=poc,
        vah=vah,
        val=val,
        hvns=hvns,
        lvns=lvns,
        bucket_centers=bucket_centers,
        volumes=volume_profile,
    )


def compute_svp(
    session_candles_df: pd.DataFrame,
    rows: int = 100,
    value_area_pct: float = 0.70,
) -> Optional[VolumeProfileResult]:
    """
    Compute Session Volume Profile from a single day's bars.
    Identical algorithm to compute_vrvp but uses fewer buckets for speed.
    """
    return compute_vrvp(session_candles_df, rows=rows, value_area_pct=value_area_pct)


def split_into_sessions(candles_df: pd.DataFrame) -> list[pd.DataFrame]:
    """
    Split a candle DataFrame into daily sessions.
    Assumes candles_df has a DatetimeTZ 'timestamp' column.
    Returns list of per-day DataFrames, sorted oldest → newest.
    """
    candles_df = candles_df.copy()
    candles_df["date"] = candles_df["timestamp"].dt.date
    sessions = []
    for day_date, group in candles_df.groupby("date"):
        sessions.append(group.drop(columns=["date"]).reset_index(drop=True))
    return sessions


def rolling_vrvp(
    sessions: list[pd.DataFrame],
    lookback: int = 20,
    rows: int = 200,
    value_area_pct: float = 0.70,
) -> list[Optional[VolumeProfileResult]]:
    """
    For each session[i], compute VRVP over sessions[max(0,i-lookback):i].
    i.e., the VRVP uses only COMPLETED sessions before the target day.

    Returns a list of VolumeProfileResult (or None) aligned with sessions.
    Element [i] is the VRVP context for day i.
    """
    results: list[Optional[VolumeProfileResult]] = []
    for i in range(len(sessions)):
        start = max(0, i - lookback)
        window = sessions[start:i]  # Exclude current session (no look-ahead)
        if len(window) < 5:
            results.append(None)
            continue
        combined = pd.concat(window, ignore_index=True)
        results.append(compute_vrvp(combined, rows=rows, value_area_pct=value_area_pct))
    return results
