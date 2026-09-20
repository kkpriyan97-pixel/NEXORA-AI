"""Market-derived self strategy discovery for Candice Brain.

This module does not execute trades or rewrite Python code. It derives a
strategy profile from the current completed-candle market regime and returns
versioned weights that the existing Brain can use without replacing its
original evidence rules.
"""
from __future__ import annotations

from math import isfinite


VERSION = "SELF-MARKET-V1"


def _f(x, default=0.0):
    try:
        v = float(x)
        return v if isfinite(v) else default
    except Exception:
        return default


def discover(*, trend, structure, rsi_value, momentum, atr_value,
             volatility_ratio, breakout_up, breakout_down,
             near_support, near_resistance, pattern):
    """Create a market-specific strategy profile from current evidence."""
    rr = _f(rsi_value, 50.0)
    mom = _f(momentum)
    atr = max(_f(atr_value), 1e-12)
    vr = _f(volatility_ratio, 1.0)

    profile = {
        "TREND_FOLLOWING": 0.0,
        "MOMENTUM": 0.0,
        "PULLBACK": 0.0,
        "BREAKOUT": 0.0,
        "REVERSAL": 0.0,
        "MEAN_REVERSION": 0.0,
        "PRICE_ACTION": 0.0,
        "VOLATILITY": 0.0,
    }

    if trend in ("UP", "DOWN"):
        profile["TREND_FOLLOWING"] += 18
    if structure in ("BULLISH", "BEARISH"):
        profile["TREND_FOLLOWING"] += 6

    if abs(mom) >= atr * 0.25:
        profile["MOMENTUM"] += 20
    if (trend == "UP" and mom > 0) or (trend == "DOWN" and mom < 0):
        profile["MOMENTUM"] += 8

    if breakout_up or breakout_down:
        profile["BREAKOUT"] += 26

    if near_support or near_resistance:
        profile["PULLBACK"] += 16

    if (rr < 35 and pattern == "BULLISH_REJECTION") or (
        rr > 65 and pattern == "BEARISH_REJECTION"
    ):
        profile["REVERSAL"] += 24

    if rr < 30 or rr > 70:
        profile["MEAN_REVERSION"] += 18

    if pattern != "NEUTRAL":
        profile["PRICE_ACTION"] += 14

    if vr >= 1.15:
        profile["VOLATILITY"] += 18

    strategy = max(profile, key=profile.get)
    strength = min(25.0, profile[strategy])

    return {
        "version": VERSION,
        "strategy": strategy,
        "strength": round(strength, 2),
        "weights": {k: round(v, 2) for k, v in profile.items()},
        "regime": {
            "trend": trend,
            "structure": structure,
            "rsi": round(rr, 2),
            "volatility_ratio": round(vr, 3),
        },
    }
