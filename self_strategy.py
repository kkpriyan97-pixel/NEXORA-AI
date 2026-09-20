"""Market-derived strategy router for Candice Brain.

The router chooses which technique is most relevant to the current completed
candle regime. It is a bounded advisory layer: it never executes trades and
cannot override the evidence gates in candice_brain.py.
"""
from __future__ import annotations

VERSION = "SELF-MARKET-V2"


def _f(x, default=0.0):
    try:
        v = float(x)
        return v if v == v else default
    except Exception:
        return default


def discover(*, trend, structure, rsi_value, momentum, atr_value,
             volatility_ratio, breakout_up, breakout_down,
             near_support, near_resistance, pattern):
    rr = _f(rsi_value, 50.0)
    mom = _f(momentum)
    atr = max(abs(_f(atr_value)), 1e-12)
    vr = _f(volatility_ratio, 1.0)
    mom_norm = abs(mom) / atr

    weights = {
        "TREND_FOLLOWING": 0.0,
        "MOMENTUM": 0.0,
        "PULLBACK": 0.0,
        "BREAKOUT": 0.0,
        "REVERSAL": 0.0,
        "MEAN_REVERSION": 0.0,
        "PRICE_ACTION": 0.0,
        "VOLATILITY": 0.0,
    }

    # Trend following requires direction + structure + live directional momentum.
    trend_aligned = (
        trend in ("UP", "DOWN")
        and (
            (trend == "UP" and structure == "BULLISH" and mom > 0)
            or (trend == "DOWN" and structure == "BEARISH" and mom < 0)
        )
    )
    if trend_aligned:
        weights["TREND_FOLLOWING"] += 20
    if trend_aligned and mom_norm >= 0.25:
        weights["TREND_FOLLOWING"] += 6
    if trend_aligned and pattern in {"BULLISH_CANDLE", "BEARISH_CANDLE"}:
        weights["TREND_FOLLOWING"] += 2

    if mom_norm >= 0.30 and (
        trend in ("SIDEWAYS", "UP", "DOWN")
    ):
        weights["MOMENTUM"] += 20
        if pattern in {"BULLISH_CANDLE", "BEARISH_CANDLE"}:
            weights["MOMENTUM"] += 5
        if ((trend == "UP" and mom > 0) or (trend == "DOWN" and mom < 0)):
            weights["MOMENTUM"] += 5

    if breakout_up or breakout_down:
        weights["BREAKOUT"] += 28
        if mom_norm >= 0.30:
            weights["BREAKOUT"] += 5

    if trend in ("UP", "DOWN") and (
        near_support or near_resistance
    ) and pattern in {"BULLISH_REJECTION", "BEARISH_REJECTION"}:
        weights["PULLBACK"] += 22
        if ((trend == "UP" and near_support) or
                (trend == "DOWN" and near_resistance)):
            weights["PULLBACK"] += 5

    reversal_ok = (
        (rr < 32 and pattern == "BULLISH_REJECTION")
        or (rr > 68 and pattern == "BEARISH_REJECTION")
    )
    if reversal_ok:
        weights["REVERSAL"] += 25
        if mom_norm < 0.35:
            weights["REVERSAL"] += 5

    mean_ok = (
        (rr < 28 or rr > 72)
        and (near_support or near_resistance)
        and mom_norm < 0.35
    )
    if mean_ok:
        weights["MEAN_REVERSION"] += 24

    if pattern != "NEUTRAL":
        weights["PRICE_ACTION"] += 15
        if near_support or near_resistance:
            weights["PRICE_ACTION"] += 8

    if vr >= 1.15 and mom_norm >= 0.30:
        weights["VOLATILITY"] += 22
        if pattern in {"BULLISH_CANDLE", "BEARISH_CANDLE"}:
            weights["VOLATILITY"] += 5

    ordered = sorted(weights.items(), key=lambda item: item[1], reverse=True)
    strategy, best = ordered[0]
    second = ordered[1][1] if len(ordered) > 1 else 0.0
    margin = max(0.0, best - second)

    # Do not let the router amplify an ambiguous regime. Candice Brain's
    # evidence scoring remains the final authority.
    if best < 20 or margin < 4:
        strategy = "NEUTRAL_ROUTER"
        strength = 0.0
    else:
        strength = min(8.0, best)

    return {
        "version": VERSION,
        "strategy": strategy,
        "strength": round(strength, 2),
        "margin": round(margin, 2),
        "weights": {k: round(v, 2) for k, v in weights.items()},
        "regime": {
            "trend": trend,
            "structure": structure,
            "rsi": round(rr, 2),
            "momentum_norm": round(mom_norm, 3),
            "volatility_ratio": round(vr, 3),
        },
    }
