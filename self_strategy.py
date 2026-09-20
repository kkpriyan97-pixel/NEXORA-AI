"""Market-derived strategy router for Candice Brain.
The router selects the most relevant technique for the observed regime.
It is advisory only; candice_brain.py remains the final evidence gate.
"""
from __future__ import annotations

VERSION = "SELF-MARKET-V3"


def _f(x, default=0.0):
    try:
        v = float(x)
        return v if v == v else default
    except Exception:
        return default


def discover(*, trend, structure, rsi_value, momentum, atr_value,
             volatility_ratio, breakout_up, breakout_down,
             near_support, near_resistance, pattern,
             ema_gap_norm=0.0, ema_slope_norm=0.0, trend_persistence=0,
             body_ratio=0.0, structure_quality=0.0, efficiency=0.0):
    rr = _f(rsi_value, 50.0)
    mom = _f(momentum)
    atr = max(abs(_f(atr_value)), 1e-12)
    vr = _f(volatility_ratio, 1.0)
    mom_norm = abs(mom) / atr
    eq = _f(ema_gap_norm)
    es = _f(ema_slope_norm)
    tp = int(_f(trend_persistence))
    br = _f(body_ratio)
    eff = _f(efficiency)

    if isinstance(structure_quality, dict):
        sq = {
            "UP": _f(structure_quality.get("UP")),
            "DOWN": _f(structure_quality.get("DOWN")),
        }
    else:
        sq = {"UP": _f(structure_quality), "DOWN": _f(structure_quality)}

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

    # Strategy selection is a regime decision, not a direction decision.
    if trend in {"UP", "DOWN"} and tp >= 2:
        t = 18
        if eq >= 0.15:
            t += 7
        elif eq >= 0.08:
            t += 4
        if es >= 0.08:
            t += 6
        elif es >= 0.04:
            t += 3
        if mom_norm >= 0.35:
            t += 5
        if eff >= 0.45:
            t += 4
        if sq.get(trend, 0.0) >= 0.70:
            t += 5
        weights["TREND_FOLLOWING"] = t

    if mom_norm >= 0.30 and br >= 0.45:
        m = 18
        if mom_norm >= 0.50:
            m += 7
        if br >= 0.60:
            m += 5
        if trend in {"UP", "DOWN"}:
            m += 3
        weights["MOMENTUM"] = m

    if breakout_up or breakout_down:
        b = 30
        if mom_norm >= 0.30:
            b += 6
        if br >= 0.55:
            b += 5
        weights["BREAKOUT"] = b

    if trend in {"UP", "DOWN"} and (near_support or near_resistance):
        rejection = (
            (pattern == "BULLISH_REJECTION" and near_support)
            or (pattern == "BEARISH_REJECTION" and near_resistance)
        )
        if rejection:
            p = 24
            if tp >= 2:
                p += 6
            if sq.get(trend, 0.0) >= 0.50:
                p += 4
            weights["PULLBACK"] = p

    reversal_ok = (
        (rr < 32 and pattern == "BULLISH_REJECTION")
        or (rr > 68 and pattern == "BEARISH_REJECTION")
    )
    if reversal_ok:
        r = 26
        if trend == "SIDEWAYS" or tp <= 1:
            r += 6
        if mom_norm < 0.35:
            r += 4
        weights["REVERSAL"] = r

    mean_ok = (
        (rr < 28 or rr > 72)
        and (near_support or near_resistance)
        and mom_norm < 0.35
    )
    if mean_ok:
        weights["MEAN_REVERSION"] = 28 if trend == "SIDEWAYS" else 23

    if pattern != "NEUTRAL":
        pa = 18
        if near_support or near_resistance:
            pa += 7
        if br >= 0.60:
            pa += 4
        weights["PRICE_ACTION"] = pa

    if vr >= 1.15 and mom_norm >= 0.30 and br >= 0.45:
        vv = 22
        if vr >= 1.30:
            vv += 5
        if mom_norm >= 0.50:
            vv += 4
        weights["VOLATILITY"] = vv

    ordered = sorted(weights.items(), key=lambda item: item[1], reverse=True)
    strategy, best = ordered[0]
    second = ordered[1][1] if len(ordered) > 1 else 0.0
    margin = max(0.0, best - second)

    if best < 24 or margin < 6:
        strategy = "NEUTRAL_ROUTER"
        strength = 0.0
    else:
        strength = min(4.0, best / 8.0)

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
            "ema_gap_norm": round(eq, 3),
            "ema_slope_norm": round(es, 3),
            "trend_persistence": tp,
            "efficiency": round(eff, 3),
        },
    }
