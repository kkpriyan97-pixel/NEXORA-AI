"""Candice Brain: regime-aware, evidence-first multi-strategy market analysis.
Read-only DEMO analysis. No trade execution and no credential handling.
"""
from __future__ import annotations

from math import isfinite
from self_strategy import discover as discover_self_strategy

EXPIRIES = (1, 2, 3, 4, 5, 10, 15)


def _f(x, d=0.0):
    try:
        v = float(x)
        return v if isfinite(v) else d
    except Exception:
        return d


def _norm(c):
    return {
        "time": c.get("time", c.get("t")),
        "open": _f(c.get("open", c.get("o"))),
        "high": _f(c.get("high", c.get("h"))),
        "low": _f(c.get("low", c.get("l"))),
        "close": _f(c.get("close", c.get("c"))),
        "volume": _f(c.get("volume", c.get("v"))),
    }


def ema(v, n):
    if not v:
        return 0.0
    k = 2 / (n + 1)
    e = v[0]
    for x in v[1:]:
        e = x * k + e * (1 - k)
    return e


def rsi(v, n=14):
    if len(v) < n + 1:
        return 50.0
    gains = []
    losses = []
    for a, b in zip(v[-n - 1:-1], v[-n:]):
        d = b - a
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = sum(gains) / n
    al = sum(losses) / n
    return 100.0 if al == 0 else 100 - (100 / (1 + ag / al))


def atr(cs, n=14):
    if len(cs) < 2:
        return 0.0
    trs = [
        max(
            b["high"] - b["low"],
            abs(b["high"] - a["close"]),
            abs(b["low"] - a["close"]),
        )
        for a, b in zip(cs[-n - 1:-1], cs[-n:])
    ]
    return sum(trs) / len(trs) if trs else 0.0


def _trend15_info(cs):
    """Build a 15m regime from three completed 15x1m blocks.

    A block is directional only when its net move is large enough relative to
    its average candle range. The last two blocks must agree; three aligned
    blocks are treated as the strongest persistence state.
    """
    if len(cs) < 45:
        return "SIDEWAYS", 0

    blocks = [cs[-45:-30], cs[-30:-15], cs[-15:]]
    block_ranges = []
    directions = []
    closes = []
    highs = []
    lows = []

    for block in blocks:
        avg_range = sum(max(c["high"] - c["low"], 0.0) for c in block) / 15.0
        move = block[-1]["close"] - block[0]["open"]
        threshold = max(avg_range * 1.25, 1e-12)
        if move > threshold:
            directions.append("UP")
        elif move < -threshold:
            directions.append("DOWN")
        else:
            directions.append("SIDEWAYS")
        block_ranges.append(avg_range)
        closes.append(block[-1]["close"])
        highs.append(max(c["high"] for c in block))
        lows.append(min(c["low"] for c in block))

    last = directions[-1]
    if last not in {"UP", "DOWN"}:
        return "SIDEWAYS", 0

    persistence = sum(1 for x in reversed(directions) if x == last)
    if persistence < 2:
        return "SIDEWAYS", persistence

    if last == "UP":
        structure_ok = closes[-1] >= closes[-2] and highs[-1] >= highs[-2] and lows[-1] >= lows[-2]
    else:
        structure_ok = closes[-1] <= closes[-2] and lows[-1] <= lows[-2] and highs[-1] <= highs[-2]

    if not structure_ok:
        return "SIDEWAYS", 1

    return last, persistence


def _trend15(cs):
    return _trend15_info(cs)[0]


def _sequence_quality(cs, direction):
    """Recent 1m directional structure quality, normalized to [0,1]."""
    recent = cs[-6:]
    if len(recent) < 6:
        return 0.0
    if direction == "UP":
        hh = sum(1 for a, b in zip(recent, recent[1:]) if b["high"] > a["high"])
        hl = sum(1 for a, b in zip(recent, recent[1:]) if b["low"] > a["low"])
    else:
        hh = sum(1 for a, b in zip(recent, recent[1:]) if b["high"] < a["high"])
        hl = sum(1 for a, b in zip(recent, recent[1:]) if b["low"] < a["low"])
    return min(1.0, (hh + hl) / 10.0)


def _efficiency(v, n=8):
    """Directional efficiency: net displacement / total absolute movement."""
    if len(v) < n + 1:
        return 0.0
    window = v[-n - 1:]
    total = sum(abs(b - a) for a, b in zip(window[:-1], window[1:]))
    if total <= 0:
        return 0.0
    return min(1.0, abs(window[-1] - window[0]) / total)


def analyze_asset(asset, candles, price=None):
    cs = [_norm(c) for c in candles if isinstance(c, dict)]
    if len(cs) < 45:
        return None

    v = [c["close"] for c in cs]
    last = cs[-1]
    p = _f(price, last["close"])

    e9 = ema(v[-40:], 9)
    e21 = ema(v[-40:], 21)
    prev_e9 = ema(v[-45:-5], 9)
    rr = rsi(v)
    aa = atr(cs)

    body = last["close"] - last["open"]
    rng = max(last["high"] - last["low"], 1e-12)
    body_ratio = min(1.0, abs(body) / rng)
    upper = last["high"] - max(last["open"], last["close"])
    lower = min(last["open"], last["close"]) - last["low"]

    trend, trend_persistence = _trend15_info(cs)
    prev = cs[-2]["close"]
    momentum = p - prev
    momentum_norm = abs(momentum) / max(aa, 1e-12)

    avg_rng = sum(max(c["high"] - c["low"], 0.0) for c in cs[-14:]) / 14.0
    volatility_ratio = (aa / avg_rng) if avg_rng else 1.0

    resistance = max(c["high"] for c in cs[-20:-1])
    support = min(c["low"] for c in cs[-20:-1])

    breakout_up = p > resistance and body > 0
    breakout_down = p < support and body < 0
    breakout_distance_up = (p - resistance) / max(aa, 1e-12)
    breakout_distance_down = (support - p) / max(aa, 1e-12)

    near_support = (p - support) <= max(aa * 0.35, 1e-12)
    near_resistance = (resistance - p) <= max(aa * 0.35, 1e-12)

    bullish_rejection = lower > max(abs(body) * 1.2, rng * 0.35) and p >= last["open"]
    bearish_rejection = upper > max(abs(body) * 1.2, rng * 0.35) and p <= last["open"]

    pattern = (
        "BULLISH_CANDLE" if body > 0 and body_ratio >= 0.55 else
        "BEARISH_CANDLE" if body < 0 and body_ratio >= 0.55 else
        "BULLISH_REJECTION" if bullish_rejection else
        "BEARISH_REJECTION" if bearish_rejection else
        "NEUTRAL"
    )

    ema_gap_norm = abs(e9 - e21) / max(aa, 1e-12)
    ema_slope = e9 - prev_e9
    ema_slope_norm = abs(ema_slope) / max(aa, 1e-12)

    ema_bull = e9 > e21 and p >= e9
    ema_bear = e9 < e21 and p <= e9

    bull_recent = sum(1 for c in cs[-3:] if c["close"] > c["open"])
    bear_recent = sum(1 for c in cs[-3:] if c["close"] < c["open"])

    def aligned_recent(direction):
        return bull_recent if direction == "UP" else bear_recent

    def rsi_supports(direction):
        return (48 <= rr <= 68) if direction == "UP" else (32 <= rr <= 52)

    structure_quality = {
        "UP": _sequence_quality(cs, "UP"),
        "DOWN": _sequence_quality(cs, "DOWN"),
    }

    def score(direction, strategy):
        structure_ok = (
            direction == "UP" and ema_bull
        ) or (
            direction == "DOWN" and ema_bear
        )
        slope_ok = (
            direction == "UP" and ema_slope > 0
        ) or (
            direction == "DOWN" and ema_slope < 0
        )
        trend_ok = direction == trend

        if strategy == "BREAKOUT":
            active = breakout_up if direction == "UP" else breakout_down
            distance = breakout_distance_up if direction == "UP" else breakout_distance_down
            prior_inside = prev <= resistance if direction == "UP" else prev >= support
            if not active or distance < 0.08 or body_ratio < 0.45 or not prior_inside:
                return -1.0
            s = 62.0
            s += 10 if distance >= 0.15 else 4
            s += 8 if body_ratio >= 0.65 else 3
            s += 7 if trend_ok else 0
            s += 6 if slope_ok else 0
            s += 5 if aligned_recent(direction) >= 2 else 0
            s += 4 if structure_ok else 0
            return min(96.0, s)

        if strategy == "PULLBACK":
            location = near_support if direction == "UP" else near_resistance
            rejection = bullish_rejection if direction == "UP" else bearish_rejection
            aligned = trend_ok and structure_ok and slope_ok
            if trend not in {"UP", "DOWN"} or not location or not rejection or not aligned:
                return -1.0
            s = 58.0
            s += 10 if structure_quality[direction] >= 0.5 else 4
            s += 8 if momentum_norm >= 0.10 else 0
            s += 8 if body_ratio >= 0.30 else 3
            s += 6 if trend_persistence >= 2 else 0
            s += 4 if rsi_supports(direction) else 0
            return min(95.0, s)

        if strategy == "REVERSAL":
            extreme = (rr < 30) if direction == "UP" else (rr > 70)
            rejection = bullish_rejection if direction == "UP" else bearish_rejection
            location = near_support if direction == "UP" else near_resistance
            trend_weak = trend == "SIDEWAYS" or trend_persistence <= 1
            if not extreme or not rejection or not location:
                return -1.0
            s = 62.0
            s += 10 if trend_weak else 3
            s += 8 if structure_quality[direction] >= 0.4 else 0
            s += 8 if momentum_norm < 0.35 else 0
            s += 6 if body_ratio >= 0.30 else 0
            s += 5 if slope_ok else 0
            return min(94.0, s)

        if strategy == "MEAN_REVERSION":
            extreme = (rr < 27) if direction == "UP" else (rr > 73)
            location = near_support if direction == "UP" else near_resistance
            weak_momentum = momentum_norm < 0.35
            rejection = bullish_rejection if direction == "UP" else bearish_rejection
            if not extreme or not location or not weak_momentum:
                return -1.0
            s = 64.0
            s += 10 if trend == "SIDEWAYS" else 0
            s += 8 if rejection else 0
            s += 8 if structure_quality[direction] >= 0.4 else 0
            s += 6 if body_ratio >= 0.25 else 0
            return min(94.0, s)

        if strategy == "PRICE_ACTION":
            directional_pattern = (
                direction == "UP" and pattern in {"BULLISH_CANDLE", "BULLISH_REJECTION"}
            ) or (
                direction == "DOWN" and pattern in {"BEARISH_CANDLE", "BEARISH_REJECTION"}
            )
            if not directional_pattern:
                return -1.0
            location = near_support if direction == "UP" else near_resistance
            s = 58.0
            s += 12 if location else 5
            s += 8 if structure_ok else 0
            s += 7 if trend_ok else 0
            s += 7 if body_ratio >= 0.60 else 2
            s += 5 if slope_ok else 0
            return min(94.0, s)

        if strategy == "MOMENTUM":
            if momentum_norm < 0.30 or body_ratio < 0.45:
                return -1.0
            if (direction == "UP" and momentum <= 0) or (direction == "DOWN" and momentum >= 0):
                return -1.0
            if (direction == "UP" and rr >= 73) or (direction == "DOWN" and rr <= 27):
                return -1.0
            s = 60.0
            s += 10 if momentum_norm >= 0.50 else 5
            s += 8 if body_ratio >= 0.60 else 3
            s += 7 if aligned_recent(direction) >= 2 else 0
            s += 6 if trend_ok else 0
            s += 5 if structure_ok else 0
            s += 4 if slope_ok else 0
            return min(95.0, s)

        if strategy == "VOLATILITY":
            if volatility_ratio < 1.15 or momentum_norm < 0.30 or body_ratio < 0.45:
                return -1.0
            if trend in {"UP", "DOWN"} and not trend_ok:
                return -1.0
            s = 61.0
            s += 10 if volatility_ratio >= 1.30 else 4
            s += 8 if momentum_norm >= 0.50 else 3
            s += 7 if body_ratio >= 0.60 else 3
            s += 5 if trend_ok else 0
            s += 5 if structure_ok else 0
            return min(94.0, s)

        if strategy == "TREND_FOLLOWING":
            if not trend_ok or not structure_ok or not slope_ok:
                return -1.0
            if momentum_norm < 0.20:
                return -1.0
            if structure_quality[direction] < 0.40:
                return -1.0
            if trend_persistence < 2:
                return -1.0
            if (direction == "UP" and near_resistance and not breakout_up) or (
                direction == "DOWN" and near_support and not breakout_down
            ):
                return -1.0
            if (direction == "UP" and rr >= 74) or (direction == "DOWN" and rr <= 26):
                return -1.0

            s = 56.0
            s += 10 if trend_persistence == 3 else 5
            s += 9 if ema_gap_norm >= 0.15 else (6 if ema_gap_norm >= 0.08 else 2)
            s += 9 if ema_slope_norm >= 0.08 else (5 if ema_slope_norm >= 0.04 else 0)
            s += 9 if momentum_norm >= 0.40 else (5 if momentum_norm >= 0.25 else 2)
            s += 7 if structure_quality[direction] >= 0.70 else 3
            s += 6 if aligned_recent(direction) >= 2 else 0
            s += 4 if rsi_supports(direction) else 0
            s += 4 if _efficiency(v, 8) >= 0.45 else 0

            # Late-trend warning: a directionally aligned trend is less useful
            # when price is pressing directly into the opposing 20-bar level.
            if direction == "UP" and near_resistance and not breakout_up:
                s -= 12
            if direction == "DOWN" and near_support and not breakout_down:
                s -= 12
            return min(96.0, max(0.0, s))

        return -1.0

    self_profile = discover_self_strategy(
        trend=trend,
        structure="BULLISH" if ema_bull else "BEARISH" if ema_bear else "MIXED",
        rsi_value=rr,
        momentum=momentum,
        atr_value=aa,
        volatility_ratio=volatility_ratio,
        breakout_up=breakout_up,
        breakout_down=breakout_down,
        near_support=near_support,
        near_resistance=near_resistance,
        pattern=pattern,
        ema_gap_norm=ema_gap_norm,
        ema_slope_norm=ema_slope_norm,
        trend_persistence=trend_persistence,
        body_ratio=body_ratio,
        structure_quality=structure_quality,
        efficiency=_efficiency(v, 8),
    )

    strategies = (
        "TREND_FOLLOWING",
        "MOMENTUM",
        "PULLBACK",
        "BREAKOUT",
        "REVERSAL",
        "MEAN_REVERSION",
        "PRICE_ACTION",
        "VOLATILITY",
    )

    candidates = []
    for strategy in strategies:
        for direction in ("UP", "DOWN"):
            sc = score(direction, strategy)
            if sc < 0:
                continue
            if strategy == self_profile["strategy"]:
                sc = min(96.0, sc + min(4.0, float(self_profile.get("strength", 0.0))))
            if sc >= 72:
                candidates.append({
                    "direction": direction,
                    "strategy": strategy,
                    "score": round(sc, 2),
                    "expiry_minutes": 3,
                    "self_strategy_version": self_profile["version"],
                })

    if not candidates:
        return None

    valid = [
        x for x in candidates
        if not (trend == "UP" and x["direction"] == "DOWN")
        and not (trend == "DOWN" and x["direction"] == "UP")
    ]
    if not valid:
        return None

    ranked = sorted(valid, key=lambda x: x["score"], reverse=True)
    best = ranked[0]
    second_score = ranked[1]["score"] if len(ranked) > 1 else 0.0
    strategy_margin = max(0.0, best["score"] - second_score)
    if best["score"] < 82 or strategy_margin < 6:
        return None

    expected = best["direction"]

    # 5m is never a normal/default expiry. This flag only says that the market
    # structure is strong enough for the expiry-learning layer to consider it.
    five_minute_eligible = bool(
        best["strategy"] == "TREND_FOLLOWING"
        and expected == trend
        and trend_persistence == 3
        and structure_quality[expected] >= 0.70
        and ema_gap_norm >= 0.15
        and ema_slope_norm >= 0.08
        and momentum_norm >= 0.35
        and _efficiency(v, 8) >= 0.45
        and aligned_recent(expected) >= 2
        and body_ratio >= 0.55
        and not ((expected == "UP" and rr >= 72) or (expected == "DOWN" and rr <= 28))
        and not ((expected == "UP" and near_resistance and not breakout_up) or
                 (expected == "DOWN" and near_support and not breakout_down))
    )

    best["five_minute_eligible"] = five_minute_eligible
    confidence = max(0, min(96, int(round(best["score"]))))
    direction_agreement = (
        1.0
        if (
            expected == trend
            and ((expected == "UP" and ema_bull) or (expected == "DOWN" and ema_bear))
            and ((expected == "UP" and momentum > 0) or (expected == "DOWN" and momentum < 0))
        )
        else 0.0
    )

    reason = (
        f"{best['strategy']} | 15m={trend} (persist={trend_persistence}) | "
        f"1m={('BULLISH' if ema_bull else 'BEARISH' if ema_bear else 'MIXED')} | "
        f"RSI={rr:.1f} | momentum={momentum_norm:.2f}ATR | "
        f"EMA_gap={ema_gap_norm:.2f}ATR | EMA_slope={ema_slope_norm:.2f}ATR | "
        f"structure_q={structure_quality[expected]:.2f} | body={body_ratio:.2f} | "
        f"efficiency={_efficiency(v, 8):.2f} | pattern={pattern} | "
        f"volatility={volatility_ratio:.2f} | support={support:.6g} | resistance={resistance:.6g}"
    )

    return {
        "pair": str(asset.get("pair", "")),
        "display_name": str(asset.get("display_name") or asset.get("title") or ""),
        "direction": expected,
        "confidence": confidence,
        "strategy": best["strategy"],
        "expiry_minutes": best["expiry_minutes"],
        "five_minute_eligible": five_minute_eligible,
        "self_strategy": self_profile["strategy"],
        "self_strategy_strength": self_profile["strength"],
        "self_strategy_weights": self_profile.get("weights", {}),
        "self_strategy_margin": self_profile.get("margin", 0.0),
        "pattern": pattern,
        "trend_15m": trend,
        "structure_1m": ("BULLISH" if ema_bull else "BEARISH" if ema_bear else "MIXED"),
        "market_quality": best["score"],
        "strategy_margin": round(strategy_margin, 2),
        "direction_agreement": direction_agreement,
        "reason": reason,
        "self_strategy_version": best.get("self_strategy_version", ""),
        "entry_candle_ts": last["time"],
        "price": p,
        "support": support,
        "resistance": resistance,
        "atr": aa,
        "momentum": momentum,
        "momentum_norm": momentum_norm,
        "ema_gap_norm": ema_gap_norm,
        "ema_slope_norm": ema_slope_norm,
        "body_ratio": body_ratio,
        "volatility_ratio": volatility_ratio,
        "trend_persistence": trend_persistence,
        "structure_quality": structure_quality[expected],
        "efficiency": _efficiency(v, 8),
        "evidence": {
            "trend": trend,
            "trend_persistence": trend_persistence,
            "structure": ("BULLISH" if ema_bull else "BEARISH" if ema_bear else "MIXED"),
            "structure_quality": structure_quality[expected],
            "momentum_norm": momentum_norm,
            "ema_gap_norm": ema_gap_norm,
            "ema_slope_norm": ema_slope_norm,
            "volatility": volatility_ratio,
            "efficiency": _efficiency(v, 8),
            "breakout_up": breakout_up,
            "breakout_down": breakout_down,
            "near_support": near_support,
            "near_resistance": near_resistance,
            "pattern": pattern,
            "recent_aligned_candles": aligned_recent(expected),
        },
    }
