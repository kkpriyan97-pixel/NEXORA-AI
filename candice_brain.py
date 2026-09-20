"""Candice Brain: evidence-first multi-strategy, multi-timeframe market analysis."""
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
    g, l = [], []
    for a, b in zip(v[-n - 1:-1], v[-n:]):
        d = b - a
        g.append(max(d, 0))
        l.append(max(-d, 0))
    ag = sum(g) / n
    al = sum(l) / n
    return 100.0 if al == 0 else 100 - (100 / (1 + ag / al))


def atr(cs, n=14):
    if len(cs) < 2:
        return 0.0
    t = [
        max(
            b["high"] - b["low"],
            abs(b["high"] - a["close"]),
            abs(b["low"] - a["close"]),
        )
        for a, b in zip(cs[-n - 1:-1], cs[-n:])
    ]
    return sum(t) / len(t) if t else 0.0


def _trend15_info(cs):
    """Infer higher-timeframe direction from completed 15-candle blocks."""
    blocks = [cs[i:i + 15] for i in range(max(0, len(cs) - 75), len(cs), 15)]
    blocks = [b for b in blocks if len(b) == 15]
    if len(blocks) < 3:
        return "SIDEWAYS", 0

    directions = []
    for block in blocks:
        move = block[-1]["close"] - block[0]["open"]
        if move > 0:
            directions.append("UP")
        elif move < 0:
            directions.append("DOWN")
        else:
            directions.append("SIDEWAYS")

    recent = directions[-3:]
    last = recent[-1]
    persistence = sum(1 for x in reversed(recent) if x == last)
    if last in ("UP", "DOWN") and persistence >= 2:
        return last, persistence
    return "SIDEWAYS", persistence


def _trend15(cs):
    return _trend15_info(cs)[0]


def analyze_asset(asset, candles, price=None):
    cs = [_norm(c) for c in candles if isinstance(c, dict)]
    if len(cs) < 45:
        return None

    v = [c["close"] for c in cs]
    last = cs[-1]
    p = _f(price, last["close"])

    e9 = ema(v[-40:], 9)
    e21 = ema(v[-40:], 21)
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

    avg_rng = sum(max(c["high"] - c["low"], 0) for c in cs[-14:]) / 14
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
    ema_bull = e9 > e21 and p >= e9
    ema_bear = e9 < e21 and p <= e9

    recent = cs[-3:]
    bull_candles = sum(1 for c in recent if c["close"] > c["open"])
    bear_candles = sum(1 for c in recent if c["close"] < c["open"])

    def aligned_recent(direction):
        return bull_candles if direction == "UP" else bear_candles

    def rsi_supports(direction):
        return (50 <= rr <= 68) if direction == "UP" else (32 <= rr <= 50)

    def common_alignment(direction):
        score = 0.0
        if direction == trend:
            score += 8
        if (direction == "UP" and ema_bull) or (direction == "DOWN" and ema_bear):
            score += 8
        if (direction == "UP" and structure == "BULLISH") or (direction == "DOWN" and structure == "BEARISH"):
            score += 8
        return score

    structure = (
        "BULLISH" if ema_bull else
        "BEARISH" if ema_bear else
        "MIXED"
    )

    def score(direction, strategy):
        s = common_alignment(direction)

        if strategy == "TREND_FOLLOWING":
            if direction != trend:
                return -1.0
            if (direction == "UP" and not ema_bull) or (direction == "DOWN" and not ema_bear):
                return -1.0
            if (direction == "UP" and structure != "BULLISH") or (direction == "DOWN" and structure != "BEARISH"):
                return -1.0
            if (direction == "UP" and momentum <= 0) or (direction == "DOWN" and momentum >= 0):
                return -1.0
            if momentum_norm < 0.15:
                return -1.0
            s = 0.0
            s += 25
            s += 20 if ((direction == "UP" and structure == "BULLISH") or (direction == "DOWN" and structure == "BEARISH")) else 0
            s += 20 if ema_gap_norm >= 0.08 else 10
            s += 15 if momentum_norm >= 0.25 else 8
            s += 10 if trend_persistence >= 2 else 0
            s += 10 if aligned_recent(direction) >= 2 and body_ratio >= 0.35 else 0
            if (direction == "UP" and rr >= 74) or (direction == "DOWN" and rr <= 26):
                s -= 12
            return max(0.0, min(100.0, s))

        if strategy == "MOMENTUM":
            if momentum_norm < 0.30 or body_ratio < 0.40:
                return -1.0
            if (direction == "UP" and momentum <= 0) or (direction == "DOWN" and momentum >= 0):
                return -1.0
            s = 30
            s += 20 if body_ratio >= 0.55 else 10
            s += 15 if aligned_recent(direction) >= 2 else 0
            s += 12 if direction == trend else 0
            s += 10 if ((direction == "UP" and structure == "BULLISH") or (direction == "DOWN" and structure == "BEARISH")) else 0
            s += 10 if rsi_supports(direction) else 0
            s += 3 if momentum_norm >= 0.50 else 0
            return min(100.0, s)

        if strategy == "BREAKOUT":
            active = breakout_up if direction == "UP" else breakout_down
            distance = breakout_distance_up if direction == "UP" else breakout_distance_down
            prior_inside = prev <= resistance if direction == "UP" else prev >= support
            if not active or distance < 0.05 or body_ratio < 0.40:
                return -1.0
            s = 40
            s += 20 if distance >= 0.10 else 8
            s += 15 if body_ratio >= 0.55 else 6
            s += 10 if direction == trend else 0
            s += 10 if prior_inside else 0
            s += 5 if aligned_recent(direction) >= 2 else 0
            return min(100.0, s)

        if strategy == "PULLBACK":
            location = near_support if direction == "UP" else near_resistance
            rejection = bullish_rejection if direction == "UP" else bearish_rejection
            aligned = direction == trend and (
                (direction == "UP" and structure == "BULLISH") or
                (direction == "DOWN" and structure == "BEARISH")
            )
            if trend not in ("UP", "DOWN") or direction != trend or not location or not rejection:
                return -1.0
            s = 25
            s += 25
            s += 20 if rejection else 0
            s += 15 if aligned else 0
            s += 10 if momentum_norm >= 0.10 else 0
            s += 5 if body_ratio >= 0.30 else 0
            return min(100.0, s)

        if strategy == "REVERSAL":
            extreme = (rr < 32) if direction == "UP" else (rr > 68)
            rejection = bullish_rejection if direction == "UP" else bearish_rejection
            location = near_support if direction == "UP" else near_resistance
            if not extreme or not rejection:
                return -1.0
            s = 35 + 25
            s += 15 if trend in ("SIDEWAYS", direction) else 0
            s += 15 if location else 0
            s += 10 if momentum_norm < 0.35 else 0
            return min(100.0, s)

        if strategy == "MEAN_REVERSION":
            extreme = (rr < 28) if direction == "UP" else (rr > 72)
            location = near_support if direction == "UP" else near_resistance
            weak_momentum = momentum_norm < 0.35
            rejection = bullish_rejection if direction == "UP" else bearish_rejection
            if not extreme or not location or not weak_momentum:
                return -1.0
            s = 30 + 25 + 20
            s += 15 if rejection else 0
            s += 10 if trend == "SIDEWAYS" else 0
            return min(100.0, s)

        if strategy == "PRICE_ACTION":
            bullish = direction == "UP" and pattern in {"BULLISH_CANDLE", "BULLISH_REJECTION"}
            bearish = direction == "DOWN" and pattern in {"BEARISH_CANDLE", "BEARISH_REJECTION"}
            location = near_support if direction == "UP" else near_resistance
            if not (bullish or bearish) or (not location and body_ratio < 0.60):
                return -1.0
            s = 35
            s += 25 if location else 10
            s += 20 if ((direction == "UP" and structure == "BULLISH") or (direction == "DOWN" and structure == "BEARISH")) else 0
            s += 10 if direction == trend else 0
            s += 10 if body_ratio >= 0.60 else 0
            return min(100.0, s)

        if strategy == "VOLATILITY":
            if volatility_ratio < 1.15 or momentum_norm < 0.30 or body_ratio < 0.45:
                return -1.0
            if direction != trend and trend in ("UP", "DOWN"):
                return -1.0
            s = 30 + 25 + 20
            s += 15 if direction == trend else 0
            s += 10 if ((direction == "UP" and structure == "BULLISH") or (direction == "DOWN" and structure == "BEARISH")) else 0
            return min(100.0, s)

        return -1.0

    self_profile = discover_self_strategy(
        trend=trend,
        structure=structure,
        rsi_value=rr,
        momentum=momentum,
        atr_value=aa,
        volatility_ratio=volatility_ratio,
        breakout_up=breakout_up,
        breakout_down=breakout_down,
        near_support=near_support,
        near_resistance=near_resistance,
        pattern=pattern,
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
                sc = min(100.0, sc + min(8.0, float(self_profile.get("strength", 0.0))))
            if sc >= 70:
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

    best = max(valid, key=lambda x: x["score"])
    if best["score"] < 78:
        return None

    expected = best["direction"]
    five_minute_eligible = bool(
        best["strategy"] == "TREND_FOLLOWING"
        and expected == trend
        and ((expected == "UP" and structure == "BULLISH") or (expected == "DOWN" and structure == "BEARISH"))
        and ((expected == "UP" and ema_bull) or (expected == "DOWN" and ema_bear))
        and momentum_norm >= 0.35
        and ema_gap_norm >= 0.12
        and aligned_recent(expected) >= 2
        and body_ratio >= 0.55
        and not ((expected == "UP" and rr >= 74) or (expected == "DOWN" and rr <= 26))
        and trend_persistence >= 2
        and not ((expected == "UP" and near_resistance and not breakout_up) or (expected == "DOWN" and near_support and not breakout_down))
    )

    # 5m is now an exceptional-duration path. If its dedicated gate is not met,
    # the adaptive expiry layer may only choose a shorter local strategy expiry.
    best["five_minute_eligible"] = five_minute_eligible

    confidence = int(round(best["score"]))
    reason = (
        f"{best['strategy']} | 15m={trend} (persist={trend_persistence}) | "
        f"1m={structure} | RSI={rr:.1f} | momentum_norm={momentum_norm:.2f}ATR | "
        f"EMA_gap={ema_gap_norm:.2f}ATR | body={body_ratio:.2f} | "
        f"pattern={pattern} | volatility={volatility_ratio:.2f} | "
        f"support={support:.6g} | resistance={resistance:.6g}"
    )

    return {
        "pair": str(asset.get("pair", "")),
        "display_name": str(asset.get("display_name") or asset.get("title") or ""),
        "direction": expected,
        "confidence": max(0, min(99, confidence)),
        "strategy": best["strategy"],
        "expiry_minutes": best["expiry_minutes"],
        "five_minute_eligible": five_minute_eligible,
        "self_strategy": self_profile["strategy"],
        "self_strategy_strength": self_profile["strength"],
        "self_strategy_weights": self_profile.get("weights", {}),
        "self_strategy_margin": self_profile.get("margin", 0.0),
        "pattern": pattern,
        "trend_15m": trend,
        "structure_1m": structure,
        "market_quality": best["score"],
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
        "body_ratio": body_ratio,
        "volatility_ratio": volatility_ratio,
        "trend_persistence": trend_persistence,
        "evidence": {
            "trend": trend,
            "trend_persistence": trend_persistence,
            "structure": structure,
            "momentum_norm": momentum_norm,
            "volatility": volatility_ratio,
            "breakout_up": breakout_up,
            "breakout_down": breakout_down,
            "near_support": near_support,
            "near_resistance": near_resistance,
            "pattern": pattern,
            "ema_gap_norm": ema_gap_norm,
            "recent_aligned_candles": aligned_recent(expected),
        },
    }
