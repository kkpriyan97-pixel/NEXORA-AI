"""Candice Brain: regime-aware, evidence-first multi-strategy market analysis.
Read-only DEMO analysis. No trade execution and no credential handling.
"""
from __future__ import annotations

from math import isfinite
from self_strategy import discover as discover_self_strategy
from strategy_knowledge import strategy_live_eligible

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


def stochastic(cs, k_period=14, d_period=3, slowing=3):
    """Stochastic %K/%D using the chart settings shown by the user: 14/3/3."""
    if len(cs) < k_period + slowing + d_period:
        return 50.0, 50.0, "NEUTRAL"
    raw=[]
    for i in range(k_period, len(cs)+1):
        w=cs[i-k_period:i]
        hi=max(x["high"] for x in w)
        lo=min(x["low"] for x in w)
        den=max(hi-lo,1e-12)
        raw.append(100.0*(cs[i-1]["close"]-lo)/den)
    smooth=[]
    for i in range(slowing, len(raw)+1):
        smooth.append(sum(raw[i-slowing:i])/slowing)
    if not smooth:
        return 50.0,50.0,"NEUTRAL"
    k=smooth[-1]
    d=sum(smooth[-d_period:])/min(d_period,len(smooth))
    prev_k=smooth[-2] if len(smooth)>=2 else k
    prev_d=(sum(smooth[-d_period-1:-1])/min(d_period,len(smooth)-1)
            if len(smooth)>=2 else d)
    cross="BULLISH_CROSS" if prev_k<=prev_d and k>d else "BEARISH_CROSS" if prev_k>=prev_d and k<d else "NEUTRAL"
    return k,d,cross



def bollinger(cs, n=30, mult=2.2):
    """Bollinger Bands matching the user's history AI settings: 30, 2.2."""
    if len(cs) < n:
        return {"middle":0.0,"upper":0.0,"lower":0.0,"signal":"UNKNOWN","position":"INSUFFICIENT","width_norm":0.0}
    closes=[x["close"] for x in cs[-n:]]
    middle=sum(closes)/n
    variance=sum((x-middle)**2 for x in closes)/n
    sd=variance**0.5
    upper=middle+mult*sd
    lower=middle-mult*sd
    last=closes[-1]
    width=max(upper-lower,0.0)
    if last>upper:
        signal="UP"; position="ABOVE_UPPER"
    elif last<lower:
        signal="DOWN"; position="BELOW_LOWER"
    elif last>=middle:
        signal="UP"; position="ABOVE_MIDDLE"
    else:
        signal="DOWN"; position="BELOW_MIDDLE"
    return {
        "middle":middle,"upper":upper,"lower":lower,
        "signal":signal,"position":position,
        "width_norm":width/max(abs(last),1e-12)
    }

def donchian(cs, period=30):
    """Donchian Channel 30: prior-channel breakout plus channel-width regime."""
    if len(cs) < period + 2:
        return {"upper":0.0,"lower":0.0,"middle":0.0,"width_norm":0.0,
                "state":"INSUFFICIENT","expansion":False,"breakout_up":False,"breakout_down":False}
    prior=cs[-period-1:-1]
    upper=max(x["high"] for x in prior)
    lower=min(x["low"] for x in prior)
    middle=(upper+lower)/2.0
    last=cs[-1]
    width=max(upper-lower,0.0)
    prev_prior=cs[-period-2:-2]
    prev_upper=max(x["high"] for x in prev_prior)
    prev_lower=min(x["low"] for x in prev_prior)
    prev_width=max(prev_upper-prev_lower,0.0)
    expansion=width>prev_width*1.03
    state="BREAKOUT_UP" if last["close"]>upper else "BREAKOUT_DOWN" if last["close"]<lower else "INSIDE"
    return {
        "upper":upper,"lower":lower,"middle":middle,
        "width_norm":width/max(abs(last["close"]),1e-12),
        "state":state,"expansion":expansion,
        "breakout_up":state=="BREAKOUT_UP","breakout_down":state=="BREAKOUT_DOWN",
    }


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


def analyze_asset(asset, candles, price=None, forced_strategy=None, learning_campaign=False):
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

    dc = donchian(cs,30)
    bb = bollinger(cs,30,2.2)
    stoch_k, stoch_d, stoch_cross = stochastic(cs,14,3,3)
    dc_breakout_up = bool(dc["breakout_up"] and body > 0)
    dc_breakout_down = bool(dc["breakout_down"] and body < 0)
    breakout_up = bool((p > resistance and body > 0) or dc_breakout_up)
    breakout_down = bool((p < support and body < 0) or dc_breakout_down)
    breakout_distance_up = max((p - resistance) / max(aa, 1e-12),
                               (p - dc["upper"]) / max(aa, 1e-12))
    breakout_distance_down = max((support - p) / max(aa, 1e-12),
                                 (dc["lower"] - p) / max(aa, 1e-12))
    stoch_bull = stoch_k > stoch_d and stoch_k >= 50
    stoch_bear = stoch_k < stoch_d and stoch_k <= 50
    stoch_oversold = stoch_k <= 20
    stoch_overbought = stoch_k >= 80

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
            # Strict breakout confirmation:
            # 1) real price displacement must be meaningful (>= 0.15 ATR)
            # 2) the breakout candle must have a strong body (>= 0.55)
            # 3) momentum/efficiency must show continuation potential
            # 4) either the 30-period Donchian itself broke in the same direction,
            #    or the 20-bar level was cleared decisively.
            dc_confirm = (
                (direction == "UP" and dc_breakout_up) or
                (direction == "DOWN" and dc_breakout_down)
            )
            level_confirm = bool(distance >= 0.15 and body_ratio >= 0.55)
            continuation_confirm = bool(momentum_norm >= 0.30 and _efficiency(v, 8) >= 0.35)
            stoch_confirm = (direction == "UP" and stoch_bull) or (direction == "DOWN" and stoch_bear)
            # A breakout in a confirmed SIDEWAYS 15m regime is too unstable
            # for a 1m directional signal. Require a directional higher-timeframe
            # regime before allowing BREAKOUT into the live candidate pool.
            if trend not in {"UP","DOWN"}:
                return -1.0
            # Tighten the live 1-minute breakout definition. A 0.15 ATR / 0.55
            # body break can fail immediately; require stronger displacement,
            # continuation and oscillator agreement without changing direction.
            if (
                not active
                or distance < 0.20
                or body_ratio < 0.60
                or not prior_inside
                or not (dc_confirm or level_confirm)
                or momentum_norm < 0.35
                or _efficiency(v, 8) < 0.40
                or not stoch_confirm
                or trend_persistence < 2
            ):
                return -1.0
            s = 62.0
            s += 10 if distance >= 0.20 else 6
            s += 8 if body_ratio >= 0.65 else 4
            s += 7 if trend_ok else 0
            s += 6 if slope_ok else 0
            s += 5 if aligned_recent(direction) >= 2 else 0
            s += 4 if structure_ok else 0
            s += 4 if stoch_confirm else 0
            s += 4 if dc["expansion"] else 0
            s += 4 if _efficiency(v, 8) >= 0.50 else 0
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
            stoch_reversal = (direction == "UP" and (stoch_oversold or stoch_cross == "BULLISH_CROSS")) or (direction == "DOWN" and (stoch_overbought or stoch_cross == "BEARISH_CROSS"))
            if not extreme or not rejection or not location or not stoch_reversal:
                return -1.0
            s = 62.0
            s += 10 if trend_weak else 3
            s += 8 if structure_quality[direction] >= 0.4 else 0
            s += 8 if momentum_norm < 0.35 else 0
            s += 6 if body_ratio >= 0.30 else 0
            s += 5 if slope_ok else 0
            s += 5 if ((direction == "UP" and stoch_bull) or (direction == "DOWN" and stoch_bear)) else 0
            return min(94.0, s)

        if strategy == "MEAN_REVERSION":
            extreme = (rr < 27) if direction == "UP" else (rr > 73)
            location = near_support if direction == "UP" else near_resistance
            weak_momentum = momentum_norm < 0.35
            rejection = bullish_rejection if direction == "UP" else bearish_rejection
            stoch_mean = (direction == "UP" and stoch_oversold) or (direction == "DOWN" and stoch_overbought)
            if not extreme or not location or not weak_momentum or not stoch_mean:
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
            rejection = bullish_rejection if direction == "UP" else bearish_rejection
            # A single directional candle away from a level is not sufficient for
            # a 1-minute expiry. Require structural agreement and a meaningful
            # location/rejection so the raw confidence cannot be inflated by one bar.
            if (
                not trend_ok
                or not structure_ok
                or structure_quality[direction] < 0.50
                or aligned_recent(direction) < 2
                or momentum_norm < 0.10
                or not (location or rejection)
            ):
                return -1.0
            s = 58.0
            s += 12 if location else 8
            s += 8 if structure_ok else 0
            s += 7 if trend_ok else 0
            s += 7 if body_ratio >= 0.60 else 2
            s += 5 if slope_ok else 0
            s += 5 if aligned_recent(direction) >= 2 else 0
            s += 3 if structure_quality[direction] >= 0.70 else 0
            return min(96.0, s)

        if strategy == "MOMENTUM":
            # Momentum is a continuation technique in this 1m system. A SIDEWAYS
            # 15m regime is excluded completely to avoid single-candle range noise.
            if trend == "SIDEWAYS":
                return -1.0
            if momentum_norm < 0.30 or body_ratio < 0.45:
                return -1.0
            if (direction == "UP" and momentum <= 0) or (direction == "DOWN" and momentum >= 0):
                return -1.0
            if (direction == "UP" and rr >= 73) or (direction == "DOWN" and rr <= 27):
                return -1.0
            if (direction == "UP" and not stoch_bull) or (direction == "DOWN" and not stoch_bear):
                return -1.0
            s = 60.0
            s += 10 if momentum_norm >= 0.50 else 5
            s += 8 if body_ratio >= 0.60 else 3
            s += 7 if aligned_recent(direction) >= 2 else 0
            s += 6 if trend_ok else 0
            s += 5 if structure_ok else 0
            s += 4 if slope_ok else 0
            s += 5 if ((direction == "UP" and stoch_bull) or (direction == "DOWN" and stoch_bear)) else 0
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
            s += 4 if dc["expansion"] else 0
            s += 4 if ((direction == "UP" and stoch_bull) or (direction == "DOWN" and stoch_bear)) else 0
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
            if (direction == "UP" and not stoch_bull) or (direction == "DOWN" and not stoch_bear):
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
            s += 5 if ((direction == "UP" and stoch_bull) or (direction == "DOWN" and stoch_bear)) else 0
            s += 4 if dc["expansion"] else 0

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
        donchian_state=dc["state"],
        donchian_expansion=dc["expansion"],
        stochastic_k=stoch_k,
        stochastic_d=stoch_d,
        stochastic_cross=stoch_cross,
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

    forced = str(forced_strategy or "").upper().strip()
    if forced and forced not in strategies:
        return None
    # Only a completed 100-trade DEMO failure at the explicit 85% gate can
    # remove a strategy family from live Brain routing. The isolated learning
    # campaign must still be able to re-test its assigned strategy; otherwise
    # a previously rejected family can deadlock its own 100-trade re-validation.
    # learning_campaign=True is used only by the overnight DEMO lab and never by
    # the daytime/manual signal path.
    if forced:
        active_strategies = (
            (forced,)
            if (learning_campaign or strategy_live_eligible(forced))
            else ()
        )
    else:
        active_strategies = tuple(s for s in strategies if strategy_live_eligible(s))
    if not active_strategies:
        return None

    candidates = []
    for strategy in active_strategies:
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
    # Do not collapse a market setup to one technique too early. Keep every
    # independently qualified strategy/direction variant (score >= 82) so the
    # final delivery layer can fall through to the next technique when the
    # first technique fails a late 2m/1m/live-price gate.
    qualified_variants = [x for x in ranked if x["score"] >= 82]
    if not qualified_variants:
        return None

    best = qualified_variants[0]
    second_score = ranked[1]["score"] if len(ranked) > 1 else 0.0
    strategy_margin = max(0.0, best["score"] - second_score)
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
        and ((expected == "UP" and stoch_bull) or (expected == "DOWN" and stoch_bear))
        and dc["expansion"]
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
        f"Donchian30={dc['state']}/{('EXPANDING' if dc['expansion'] else 'FLAT')} | "
        f"Stoch14,3,3={stoch_k:.1f}/{stoch_d:.1f}/{stoch_cross} | "
        f"volatility={volatility_ratio:.2f} | support={support:.6g} | resistance={resistance:.6g}"
    )

    return {
        "pair": str(asset.get("pair", "")),
        "display_name": str(asset.get("display_name") or asset.get("title") or ""),
        "direction": expected,
        "confidence": confidence,
        "strategy": best["strategy"],
        "expiry_minutes": best["expiry_minutes"],
        "strategy_candidates": [
            {
                "strategy": str(v.get("strategy") or ""),
                "direction": str(v.get("direction") or "").upper(),
                "score": int(round(v.get("score") or 0)),
                "expiry_minutes": int(v.get("expiry_minutes") or 3),
                "self_strategy_version": str(v.get("self_strategy_version") or ""),
            }
            for v in qualified_variants
        ],
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
        "breakout_distance_up": round(breakout_distance_up, 4),
        "breakout_distance_down": round(breakout_distance_down, 4),
        "breakout_confirmed": bool(
            (expected == "UP" and breakout_up and breakout_distance_up >= 0.15)
            or (expected == "DOWN" and breakout_down and breakout_distance_down >= 0.15)
        ),
        "ema_gap_norm": ema_gap_norm,
        "ema_slope_norm": ema_slope_norm,
        "body_ratio": body_ratio,
        "volatility_ratio": volatility_ratio,
        "trend_persistence": trend_persistence,
        "structure_quality": structure_quality[expected],
        "efficiency": _efficiency(v, 8),
        "indicators": {
            "bollinger_period": 30,
            "bollinger_stddev": 2.2,
            "bollinger_signal": bb["signal"],
            "bollinger_position": bb["position"],
            "bollinger_middle": bb["middle"],
            "bollinger_upper": bb["upper"],
            "bollinger_lower": bb["lower"],
            "bollinger_width_norm": bb["width_norm"],
            "donchian_period": 30,
            "bollinger_signal": bb["signal"],
            "bollinger_position": bb["position"],
            "donchian_state": dc["state"],
            "donchian_expansion": dc["expansion"],
            "donchian_upper": dc["upper"],
            "donchian_lower": dc["lower"],
            "donchian_middle": dc["middle"],
            "stochastic_k": round(stoch_k,2),
            "stochastic_d": round(stoch_d,2),
            "stochastic_cross": stoch_cross,
            "stochastic_oversold": stoch_oversold,
            "stochastic_overbought": stoch_overbought,
        },
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
            "breakout_distance": breakout_distance_up if expected == "UP" else breakout_distance_down,
            "breakout_confirmed": bool(
                (expected == "UP" and breakout_up and breakout_distance_up >= 0.15)
                or (expected == "DOWN" and breakout_down and breakout_distance_down >= 0.15)
            ),
            "near_support": near_support,
            "near_resistance": near_resistance,
            "pattern": pattern,
            "recent_aligned_candles": aligned_recent(expected),
            "donchian_state": dc["state"],
            "donchian_expansion": dc["expansion"],
            "stochastic_k": stoch_k,
            "stochastic_d": stoch_d,
            "stochastic_cross": stoch_cross,
        },
    }
