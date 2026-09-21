"""Evidence-led research layer for Candice self-learning.

This module converts multilingual web/academic research into bounded, testable
priors. It never declares a strategy "guaranteed"; local authenticated outcomes
must validate a hypothesis before the learning layer can materially influence
ranking.
"""

from __future__ import annotations

RESEARCH_VERSION = "WEB-RESEARCH-V1"
TARGET_DAYS = 15

# Research was reviewed across academic/quant sources and multilingual trading
# education. Retail claims are stored as hypotheses, not as ground truth.
SOURCE_CATALOG = (
    {"lang":"EN","type":"academic","topic":"candlestick_intraday","source":"SSRN 2125889","finding":"candlestick rules can show weak intraday predictability, but transaction costs can erase it"},
    {"lang":"EN","type":"academic","topic":"short_horizon_momentum_reversal","source":"Journal of Empirical Finance 72","finding":"intraday momentum/reversal effects can coexist; realized semivariance can help identify reversals"},
    {"lang":"EN","type":"academic","topic":"one_minute_reversal","source":"Quarterly Review of Economics and Finance 81","finding":"extreme one-minute moves can partially reverse in the following minute in a liquid-stock sample"},
    {"lang":"EN","type":"academic","topic":"overfitting","source":"SSRN 3177057","finding":"multiple testing can create false discoveries; validation must account for search breadth"},
    {"lang":"EN","type":"methodology","topic":"walk_forward","source":"ML4Trading Cross-Validation","finding":"chronological/purged walk-forward validation reduces leakage around forward labels"},
    {"lang":"EN","type":"2026_replication","topic":"one_minute_momentum","source":"SSRN 7290621 / 7323419","finding":"a strategy can reproduce in-sample results and deteriorate materially out of sample"},
    {"lang":"EN","type":"2026_negative","topic":"one_minute_price_only","source":"Capo Horn ES 1m study","finding":"price-only 1-minute signals can be dominated by noise; negative evidence is useful for rejection rules"},
    {"lang":"ZH","type":"research","topic":"minute_factor_mining","source":"华泰金工 / 新浪财经 2026-04-01","finding":"minute-level factors benefit from fixed, interpretable formulas, temporal slicing/masking and anti-overfit controls"},
    {"lang":"ZH","type":"education","topic":"one_minute_volume","source":"财云财经 2026-08-25","finding":"volume expansion with directional price movement is commonly used as a confirmation hypothesis"},
    {"lang":"JA","type":"education","topic":"one_minute_scalping","source":"FXコツ 2026-03-28","finding":"higher-timeframe confirmation plus completed 1-minute patterns was reported as more stable than 1-minute-only entries; retail result, not universal evidence"},
    {"lang":"ES","type":"education","topic":"scalping_structure","source":"Whale Analytics / FX education 2026","finding":"5m/15m structure, 1m trigger, key levels and volume/session filters are common discretionary components; treat as hypotheses"},
    {"lang":"AR","type":"gateway","topic":"multilingual_forex","source":"FXStreet Arabic edition","finding":"multilingual market education exists; Arabic material is mined as hypothesis input, not automatically trusted"},
    {"lang":"RU","type":"gateway","topic":"multilingual_forex","source":"FXStreet Russian edition","finding":"Russian-language market material is available for hypothesis discovery"},
    {"lang":"TR","type":"gateway","topic":"multilingual_forex","source":"FXStreet Turkish edition","finding":"Turkish-language market material is available for hypothesis discovery"},
    {"lang":"VI","type":"gateway","topic":"multilingual_forex","source":"FXStreet Vietnamese edition","finding":"Vietnamese-language market material is available for hypothesis discovery"},
)

CURRICULUM = (
    ("microstructure_and_noise","1m noise, spread, slippage, execution lag"),
    ("higher_timeframe_regime","15m/5m regime direction and trend persistence"),
    ("candle_geometry","body/range, wick/rejection, consecutive closes"),
    ("breakout_structure","compression, range expansion, breakout distance"),
    ("volume_confirmation","volume availability, relative volume and volume failure modes"),
    ("momentum_vs_reversal","trend continuation versus post-extreme reversal"),
    ("volatility_regimes","low/normal/high volatility and regime switching"),
    ("session_and_time_of_day","session liquidity and clock-bound behavior"),
    ("execution_freshness","fresh quote, stale candle, timing/latency controls"),
    ("multi_frame_agreement","short-frame and higher-frame agreement without overcounting"),
    ("asset_specificity","pair/asset-specific behavior; no blind transfer"),
    ("otc_and_feed_quality","OTC/feed-specific caveats and missing-volume handling"),
    ("feature_interactions","small interpretable feature combinations instead of indicator soup"),
    ("walk_forward_and_overfit","chronological holdouts, multiple-testing and challenger rejection"),
    ("freeze_and_promote","champion/challenger rules, drift detection and promotion only after validation"),
)

def research_day_from_elapsed(elapsed_days: float) -> int:
    try:
        d = int(max(0.0, float(elapsed_days)))
    except Exception:
        d = 0
    return min(TARGET_DAYS, d + 1)

def curriculum_item(day: int):
    idx = min(max(int(day) - 1, 0), len(CURRICULUM) - 1)
    topic, focus = CURRICULUM[idx]
    return {
        "day": idx + 1,
        "topic": topic,
        "focus": focus,
        "target_days": TARGET_DAYS,
        "completed": idx + 1 >= TARGET_DAYS,
    }

def research_status(start_ts: float | None, now_ts: float):
    if start_ts is None:
        return {"version":RESEARCH_VERSION,"target_days":TARGET_DAYS,"day":1,"elapsed_days":0.0,**curriculum_item(1)}
    elapsed=max(0.0,(float(now_ts)-float(start_ts))/86400.0)
    day=research_day_from_elapsed(elapsed)
    item=curriculum_item(day)
    return {
        "version":RESEARCH_VERSION,
        "target_days":TARGET_DAYS,
        "day":day,
        "elapsed_days":round(elapsed,2),
        **item,
    }

def strategy_research_prior(
    *,
    trend: str,
    trend_persistence: int,
    momentum_norm: float,
    body_ratio: float,
    efficiency: float,
    breakout: bool,
    donchian_expansion: bool,
    near_support: bool,
    near_resistance: bool,
    pattern: str,
    volatility_ratio: float,
):
    """Return small, bounded priors. They are hypotheses, not guarantees."""
    t=str(trend or "SIDEWAYS").upper()
    tp=int(trend_persistence or 0)
    mom=float(momentum_norm or 0.0)
    br=float(body_ratio or 0.0)
    eff=float(efficiency or 0.0)
    vr=float(volatility_ratio or 1.0)
    pa=str(pattern or "NEUTRAL").upper()

    out={
        "TREND_FOLLOWING":0.0,
        "MOMENTUM":0.0,
        "BREAKOUT":0.0,
        "PULLBACK":0.0,
        "REVERSAL":0.0,
        "MEAN_REVERSION":0.0,
        "PRICE_ACTION":0.0,
        "VOLATILITY":0.0,
    }

    # Research-consistent positive evidence: multi-frame/regime alignment,
    # strong candle geometry and efficient directional movement.
    if t in {"UP","DOWN"} and tp >= 2:
        out["TREND_FOLLOWING"] += 2.5
        if eff >= 0.45: out["TREND_FOLLOWING"] += 1.5
        if mom >= 0.35 and br >= 0.50: out["MOMENTUM"] += 1.5

    if breakout and donchian_expansion and br >= 0.55:
        out["BREAKOUT"] += 3.0

    if t in {"UP","DOWN"} and (near_support or near_resistance):
        if pa in {"BULLISH_REJECTION","BEARISH_REJECTION"}:
            out["PULLBACK"] += 1.5
            out["PRICE_ACTION"] += 1.0

    # Extreme + low momentum is treated as a reversal hypothesis, especially
    # in non-trending regimes. This is deliberately smaller than breakout/trend
    # priors because one-minute reversal evidence is highly market-dependent.
    if t == "SIDEWAYS" and vr >= 0.85 and mom < 0.35:
        out["MEAN_REVERSION"] += 2.0
        out["REVERSAL"] += 1.5

    # High volatility without a strong body is a caution signal rather than a
    # reason to prefer a volatility strategy.
    if vr >= 1.30 and br >= 0.55 and mom >= 0.35:
        out["VOLATILITY"] += 1.0
    elif vr >= 1.50 and br < 0.40:
        for k in out:
            out[k] -= 1.0

    return {k:round(max(-2.0,min(3.0,v)),2) for k,v in out.items()}
