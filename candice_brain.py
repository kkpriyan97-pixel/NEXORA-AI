"""Candice live technical brain.

Live signal direction is generated ONLY from:
1) Anchored VWAP
2) Volume Profile (POC / VAH / VAL)

The production scheduler, Telegram delivery and DEMO result watcher live in
app.py. This module intentionally contains no legacy EMA/RSI/MACD/Donchian/
Bollinger/Stochastic strategy families.
"""
from __future__ import annotations

import logging
import time
from math import isfinite

EXPIRIES=(1,)
MIN_CLOSED_CANDLES=60
ONE_MINUTE=60
FIFTEEN_MINUTES=900
CLOSE_GRACE_SECONDS=1
PROFILE_LOOKBACK=60
PROFILE_BINS=24
VALUE_AREA_FRACTION=0.70
ALLOWED_STRATEGY="AVWAP_VOLUME_PROFILE"
log=logging.getLogger("candice.brain")
_DIAG_LAST={}

def _diag(pair, reason, **fields):
    now=time.time()
    key=(str(pair),str(reason))
    last=_DIAG_LAST.get(key,0.0)
    if now-last<60.0:
        return
    _DIAG_LAST[key]=now
    log.info("AVWAP_VP_DIAGNOSTIC pair=%s reason=%s fields=%s",pair,reason,fields)



def _f(value, default=0.0):
    try:
        x=float(value)
        return x if isfinite(x) else default
    except (TypeError, ValueError):
        return default


def _ts(value, default=-1.0):
    try:
        x=float(value)
        if x>20_000_000_000:
            x/=1000.0
        return x if isfinite(x) else default
    except (TypeError, ValueError):
        return default


def _norm(raw):
    return {
        "time":_ts(raw.get("time",raw.get("t"))),
        "open":_f(raw.get("open",raw.get("o"))),
        "high":_f(raw.get("high",raw.get("h"))),
        "low":_f(raw.get("low",raw.get("l"))),
        "close":_f(raw.get("close",raw.get("c"))),
        "volume":max(_f(raw.get("volume",raw.get("v")),1.0),1.0),
    }


def _closed_1m(candles,now=None):
    now=time.time() if now is None else float(now)
    latest={}
    for raw in candles or []:
        if not isinstance(raw,dict):
            continue
        c=_norm(raw)
        if c["time"]<0:
            continue
        minute=int(c["time"]//ONE_MINUTE)*ONE_MINUTE
        # Never use a currently forming candle.
        if minute+ONE_MINUTE>now-CLOSE_GRACE_SECONDS:
            continue
        c["time"]=minute
        latest[minute]=c
    return [latest[k] for k in sorted(latest)]


def _complete_15m_blocks(cs,now=None):
    now=time.time() if now is None else float(now)
    groups={}
    for c in cs:
        minute=int(c["time"])
        bucket=(minute//FIFTEEN_MINUTES)*FIFTEEN_MINUTES
        if bucket+FIFTEEN_MINUTES>now-CLOSE_GRACE_SECONDS:
            continue
        groups.setdefault(bucket,[]).append(c)

    blocks=[]
    for bucket,bars in sorted(groups.items()):
        bars=sorted(bars,key=lambda x:x["time"])
        expected=[bucket+i*ONE_MINUTE for i in range(15)]
        if [int(x["time"]) for x in bars]!=expected:
            continue
        blocks.append({
            "time":bucket,
            "open":bars[0]["open"],
            "high":max(x["high"] for x in bars),
            "low":min(x["low"] for x in bars),
            "close":bars[-1]["close"],
            "volume":sum(x["volume"] for x in bars),
        })
    return blocks


def _anchored_vwap(cs,anchor_ts):
    sample=[c for c in cs if int(c["time"])>=int(anchor_ts)]
    if not sample:
        return 0.0,0.0
    pv=0.0
    vv=0.0
    for c in sample:
        typical=(c["high"]+c["low"]+c["close"])/3.0
        vol=max(c["volume"],1.0)
        pv+=typical*vol
        vv+=vol
    return (pv/vv if vv else 0.0),vv


def _volume_profile(cs,bins=PROFILE_BINS):
    sample=list(cs[-PROFILE_LOOKBACK:])
    if not sample:
        return None
    low=min(c["low"] for c in sample)
    high=max(c["high"] for c in sample)
    total=sum(max(c["volume"],1.0) for c in sample)

    if high<=low:
        px=sample[-1]["close"]
        return {
            "poc":px,"vah":px,"val":px,
            "range_high":high,"range_low":low,
            "total_volume":total,"bins":1,
        }

    step=(high-low)/float(bins)
    volumes=[0.0]*bins
    for c in sample:
        typical=(c["high"]+c["low"]+c["close"])/3.0
        idx=int((typical-low)/step)
        idx=max(0,min(bins-1,idx))
        volumes[idx]+=max(c["volume"],1.0)

    poc_idx=max(range(bins),key=lambda i:volumes[i])
    target=sum(volumes)*VALUE_AREA_FRACTION
    accumulated=volumes[poc_idx]
    left=right=poc_idx

    while accumulated<target and (left>0 or right<bins-1):
        lv=volumes[left-1] if left>0 else -1.0
        rv=volumes[right+1] if right<bins-1 else -1.0
        if rv>=lv:
            right+=1
            accumulated+=volumes[right]
        else:
            left-=1
            accumulated+=volumes[left]

    return {
        "poc":low+(poc_idx+0.5)*step,
        "vah":min(high,low+(right+1)*step),
        "val":max(low,low+left*step),
        "range_high":high,
        "range_low":low,
        "total_volume":sum(volumes),
        "bins":bins,
    }


def _slope(cs,anchor_ts):
    if len(cs)<2:
        return 0.0
    current,_=_anchored_vwap(cs,anchor_ts)
    previous,_=_anchored_vwap(cs[:-1],anchor_ts)
    return current-previous


def analyze_asset(
    asset,
    candles,
    price=None,
    forced_strategy=None,
    learning_campaign=False,
):
    """Return one deterministic AVWAP + Volume Profile candidate or None.

    `forced_strategy` and `learning_campaign` remain accepted for API
    compatibility with the overnight learning lab. Live direction is never
    delegated to those legacy strategy names.
    """
    forced=str(forced_strategy or "").upper().strip()
    if forced and forced!=ALLOWED_STRATEGY:
        return None

    now=time.time()
    pair=str(asset.get("pair") or "")
    cs=_closed_1m(candles,now)
    if len(cs)<MIN_CLOSED_CANDLES:
        _diag(pair,"history_short",closed=len(cs),required=MIN_CLOSED_CANDLES)
        return None

    blocks=_complete_15m_blocks(cs,now)
    if not blocks:
        _diag(pair,"no_complete_15m_block",closed=len(cs),unique_minutes=len({int(x["time"]) for x in cs}))
        return None

    last=cs[-1]
    anchor_ts=int(blocks[-1]["time"])
    avwap,avwap_volume=_anchored_vwap(cs,anchor_ts)
    previous_avwap,_=_anchored_vwap(cs[:-1],anchor_ts)
    profile=_volume_profile(cs)
    if avwap<=0 or not profile:
        _diag(pair,"invalid_avwap_or_profile",avwap=avwap,profile=bool(profile))
        return None

    px=float(last["close"])
    poc=float(profile["poc"])
    vah=float(profile["vah"])
    val=float(profile["val"])
    slope=avwap-previous_avwap

    up=px>avwap and px>poc
    down=px<avwap and px<poc
    if not (up or down):
        _diag(pair,"no_two_indicator_alignment",price=round(px,10),avwap=round(avwap,10),poc=round(poc,10),vah=round(vah,10),val=round(val,10))
        return None

    direction="UP" if up else "DOWN"
    slope_aligned=(slope>0) if direction=="UP" else (slope<0)
    value_acceptance=(px>=vah) if direction=="UP" else (px<=val)

    # This is a rule-quality score, NOT a future win probability.
    score=90
    if slope_aligned:
        score+=5
    if value_acceptance:
        score+=4
    confidence=min(99,score)

    trend="AVWAP_BULLISH" if up else "AVWAP_BEARISH"
    structure="ABOVE_AVWAP_POC" if up else "BELOW_AVWAP_POC"

    features={
        "anchored_vwap":avwap,
        "volume_profile_poc":poc,
        "volume_profile_vah":vah,
        "volume_profile_val":val,
        "avwap_slope":slope,
        "profile_range_high":profile["range_high"],
        "profile_range_low":profile["range_low"],
        "profile_total_volume":profile["total_volume"],
        "profile_bins":profile["bins"],
        "anchor_15m_start_ts":anchor_ts,
        "avwap_volume":avwap_volume,
        "slope_aligned":slope_aligned,
        "value_area_acceptance":value_acceptance,
    }

    result={
        "pair":str(asset.get("pair") or ""),
        "display_name":str(
            asset.get("display_name")
            or asset.get("title")
            or asset.get("name")
            or asset.get("pair")
            or ""
        ),
        "direction":direction,
        "confidence":confidence,
        "strategy":ALLOWED_STRATEGY,
        "expiry_minutes":1,
        "pattern":"AVWAP_VOLUME_PROFILE_ALIGNMENT",
        "trend_15m":trend,
        "structure_1m":structure,
        "market_quality":float(score),
        "confluence_score":float(score),
        "direction_agreement":1.0,
        "value_area_acceptance":value_acceptance,
        "reason":(
            f"Closed M1 price={px:.8f}; AVWAP={avwap:.8f}; POC={poc:.8f}; "
            f"VAH={vah:.8f}; VAL={val:.8f}; AVWAP_slope={slope:.8f}; "
            f"AVWAP+POC aligned; closed 1M + completed 15M anchor only."
        ),
        "indicator_features":features,
        "indicators":features,
        "avwap":avwap,
        "poc":poc,
        "vah":vah,
        "val":val,
        "avwap_slope":slope,
        "entry_candle_ts":last["time"],
        "closed_1m_ts":last["time"],
        "closed_15m_ts":anchor_ts,
        "decision_candle_closed":True,
        "price":px,
        "five_minute_eligible":False,
        "self_strategy_version":"AVWAP_VP_V1",
        "strategy_candidates":[{
            "strategy":ALLOWED_STRATEGY,
            "direction":direction,
            "score":confidence,
            "expiry_minutes":1,
            "self_strategy_version":"AVWAP_VP_V1",
        }],
        "strategy_audit":[{
            "strategy":ALLOWED_STRATEGY,
            "active_for_live":True,
            "up_qualified":bool(up),
            "down_qualified":bool(down),
        }],
        "strategy_audit_count":1,
        "indicator_audit_scope":"AVWAP_VOLUME_PROFILE_ONLY",
    }
    return result
