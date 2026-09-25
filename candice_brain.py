"""Candice live technical brain.

Live signal direction is generated ONLY from:
1) Anchored VWAP
2) Volume Profile (POC / VAH / VAL)
3) Bill Williams Alligator confirmation (13/8, 8/5, 5/3)

The production scheduler, Telegram delivery and DEMO result watcher live in
app.py. This module intentionally contains no legacy EMA/RSI/MACD/Donchian/
Bollinger/Stochastic strategy families.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from math import isfinite

EXPIRIES=(1,)
MIN_CLOSED_CANDLES=60
ONE_MINUTE=60
FIFTEEN_MINUTES=900
CLOSE_GRACE_SECONDS=1
PROFILE_LOOKBACK=60
PROFILE_BINS=24
VALUE_AREA_FRACTION=0.70
HIGH_VOLUME_MIN_COVERAGE=0.80
ALLOWED_STRATEGY="AVWAP_VOLUME_PROFILE"

# Bill Williams Alligator confirmation settings, matching the terminal:
# Jaw 13 / shift 8, Teeth 8 / shift 5, Lips 5 / shift 3.
# The Alligator never creates or flips direction; it only confirms the
# authoritative AVWAP + Volume Profile direction.
ALLIGATOR_JAW_PERIOD=13
ALLIGATOR_JAW_SHIFT=8
ALLIGATOR_TEETH_PERIOD=8
ALLIGATOR_TEETH_SHIFT=5
ALLIGATOR_LIPS_PERIOD=5
ALLIGATOR_LIPS_SHIFT=3
ALLIGATOR_MIN_SEPARATION=0.0
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


def _extract_volume(raw):
    """Extract broker-supplied real/tick volume without confusing price with volume."""
    if not isinstance(raw,dict):
        return 0.0,"NONE"

    # Explicit real/traded-volume field names first.
    for key in (
        "real_volume","realVolume","trade_volume","tradeVolume",
        "traded_volume","tradedVolume","base_volume","baseVolume",
        "quote_volume","quoteVolume","volume"
    ):
        if key in raw:
            value=_f(raw.get(key),0.0)
            if value>0.0:
                return value,"REAL_VOLUME"

    # Common tick-volume field names. Tick volume is useful, but is not the
    # same thing as traded/notional volume and therefore is not marked real_volume_verified.
    for key in (
        "tick_volume","tickVolume","ticks","tick_count","tickCount",
        "vol","v"
    ):
        if key in raw:
            value=_f(raw.get(key),0.0)
            if value>0.0:
                return value,"TICK_VOLUME"

    # A few broker/API variants nest market statistics under a dict.
    for parent_key in ("data","stats","metrics","meta"):
        nested=raw.get(parent_key)
        if isinstance(nested,dict):
            value,source=_extract_volume(nested)
            if value>0.0:
                return value,source

    return 0.0,"NONE"


def _norm(raw):
    explicit_source=str(raw.get("volume_source") or "").upper()
    if explicit_source=="TICK_ACTIVITY":
        tick_value=0.0
        for key in ("tick_volume","tickVolume","ticks","tick_count","tickCount"):
            tick_value=_f(raw.get(key),0.0)
            if tick_value>0.0:
                volume_source="TICK_ACTIVITY"
                volume=tick_value
                break
        else:
            volume,volume_source=_extract_volume(raw)
    else:
        volume,volume_source=_extract_volume(raw)
    return {
        "time":_ts(raw.get("time",raw.get("t"))),
        "open":_f(raw.get("open",raw.get("o"))),
        "high":_f(raw.get("high",raw.get("h"))),
        "low":_f(raw.get("low",raw.get("l"))),
        "close":_f(raw.get("close",raw.get("c"))),
        "volume":max(volume,0.0),
        "volume_present":bool(volume>0.0),
        "volume_source":volume_source,
        # Explicitly distinguish locally enriched tick activity from broker
        # reported tick volume. Partial sampled minutes are never treated as
        # complete volume observations downstream.
        "tick_activity_proxy":str(raw.get("volume_source") or "").upper()=="TICK_ACTIVITY",
        "tick_activity_observed_seconds":_f(raw.get("tick_activity_observed_seconds"),0.0),
        "tick_activity_complete":bool(raw.get("tick_activity_complete")),
        "tick_activity_source":str(raw.get("tick_activity_source") or "").upper(),
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
    raw_volumes=[]
    volume_sources=[]
    tick_activity_partial_bars=0
    for c in sample:
        source=str(c.get("volume_source") or "NONE").upper()
        value=max(float(c.get("volume",0.0) or 0.0),0.0)
        # A sampled Event-1 minute may have ticks for only part of its 60s.
        # It is useful for diagnostics but must not enter the volume profile
        # as though it were a complete minute of tick activity.
        if source=="TICK_ACTIVITY" and not bool(c.get("tick_activity_complete")):
            value=0.0
            if float(c.get("tick_activity_observed_seconds") or 0.0)>0.0:
                tick_activity_partial_bars+=1
        raw_volumes.append(value)
        volume_sources.append(source)
    volume_bars=sum(1 for v in raw_volumes if v>0.0)
    volume_coverage=volume_bars/max(1,len(sample))
    real_volume_bars=sum(
        1 for v,s in zip(raw_volumes,volume_sources)
        if v>0.0 and s=="REAL_VOLUME"
    )
    tick_volume_bars=sum(
        1 for v,s in zip(raw_volumes,volume_sources)
        if v>0.0 and s=="TICK_VOLUME"
    )
    tick_activity_bars=sum(
        1 for v,s in zip(raw_volumes,volume_sources)
        if v>0.0 and s=="TICK_ACTIVITY"
    )
    real_volume_coverage=real_volume_bars/max(1,len(sample))
    tick_volume_coverage=tick_volume_bars/max(1,len(sample))
    tick_activity_coverage=tick_activity_bars/max(1,len(sample))
    if real_volume_coverage>=0.80:
        volume_mode="REAL_VOLUME"
    elif tick_volume_coverage>=0.80:
        volume_mode="TICK_VOLUME"
    elif tick_activity_coverage>0.0:
        volume_mode="M1_TICK_ACTIVITY_PROXY"
    else:
        volume_mode="M1_EQUAL_ACTIVITY_PROXY"

    if high<=low:
        px=sample[-1]["close"]
        return {
            "poc":px,"vah":px,"val":px,
            "range_high":high,"range_low":low,
            "total_volume":sum(v if v>0 else 1.0 for v in raw_volumes),
            "bins":1,
            "volume_coverage":volume_coverage,
            "volume_mode":volume_mode,
            "volume_bars":volume_bars,
            "real_volume_bars":real_volume_bars,
            "real_volume_coverage":real_volume_coverage,
            "tick_volume_bars":tick_volume_bars,
            "tick_volume_coverage":tick_volume_coverage,
            "tick_activity_bars":tick_activity_bars,
            "tick_activity_coverage":tick_activity_coverage,
            "tick_activity_partial_bars":tick_activity_partial_bars,
        }

    step=(high-low)/float(bins)
    volumes=[0.0]*bins
    for c,v_raw in zip(sample,raw_volumes):
        vol=v_raw if v_raw>0.0 else 1.0
        clo=float(c["low"]); chi=float(c["high"])
        if chi<=clo:
            idx=int((float(c["close"])-low)/step)
            idx=max(0,min(bins-1,idx))
            volumes[idx]+=vol
            continue

        first=max(0,min(bins-1,int((clo-low)/step)))
        last=max(0,min(bins-1,int((chi-low)/step)))
        span=max(chi-clo,1e-12)
        for idx in range(first,last+1):
            row_low=low+idx*step
            row_high=low+(idx+1)*step
            overlap=max(0.0,min(chi,row_high)-max(clo,row_low))
            if overlap>0.0:
                volumes[idx]+=vol*(overlap/span)

    poc_idx=max(range(bins),key=lambda i:(volumes[i],-abs(i-(bins-1)/2.0)))
    target=sum(volumes)*VALUE_AREA_FRACTION
    accumulated=volumes[poc_idx]
    left=right=poc_idx

    while accumulated<target and (left>0 or right<bins-1):
        candidates=[]
        if left>0:
            candidates.append((volumes[left-1],"L"))
        if right<bins-1:
            candidates.append((volumes[right+1],"R"))
        if not candidates:
            break
        next_volume,side=max(candidates,key=lambda item:item[0])
        if accumulated+next_volume>target and accumulated>0:
            break
        if side=="L":
            left-=1
            accumulated+=next_volume
        else:
            right+=1
            accumulated+=next_volume

    return {
        "poc":low+(poc_idx+0.5)*step,
        "vah":min(high,low+(right+1)*step),
        "val":max(low,low+left*step),
        "range_high":high,
        "range_low":low,
        "total_volume":sum(volumes),
        "bins":bins,
        "volume_coverage":volume_coverage,
        "volume_mode":volume_mode,
        "volume_bars":volume_bars,
        "real_volume_bars":real_volume_bars,
        "real_volume_coverage":real_volume_coverage,
        "tick_volume_bars":tick_volume_bars,
        "tick_volume_coverage":tick_volume_coverage,
        "tick_activity_bars":tick_activity_bars,
        "tick_activity_coverage":tick_activity_coverage,
        "tick_activity_partial_bars":tick_activity_partial_bars,
    }


def _profile_migration(cs):
    current=_volume_profile(cs[-PROFILE_LOOKBACK:])
    if not current:
        return None
    previous_sample=cs[-(PROFILE_LOOKBACK*2):-PROFILE_LOOKBACK]
    previous=_volume_profile(previous_sample) if previous_sample else None
    if not previous:
        return {
            "poc_delta":0.0,
            "poc_migration_norm":0.0,
            "previous_poc":None,
            "available":False,
        }
    scale=max(
        abs(float(current["range_high"])-float(current["range_low"])),
        abs(float(previous["range_high"])-float(previous["range_low"])),
        1e-12,
    )
    delta=float(current["poc"])-float(previous["poc"])
    return {
        "poc_delta":delta,
        "poc_migration_norm":delta/scale,
        "previous_poc":float(previous["poc"]),
        "available":True,
    }


def _avwap_slope_features(cs,anchor_ts):
    if len(cs)<3:
        current,_=_anchored_vwap(cs,anchor_ts)
        return {
            "avwap_series":[current],
            "avwap_slope_1":0.0,
            "avwap_slope_2":0.0,
            "avwap_slope_3":0.0,
            "slope_persistence":0,
        }

    values=[]
    for cut in (3,2,1,0):
        subset=cs[:-cut] if cut else cs
        av,_=_anchored_vwap(subset,anchor_ts)
        values.append(float(av))
    s1=values[-1]-values[-2]
    s2=values[-2]-values[-3]
    s3=values[-3]-values[-4]
    pos=sum(1 for s in (s1,s2,s3) if s>0)
    neg=sum(1 for s in (s1,s2,s3) if s<0)
    return {
        "avwap_series":values,
        "avwap_slope_1":s1,
        "avwap_slope_2":s2,
        "avwap_slope_3":s3,
        "slope_persistence":pos if pos>=neg else -neg,
    }


def _slope(cs,anchor_ts):
    if len(cs)<2:
        return 0.0
    current,_=_anchored_vwap(cs,anchor_ts)
    previous,_=_anchored_vwap(cs[:-1],anchor_ts)
    return current-previous


def _m1_sequence_signature(cs,length=6):
    """Compact closed-M1 price geometry signature; no new indicator."""
    sample=list(cs[-max(3,int(length)):])
    parts=[]
    for c in sample:
        try:
            o=float(c["open"]); h=float(c["high"]); l=float(c["low"]); close=float(c["close"])
            rng=max(h-l,1e-12)
            body=min(1.0,abs(close-o)/rng)
            close_pos=min(1.0,max(0.0,(close-l)/rng))
            body_bin=min(3,int(body*4.0))
            close_bin=min(3,int(close_pos*4.0))
            direction="U" if close>o else "D" if close<o else "F"
            parts.append(f"{direction}{body_bin}{close_bin}")
        except (TypeError,ValueError):
            parts.append("F00")
    return ">".join(parts)


def _market_regime(direction,slope_persistent,value_acceptance,level_reclaim,migration_aligned,migration_against,value_position):
    d=str(direction or "").upper()
    if d in {"UP","DOWN"} and migration_against:
        return "CONFLICT"
    if d in {"UP","DOWN"} and value_acceptance and migration_aligned:
        return "ACCEPTED_" + d
    if d in {"UP","DOWN"} and level_reclaim and slope_persistent:
        return "RECLAIM_" + d
    if d in {"UP","DOWN"} and slope_persistent:
        return "TREND_" + d
    vp=str(value_position or "").upper()
    if vp in {"UPPER_VALUE","LOWER_VALUE"}:
        return "ROTATION_" + ("UP" if vp=="UPPER_VALUE" else "DOWN")
    return "BALANCED"


def _decision_time_bucket(ts):
    """Coarse two-hour UAE bucket to reduce time-of-day overfitting."""
    try:
        hour=(datetime.fromtimestamp(float(ts),tz=timezone.utc).hour+4)%24
        return f"UAE_{(hour//2)*2:02d}_{((hour//2)*2+2)%24:02d}"
    except (TypeError,ValueError,OSError):
        return "UAE_UNKNOWN"




def _smma_series(values,period):
    """Return a Wilder-style SMMA sequence without using future candles."""
    n=int(period)
    if n<=0 or len(values)<n:
        return []
    out=[None]*len(values)
    smma=sum(float(x) for x in values[:n])/float(n)
    out[n-1]=smma
    for i in range(n,len(values)):
        smma=((smma*(n-1))+float(values[i]))/float(n)
        out[i]=smma
    return out


def _alligator_confirmation(cs,direction):
    """Confirm AVWAP+VP direction with the configured Alligator.
    
    The terminal shift is a plot shift, not future data. At closed candle t,
    the visible Jaw/Teeth/Lips values correspond to the SMMA values from
    t-8/t-5/t-3 respectively. Only closed M1 candles are used.
    """
    direction=str(direction or "").upper()
    if direction not in {"UP","DOWN"}:
        return {"ready":False,"confirmed":False,"direction":direction}

    medians=[
        (float(c["high"])+float(c["low"]))/2.0
        for c in cs if isinstance(c,dict)
    ]
    required=max(
        ALLIGATOR_JAW_PERIOD+ALLIGATOR_JAW_SHIFT,
        ALLIGATOR_TEETH_PERIOD+ALLIGATOR_TEETH_SHIFT,
        ALLIGATOR_LIPS_PERIOD+ALLIGATOR_LIPS_SHIFT
    )+2
    if len(medians)<required:
        return {
            "ready":False,
            "confirmed":False,
            "direction":direction,
            "reason":"insufficient_closed_m1_history"
        }

    jaw_series=_smma_series(medians,ALLIGATOR_JAW_PERIOD)
    teeth_series=_smma_series(medians,ALLIGATOR_TEETH_PERIOD)
    lips_series=_smma_series(medians,ALLIGATOR_LIPS_PERIOD)
    idx=len(medians)-1

    def shifted(series,shift,at_idx):
        pos=int(at_idx)-int(shift)
        if pos<0 or pos>=len(series) or series[pos] is None:
            return None
        return float(series[pos])

    jaw=shifted(jaw_series,ALLIGATOR_JAW_SHIFT,idx)
    teeth=shifted(teeth_series,ALLIGATOR_TEETH_SHIFT,idx)
    lips=shifted(lips_series,ALLIGATOR_LIPS_SHIFT,idx)
    prev_jaw=shifted(jaw_series,ALLIGATOR_JAW_SHIFT,idx-1)
    prev_teeth=shifted(teeth_series,ALLIGATOR_TEETH_SHIFT,idx-1)
    prev_lips=shifted(lips_series,ALLIGATOR_LIPS_SHIFT,idx-1)

    if None in (jaw,teeth,lips,prev_jaw,prev_teeth,prev_lips):
        return {
            "ready":False,
            "confirmed":False,
            "direction":direction,
            "reason":"alligator_not_ready"
        }

    last_close=float(cs[-1]["close"])
    if direction=="UP":
        aligned=(
            lips>teeth+ALLIGATOR_MIN_SEPARATION
            and teeth>jaw+ALLIGATOR_MIN_SEPARATION
        )
        slope_votes=sum(
            1 for current,previous in ((lips,prev_lips),(teeth,prev_teeth),(jaw,prev_jaw))
            if current>=previous
        )
        sloping=slope_votes>=2
        price_position=last_close>=lips
    else:
        aligned=(
            lips<teeth-ALLIGATOR_MIN_SEPARATION
            and teeth<jaw-ALLIGATOR_MIN_SEPARATION
        )
        slope_votes=sum(
            1 for current,previous in ((lips,prev_lips),(teeth,prev_teeth),(jaw,prev_jaw))
            if current<=previous
        )
        sloping=slope_votes>=2
        price_position=last_close<=lips

    return {
        "ready":True,
        "confirmed":bool(aligned and sloping and price_position),
        "direction":direction,
        "jaw":jaw,
        "teeth":teeth,
        "lips":lips,
        "prev_jaw":prev_jaw,
        "prev_teeth":prev_teeth,
        "prev_lips":prev_lips,
        "aligned":bool(aligned),
        "sloping":bool(sloping),
        "price_position":bool(price_position),
        "periods":"13/8,8/5,5/3",
        "confirmation":"CONFIRMED" if (aligned and sloping and price_position) else "REJECTED",
    }


def _bollinger_confirmation(candles, direction, period=18, multiplier=2.0):
    """Return a zero-network M1 Bollinger 18/2 confirmation layer.
    
    Bollinger is confirmation only: it never chooses or flips the AVWAP+VP
    direction and it never hard-blocks a live candidate. This keeps the
    3-minute scheduler and signal cadence independent of the confirmation.
    """
    try:
        n=int(period)
        k=float(multiplier)
    except (TypeError,ValueError):
        n=18
        k=2.0
    n=max(18,min(18,n))
    k=2.0

    closes=[_f(x.get("close"),0.0) for x in (candles or []) if isinstance(x,dict)]
    closes=[x for x in closes if x>0.0]
    if len(closes)<n:
        return {
            "bb_period":n,
            "bb_multiplier":k,
            "bb_ready":False,
            "bb_confirmation":"UNAVAILABLE",
            "bb_basis":None,
            "bb_upper":None,
            "bb_lower":None,
            "bb_bandwidth":None,
            "bb_bandwidth_expanding":False,
        }

    window=closes[-n:]
    mean=sum(window)/n
    variance=sum((x-mean)*(x-mean) for x in window)/n
    stdev=variance**0.5
    upper=mean+(k*stdev)
    lower=mean-(k*stdev)

    prev_window=closes[-(n+1):-1] if len(closes)>=n+1 else []
    prev_mean=(sum(prev_window)/n) if len(prev_window)==n else mean
    prev_var=(
        sum((x-prev_mean)*(x-prev_mean) for x in prev_window)/n
        if len(prev_window)==n else variance
    )
    prev_stdev=max(0.0,prev_var**0.5)
    prev_upper=prev_mean+(k*prev_stdev)
    prev_lower=prev_mean-(k*prev_stdev)

    bandwidth=(upper-lower)/max(abs(mean),1e-12)
    prev_bandwidth=(prev_upper-prev_lower)/max(abs(prev_mean),1e-12)
    bandwidth_expanding=bandwidth>prev_bandwidth

    last_close=window[-1]
    direction=str(direction or "").upper()
    if direction=="UP":
        aligned=(last_close>=mean and upper>=prev_upper and (bandwidth_expanding or last_close>=upper*0.995))
        state="ALIGNED" if aligned else "NEUTRAL"
    elif direction=="DOWN":
        aligned=(last_close<=mean and lower<=prev_lower and (bandwidth_expanding or last_close<=lower*1.005))
        state="ALIGNED" if aligned else "NEUTRAL"
    else:
        state="UNAVAILABLE"

    return {
        "bb_period":n,
        "bb_multiplier":k,
        "bb_ready":True,
        "bb_confirmation":state,
        "bb_basis":mean,
        "bb_upper":upper,
        "bb_lower":lower,
        "bb_bandwidth":bandwidth,
        "bb_bandwidth_expanding":bandwidth_expanding,
        "bb_last_close":last_close,
        "bb_previous_basis":prev_mean,
        "bb_previous_upper":prev_upper,
        "bb_previous_lower":prev_lower,
    }
def analyze_asset(
    asset,
    candles,
    price=None,
    forced_strategy=None,
    learning_campaign=False,
):
    """Return one deterministic AVWAP + Volume Profile candidate or None."""
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
    previous=cs[-2]
    anchor_ts=int(blocks[-1]["time"])
    avwap,avwap_volume=_anchored_vwap(cs,anchor_ts)
    if avwap<=0:
        _diag(pair,"invalid_avwap",avwap=avwap)
        return None

    profile=_volume_profile(cs)
    if not profile or float(profile.get("poc") or 0.0)<=0:
        _diag(pair,"invalid_profile",profile=bool(profile))
        return None

    px=float(last["close"])
    poc=float(profile["poc"])
    vah=float(profile["vah"])
    val=float(profile["val"])
    slope=avwap-float(_anchored_vwap(cs[:-1],anchor_ts)[0])
    slope_features=_avwap_slope_features(cs,anchor_ts)
    migration=_profile_migration(cs)

    up=px>avwap and px>poc
    down=px<avwap and px<poc
    if not (up or down):
        _diag(pair,"no_two_indicator_alignment",price=round(px,10),avwap=round(avwap,10),poc=round(poc,10),vah=round(vah,10),val=round(val,10))
        return None

    direction="UP" if up else "DOWN"
    # Bollinger Bands 18/2 are a secondary M1 confirmation layer only.
    # No BB state can change the AVWAP+VP direction or block the scheduler.
    bb=_bollinger_confirmation(cs,direction,period=18,multiplier=2.0)
    slope_aligned_steps=(
        sum(
            1 for s in (
                slope_features["avwap_slope_1"],
                slope_features["avwap_slope_2"],
                slope_features["avwap_slope_3"],
            ) if s>0
        ) if direction=="UP" else
        sum(
            1 for s in (
                slope_features["avwap_slope_1"],
                slope_features["avwap_slope_2"],
                slope_features["avwap_slope_3"],
            ) if s<0
        )
    )
    slope_persistent=slope_aligned_steps>=2

    value_acceptance=(px>=vah) if direction=="UP" else (px<=val)
    prev_key=max(avwap,poc) if direction=="UP" else min(avwap,poc)
    current_key=max(avwap,poc) if direction=="UP" else min(avwap,poc)
    level_reclaim=(
        float(previous["close"])<=prev_key and px>current_key
        if direction=="UP"
        else float(previous["close"])>=prev_key and px<current_key
    )

    migration_norm=float((migration or {}).get("poc_migration_norm") or 0.0)
    migration_aligned=(
        migration_norm>0.05 if direction=="UP"
        else migration_norm<-0.05
    )
    migration_against=(
        migration_norm<-0.15 if direction=="UP"
        else migration_norm>0.15
    )

    value_width=max(vah-val,1e-12)
    profile_range=max(float(profile["range_high"])-float(profile["range_low"]),1e-12)
    value_position=(
        "ABOVE_VALUE" if px>vah else
        "BELOW_VALUE" if px<val else
        "UPPER_VALUE" if px>poc else
        "LOWER_VALUE" if px<poc else "AT_POC"
    )

    # Alligator confirmation is mandatory for the live candidate.
    # It validates the AVWAP+VP direction; it never creates or flips direction.
    alligator=_alligator_confirmation(cs,direction)
    if not bool(alligator.get("ready")):
        _diag(pair,"alligator_not_ready",details=alligator.get("reason"))
        return None
    if not bool(alligator.get("confirmed")):
        _diag(
            pair,"alligator_confirmation_rejected",
            direction=direction,
            aligned=alligator.get("aligned"),
            sloping=alligator.get("sloping"),
            price_position=alligator.get("price_position"),
        )
        return None

    # LIVE EXACT-SETUP FILTER:
    # Only the exact point identified by the current result analysis enters the
    # live selector: ABOVE_VALUE + no level reclaim + persistent AVWAP slope.
    # This is an empirical qualification rule, not a promise of future wins.
    exact_live_setup=(
        (
            (direction=="UP" and value_position=="ABOVE_VALUE")
            or (direction=="DOWN" and value_position=="BELOW_VALUE")
        )
        and level_reclaim is False
        and slope_persistent is True
    )
    if not exact_live_setup:
        _diag(
            pair,"exact_live_setup_rejected",
            direction=direction,value_position=value_position,
            level_reclaim=level_reclaim,slope_persistent=slope_persistent
        )
        return None

    coverage=float(profile.get("volume_coverage") or 0.0)
    real_coverage=float(profile.get("real_volume_coverage") or 0.0)
    tick_coverage=float(profile.get("tick_volume_coverage") or 0.0)
    tick_activity_coverage=float(profile.get("tick_activity_coverage") or 0.0)
    tick_activity_partial_bars=int(profile.get("tick_activity_partial_bars") or 0)
    # Keep quality semantics truthful. "HIGH" is reserved for broker-reported
    # real/traded volume. Broker tick volume and locally observed tick activity
    # are useful proxies but are never labelled as real volume.
    if real_coverage>=0.80:
        volume_quality="HIGH"
    elif tick_coverage>=0.80:
        volume_quality="TICK_VOLUME"
    elif tick_activity_coverage>=0.80:
        volume_quality="TICK_ACTIVITY"
    else:
        volume_quality="LOW"
    volume_bars=int(profile.get("volume_bars") or 0)
    volume_proxy_mode=real_coverage<HIGH_VOLUME_MIN_COVERAGE
    high_volume_confirmed=bool(
        real_coverage>=HIGH_VOLUME_MIN_COVERAGE
        and volume_bars>=int(PROFILE_LOOKBACK*HIGH_VOLUME_MIN_COVERAGE)
    )
    # Live signals are allowed only when broker-reported real/traded volume is
    # sufficiently complete. Tick-volume and locally observed activity remain
    # diagnostics/filters but cannot qualify a "HIGH VOLUME" live signal.
    if not high_volume_confirmed:
        _diag(
            pair,"high_volume_required",
            real_volume_coverage=round(real_coverage,3),
            volume_bars=int(profile.get("volume_bars") or 0),
            required_bars=int(PROFILE_LOOKBACK*HIGH_VOLUME_MIN_COVERAGE),
            volume_mode=profile.get("volume_mode"),
        )
        return None

    if volume_proxy_mode:
        _diag(
            pair,"volume_proxy_candidate",
            coverage=round(coverage,3),
            volume_bars=int(profile.get("volume_bars") or 0),
            volume_mode=profile.get("volume_mode"),
        )

    # One-minute continuation guard: the exact setup must still have upward
    # closed-M1 continuation at the decision candle. This is a zero-network
    # price-action check on the same closed candles already used by the Brain.
    last_open=float(last["open"])
    last_close=float(last["close"])
    previous_close=float(previous["close"])
    m1_continuation_ok=(
        (
            (direction=="UP" and last_close>=last_open and last_close>previous_close)
            or (direction=="DOWN" and last_close<=last_open and last_close<previous_close)
        )
    )
    if not m1_continuation_ok:
        _diag(
            pair,"m1_continuation_failed",
            direction=direction,
            last_open=round(last_open,10),
            last_close=round(last_close,10),
            previous_close=round(previous_close,10),
        )
        return None

    # A one-minute expiry benefits from directional acceptance or a genuine
    # reclaim of AVWAP/POC. A mere location above/below both levels while still
    # trapped inside the value area is treated as a weak setup.
    directional_acceptance=bool(value_acceptance or level_reclaim)
    if not directional_acceptance:
        _diag(
            pair,"inside_value_without_acceptance",
            direction=direction,price=round(px,10),avwap=round(avwap,10),
            poc=round(poc,10),vah=round(vah,10),val=round(val,10),
            reclaim=level_reclaim
        )
        return None

    if migration_against:
        _diag(pair,"poc_migration_against",direction=direction,migration_norm=round(migration_norm,4))
        return None

    # Alligator is already a mandatory gate; add a small confluence
    # bonus because it is fully aligned with the authoritative direction.
    alligator_bonus=3

    # Bollinger 18/2 confirmation affects confluence/ranking only.
    # It is deliberately soft so an isolated BB disagreement cannot suppress
    # otherwise valid AVWAP+VP setups or disturb the 3-minute signal cadence.
    bb_bonus=2 if bb.get("bb_confirmation")=="ALIGNED" else 0

    # If a previous profile exists and the POC is migrating in the same
    # direction, that is a reinforcing confluence. Flat migration is neutral.
    score=90
    if slope_persistent:
        score+=3
    if value_acceptance:
        score+=3
    elif level_reclaim:
        score+=1
    if migration_aligned:
        score+=2
    if alligator_bonus:
        score+=alligator_bonus
    if bb_bonus:
        score+=bb_bonus
    if high_volume_confirmed:
        score+=2

    confidence=min(99,max(0,int(score)))

    trend="AVWAP_BULLISH" if up else "AVWAP_BEARISH"
    structure="ABOVE_AVWAP_POC" if up else "BELOW_AVWAP_POC"

    features={
        "anchored_vwap":avwap,
        "volume_profile_poc":poc,
        "volume_profile_vah":vah,
        "volume_profile_val":val,
        "avwap_slope":slope,
        "avwap_slope_1":slope_features["avwap_slope_1"],
        "avwap_slope_2":slope_features["avwap_slope_2"],
        "avwap_slope_3":slope_features["avwap_slope_3"],
        "avwap_slope_persistence":slope_features["slope_persistence"],
        "profile_poc_delta":float((migration or {}).get("poc_delta") or 0.0),
        "profile_poc_migration_norm":migration_norm,
        "profile_previous_poc":(migration or {}).get("previous_poc"),
        "profile_migration_available":bool((migration or {}).get("available")),
        "alligator_confirmed":True,
        "alligator_confirmation":"CONFIRMED",
        "alligator_periods":alligator.get("periods","13/8,8/5,5/3"),
        "alligator_jaw":alligator.get("jaw"),
        "alligator_teeth":alligator.get("teeth"),
        "alligator_lips":alligator.get("lips"),
        "alligator_prev_jaw":alligator.get("prev_jaw"),
        "alligator_prev_teeth":alligator.get("prev_teeth"),
        "alligator_prev_lips":alligator.get("prev_lips"),
        "alligator_aligned":True,
        "alligator_sloping":True,
        "alligator_price_position":True,
        "alligator_bonus":alligator_bonus,
        "bb_period":bb.get("bb_period",18),
        "bb_multiplier":bb.get("bb_multiplier",2.0),
        "bb_ready":bb.get("bb_ready",False),
        "bb_confirmation":bb.get("bb_confirmation","UNAVAILABLE"),
        "bb_basis":bb.get("bb_basis"),
        "bb_upper":bb.get("bb_upper"),
        "bb_lower":bb.get("bb_lower"),
        "bb_bandwidth":bb.get("bb_bandwidth"),
        "bb_bandwidth_expanding":bb.get("bb_bandwidth_expanding",False),
        "bb_confirmation_bonus":bb_bonus,
        "profile_range_high":profile["range_high"],
        "profile_range_low":profile["range_low"],
        "profile_range":profile_range,
        "profile_value_area_width":value_width,
        "profile_total_volume":profile["total_volume"],
        "profile_bins":profile["bins"],
        "volume_coverage":coverage,
        "volume_bars":int(profile.get("volume_bars") or 0),
        "high_volume_confirmed":high_volume_confirmed,
        "high_volume_min_coverage":HIGH_VOLUME_MIN_COVERAGE,
        "high_volume_required_bars":int(PROFILE_LOOKBACK*HIGH_VOLUME_MIN_COVERAGE),
        "volume_mode":profile.get("volume_mode"),
        "volume_quality":volume_quality,
        "volume_data_class":(
            "REAL_VOLUME" if real_coverage>=0.80
            else "TICK_VOLUME" if tick_coverage>=0.80
            else "TICK_ACTIVITY_PROXY" if tick_activity_coverage>0.0
            else "NO_BROKER_VOLUME"
        ),
        "real_volume_verified":bool(real_coverage>=0.80),
        "real_volume_bars":int(profile.get("real_volume_bars") or 0),
        "real_volume_coverage":float(profile.get("real_volume_coverage") or 0.0),
        "tick_volume_bars":int(profile.get("tick_volume_bars") or 0),
        "tick_volume_coverage":float(profile.get("tick_volume_coverage") or 0.0),
        "tick_activity_bars":int(profile.get("tick_activity_bars") or 0),
        "tick_activity_coverage":float(profile.get("tick_activity_coverage") or 0.0),
        "tick_activity_partial_bars":tick_activity_partial_bars,
        "volume_proxy_mode":volume_proxy_mode,
        "m1_continuation_ok":m1_continuation_ok,
        "value_position":value_position,
        "value_area_acceptance":value_acceptance,
        "level_reclaim":level_reclaim,
        "slope_persistent":slope_persistent,
        "exact_live_setup":exact_live_setup,
        "poc_migration_aligned":migration_aligned,
        "poc_migration_against":migration_against,
        "anchor_15m_start_ts":anchor_ts,
        "avwap_volume":avwap_volume,
        "m1_sequence_signature":_m1_sequence_signature(cs,6),
        "market_regime":_market_regime(
            direction,slope_persistent,value_acceptance,level_reclaim,
            migration_aligned,migration_against,value_position
        ),
        "decision_time_bucket":_decision_time_bucket(last["time"]),
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
            f"slope_persistence={slope_aligned_steps}/3; value={value_position}; "
            f"reclaim={level_reclaim}; POC_migration={migration_norm:.4f}; "
            f"volume_quality={volume_quality}; HIGH_VOLUME=CONFIRMED; AVWAP+POC aligned; "
            f"Alligator(13/8,8/5,5/3)=CONFIRMED; "
            f"BB(18,2)={bb.get('bb_confirmation','UNAVAILABLE')}."
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
        "self_strategy_version":"AVWAP_VP_V2",
        "strategy_candidates":[{
            "strategy":ALLOWED_STRATEGY,
            "direction":direction,
            "score":confidence,
            "expiry_minutes":1,
            "self_strategy_version":"AVWAP_VP_V2",
        }],
        "strategy_audit":[{
            "strategy":ALLOWED_STRATEGY,
            "active_for_live":True,
            "up_qualified":bool(up),
            "down_qualified":bool(down),
        }],
        "strategy_audit_count":1,
        "indicator_audit_scope":"AVWAP_VOLUME_PROFILE_WITH_ALLIGATOR_13_8_8_5_5_3_AND_BB18_2_CONFIRMATION",
        "m1_sequence_signature":features["m1_sequence_signature"],
        "market_regime":features["market_regime"],
        "decision_time_bucket":features["decision_time_bucket"],
    }
    return result