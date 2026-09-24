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
    volume_raw=raw.get("volume",raw.get("v"))
    volume=_f(volume_raw,0.0)
    return {
        "time":_ts(raw.get("time",raw.get("t"))),
        "open":_f(raw.get("open",raw.get("o"))),
        "high":_f(raw.get("high",raw.get("h"))),
        "low":_f(raw.get("low",raw.get("l"))),
        "close":_f(raw.get("close",raw.get("c"))),
        "volume":max(volume,0.0),
        "volume_present":bool(volume>0.0),
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
    raw_volumes=[max(float(c.get("volume",0.0) or 0.0),0.0) for c in sample]
    volume_bars=sum(1 for v in raw_volumes if v>0.0)
    volume_coverage=volume_bars/max(1,len(sample))
    volume_mode="REAL_VOLUME_OR_TICK_VOLUME" if volume_coverage>=0.80 else "M1_EQUAL_ACTIVITY_PROXY"

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
    # Broker candle feeds may expose no usable volume (coverage=0). In that case
    # Volume Profile is computed from equal-activity bars as a clearly labelled
    # proxy. The proxy is NEVER treated as real volume: live delivery requires
    # strict external-AI verification later in app.py.
    volume_quality="HIGH" if coverage>=0.80 else "MEDIUM" if coverage>=0.50 else "LOW"
    volume_proxy_mode=coverage<0.80
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
    if bb_bonus:
        score+=bb_bonus
    if coverage>=0.80:
        score+=1
    elif volume_proxy_mode:
        # Proxy-volume candidates need independent verification and therefore
        # start below true-volume candidates in ranking.
        score-=3

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
        "volume_mode":profile.get("volume_mode"),
        "volume_quality":volume_quality,
        "real_volume_verified":not volume_proxy_mode,
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
            f"volume_quality={volume_quality}; AVWAP+POC aligned; "
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
        "indicator_audit_scope":"AVWAP_VOLUME_PROFILE_WITH_BB18_2_CONFIRMATION",
        "m1_sequence_signature":features["m1_sequence_signature"],
        "market_regime":features["market_regime"],
        "decision_time_bucket":features["decision_time_bucket"],
    }
    return result