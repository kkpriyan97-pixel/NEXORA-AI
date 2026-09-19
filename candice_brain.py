"""Candice Brain: multi-strategy, multi-timeframe, evidence-first technical analysis."""
from __future__ import annotations
from math import isfinite
EXPIRIES=(1,2,3,4,5,10,15)

def _f(x,d=0.0):
    try:
        v=float(x); return v if isfinite(v) else d
    except Exception:return d
def _norm(c):
    return {"time":c.get("time",c.get("t")),"open":_f(c.get("open",c.get("o"))),"high":_f(c.get("high",c.get("h"))),"low":_f(c.get("low",c.get("l"))),"close":_f(c.get("close",c.get("c"))),"volume":_f(c.get("volume",c.get("v")))}
def ema(v,n):
    if not v:return 0.0
    k=2/(n+1);e=v[0]
    for x in v[1:]:e=x*k+e*(1-k)
    return e
def rsi(v,n=14):
    if len(v)<n+1:return 50.0
    g=[];l=[]
    for a,b in zip(v[-n-1:-1],v[-n:]):
        d=b-a;g.append(max(d,0));l.append(max(-d,0))
    ag=sum(g)/n;al=sum(l)/n
    return 100.0 if al==0 else 100-(100/(1+ag/al))
def atr(cs,n=14):
    if len(cs)<2:return 0.0
    t=[max(b["high"]-b["low"],abs(b["high"]-a["close"]),abs(b["low"]-a["close"])) for a,b in zip(cs[-n-1:-1],cs[-n:])]
    return sum(t)/len(t) if t else 0.0
def _trend15(cs):
    # 15 completed 1m candles per block; use the most recent completed blocks.
    blocks=[cs[i:i+15] for i in range(max(0,len(cs)-75),len(cs),15)]
    blocks=[b for b in blocks if len(b)==15]
    if len(blocks)<3:return "SIDEWAYS"
    a,b=blocks[-2],blocks[-1]
    ah=max(x["high"] for x in a);al=min(x["low"] for x in a);ac=a[-1]["close"]
    bh=max(x["high"] for x in b);bl=min(x["low"] for x in b);bc=b[-1]["close"]
    if bc>ac and bh>=ah:return "UP"
    if bc<ac and bl<=al:return "DOWN"
    return "SIDEWAYS"
def _score(direction,trend,s1,rr,body,rng,dist,aa,pattern):
    score=0.0
    score+=25 if direction==trend else 0
    score+=20 if (direction=="UP" and s1=="BULLISH") or (direction=="DOWN" and s1=="BEARISH") else 0
    score+=15 if (direction=="UP" and rr>=52) or (direction=="DOWN" and rr<=48) else 0
    score+=12*min(1.0,abs(body)/rng)
    score+=10 if aa>0 and dist>=0 else 0
    score+=10 if pattern in ("BULLISH_CANDLE","BEARISH_CANDLE") and ((direction=="UP" and pattern.startswith("BULL")) or (direction=="DOWN" and pattern.startswith("BEAR")) ) else 0
    score+=8 if (direction=="UP" and 35<=rr<=68) or (direction=="DOWN" and 32<=rr<=65) else 0
    return round(min(100,score),2)

def analyze_asset(asset,candles,price=None):
    cs=[_norm(c) for c in candles if isinstance(c,dict)]
    if len(cs)<45:return None
    v=[c["close"] for c in cs];last=cs[-1];p=_f(price,last["close"])
    e9=ema(v[-40:],9);e21=ema(v[-40:],21);rr=rsi(v);aa=atr(cs)
    hi=max(c["high"] for c in cs[-30:]);lo=min(c["low"] for c in cs[-30:])
    body=last["close"]-last["open"];rng=max(last["high"]-last["low"],1e-12)
    upper=last["high"]-max(last["open"],last["close"]);lower=min(last["open"],last["close"])-last["low"]
    s1="BULLISH" if e9>e21 and p>=e9 else "BEARISH" if e9<e21 and p<=e9 else "MIXED"
    trend=_trend15(cs)
    pattern="BULLISH_CANDLE" if body>0 and body/rng>=.55 else "BEARISH_CANDLE" if body<0 and -body/rng>=.55 else "PIN_REJECTION" if max(upper,lower)/rng>.45 else "NEUTRAL"
    candidates=[]
    def add(direction,strategy,score,expiry,reason):
        if direction in ("UP","DOWN") and score>=45:
            candidates.append({"direction":direction,"strategy":strategy,"score":score,"expiry_minutes":expiry,"reason":reason})
    add("UP","TREND_FOLLOWING",_score("UP",trend,s1,rr,body,rng,p-e21,aa,pattern),5,
        f"Trend continuation; 15m={trend}, 1m={s1}, RSI={rr:.1f}, EMA9>EMA21.")
    add("DOWN","TREND_FOLLOWING",_score("DOWN",trend,s1,rr,body,rng,e21-p,aa,pattern),5,
        f"Trend continuation; 15m={trend}, 1m={s1}, RSI={rr:.1f}, EMA9<EMA21.")
    add("UP","PULLBACK",_score("UP",trend,s1,rr,body,rng,p-e21,aa,pattern)+ (8 if lower>abs(body)*1.2 else 0),2,
        f"Pullback/rejection support; 15m={trend}, lower_wick={lower:.5g}.")
    add("DOWN","PULLBACK",_score("DOWN",trend,s1,rr,body,rng,e21-p,aa,pattern)+ (8 if upper>abs(body)*1.2 else 0),2,
        f"Pullback/rejection resistance; 15m={trend}, upper_wick={upper:.5g}.")
    add("UP","BREAKOUT",_score("UP",trend,s1,rr,body,rng,p-e21,aa,pattern)+ (10 if p>=hi-aa*.25 and body>0 else 0),1,
        f"Breakout test near 30-candle high; ATR={aa:.5g}.")
    add("DOWN","BREAKOUT",_score("DOWN",trend,s1,rr,body,rng,e21-p,aa,pattern)+ (10 if p<=lo+aa*.25 and body<0 else 0),1,
        f"Breakdown test near 30-candle low; ATR={aa:.5g}.")
    add("UP","MEAN_REVERSION",min(100,55+(30-rr) if rr<30 and body>0 else 0),3,
        f"Oversold rebound candidate; RSI={rr:.1f}.")
    add("DOWN","MEAN_REVERSION",min(100,55+(rr-70) if rr>70 and body<0 else 0),3,
        f"Overbought rejection candidate; RSI={rr:.1f}.")
    # Conflict gate: do not force a signal when 15m and 1m structures disagree.
    valid=[x for x in candidates if not (trend=="UP" and x["direction"]=="DOWN") and not (trend=="DOWN" and x["direction"]=="UP")]
    if not valid:return None
    best=max(valid,key=lambda x:x["score"])
    if best["score"]<78:return None
    confidence=min(99,int(best["score"]+8))
    return {"pair":str(asset.get("pair","")),"display_name":str(asset.get("display_name") or asset.get("title") or ""),
            "direction":best["direction"],"confidence":confidence,"strategy":best["strategy"],"expiry_minutes":best["expiry_minutes"],
            "pattern":pattern,"trend_15m":trend,"structure_1m":s1,"market_quality":best["score"],
            "reason":best["reason"],"entry_candle_ts":last["time"],"price":p}
