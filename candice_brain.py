"""Candice live technical brain. Read-only; never places orders."""
from __future__ import annotations
from math import isfinite

EXPIRIES=(1,2,3,4,5,10,15)

def _f(x,d=0.0):
    try:
        v=float(x); return v if isfinite(v) else d
    except Exception: return d

def _norm(c):
    return {"time":c.get("time",c.get("t")),"open":_f(c.get("open",c.get("o"))),"high":_f(c.get("high",c.get("h"))),"low":_f(c.get("low",c.get("l"))),"close":_f(c.get("close",c.get("c"))),"volume":_f(c.get("volume",c.get("v")))}

def ema(v,n):
    if not v:return 0.0
    k=2/(n+1); e=v[0]
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
    t=[]
    for a,b in zip(cs[-n-1:-1],cs[-n:]):
        t.append(max(b["high"]-b["low"],abs(b["high"]-a["close"]),abs(b["low"]-a["close"])))
    return sum(t)/len(t) if t else 0.0

def analyze_asset(asset,candles,price=None):
    cs=[_norm(c) for c in candles if isinstance(c,dict)]
    if len(cs)<30:return None
    v=[c["close"] for c in cs]; last=cs[-1];p=_f(price,last["close"])
    e9=ema(v[-30:],9);e21=ema(v[-30:],21);rr=rsi(v);aa=atr(cs)
    hi=max(c["high"] for c in cs[-30:]);lo=min(c["low"] for c in cs[-30:])
    body=last["close"]-last["open"];rng=max(last["high"]-last["low"],1e-12)
    upper=last["high"]-max(last["open"],last["close"]);lower=min(last["open"],last["close"])-last["low"]
    s1="BULLISH" if e9>e21 and rr>=52 and p>=e9 else "BEARISH" if e9<e21 and rr<=48 and p<=e9 else "MIXED"
    blocks=[]
    for i in range(max(0,len(cs)-60),len(cs),15):
        b=cs[i:i+15]
        if len(b)>=10:blocks.append(b)
    if len(blocks)>=2:
        a,b=blocks[-2],blocks[-1];ca=a[-1]["close"];cb=b[-1]["close"]
        trend="UP" if cb>ca and max(x["high"] for x in b)>=max(x["high"] for x in a) else "DOWN" if cb<ca and min(x["low"] for x in b)<=min(x["low"] for x in a) else "SIDEWAYS"
    else:trend="UP" if e9>e21 else "DOWN" if e9<e21 else "SIDEWAYS"
    pattern="BULLISH_CANDLE" if body>0 and body/rng>=.55 else "BEARISH_CANDLE" if body<0 and -body/rng>=.55 else "PIN_REJECTION" if max(upper,lower)/rng>.45 else "NEUTRAL"
    direction=None;strategy=""
    if trend=="UP" and s1=="BULLISH" and (pattern=="BULLISH_CANDLE" or p>e9):direction="UP";strategy="TREND_FOLLOWING"
    elif trend=="DOWN" and s1=="BEARISH" and (pattern=="BEARISH_CANDLE" or p<e9):direction="DOWN";strategy="TREND_FOLLOWING"
    elif trend=="UP" and lower>abs(body)*1.2 and p>=e21:direction="UP";strategy="PULLBACK"
    elif trend=="DOWN" and upper>abs(body)*1.2 and p<=e21:direction="DOWN";strategy="PULLBACK"
    elif p>=hi-aa*.25 and body>0:direction="UP";strategy="BREAKOUT"
    elif p<=lo+aa*.25 and body<0:direction="DOWN";strategy="BREAKOUT"
    elif rr<30 and body>0:direction="UP";strategy="MEAN_REVERSION"
    elif rr>70 and body<0:direction="DOWN";strategy="MEAN_REVERSION"
    if not direction or trend=="SIDEWAYS" or s1=="MIXED":return None
    alignment=25
    momentum=min(20,abs(rr-50)*.45);candle=min(15,abs(body)/rng*15);vol=10 if aa>0 else 5
    quality=alignment+momentum+candle+vol+min(15,max(0,abs(p-e21)/(aa or 1)*5))
    confidence=min(99,int(70+quality*.28))
    if confidence<90:return None
    expiry=5
    if strategy=="PULLBACK":expiry=2
    elif strategy=="BREAKOUT":expiry=1
    elif abs(rr-50)>15:expiry=3
    return {"pair":str(asset.get("pair","")),"display_name":str(asset.get("display_name") or asset.get("title") or ""),
            "direction":direction,"confidence":confidence,"strategy":strategy,"expiry_minutes":expiry,
            "pattern":pattern,"trend_15m":trend,"structure_1m":s1,"market_quality":round(quality,2),
            "reason":f"{strategy}: {pattern}; 15m={trend}; 1m={s1}; RSI={rr:.1f}; EMA9/21 aligned.",
            "entry_candle_ts":last["time"],"price":p}
