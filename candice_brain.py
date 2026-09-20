"""Candice Brain: evidence-first multi-strategy, multi-timeframe market analysis."""
from __future__ import annotations
from math import isfinite
from self_strategy import discover as discover_self_strategy

EXPIRIES=(1,2,3,4,5,10,15)

def _f(x,d=0.0):
    try:
        v=float(x); return v if isfinite(v) else d
    except Exception:return d

def _norm(c):
    return {"time":c.get("time",c.get("t")),"open":_f(c.get("open",c.get("o"))),
            "high":_f(c.get("high",c.get("h"))),"low":_f(c.get("low",c.get("l"))),
            "close":_f(c.get("close",c.get("c"))),"volume":_f(c.get("volume",c.get("v")))}

def ema(v,n):
    if not v:return 0.0
    k=2/(n+1); e=v[0]
    for x in v[1:]: e=x*k+e*(1-k)
    return e

def rsi(v,n=14):
    if len(v)<n+1:return 50.0
    g=[];l=[]
    for a,b in zip(v[-n-1:-1],v[-n:]):
        d=b-a; g.append(max(d,0)); l.append(max(-d,0))
    ag=sum(g)/n; al=sum(l)/n
    return 100.0 if al==0 else 100-(100/(1+ag/al))

def atr(cs,n=14):
    if len(cs)<2:return 0.0
    t=[max(b["high"]-b["low"],abs(b["high"]-a["close"]),abs(b["low"]-a["close"]))
       for a,b in zip(cs[-n-1:-1],cs[-n:])]
    return sum(t)/len(t) if t else 0.0

def _trend15(cs):
    # 15 completed one-minute candles per block.
    blocks=[cs[i:i+15] for i in range(max(0,len(cs)-75),len(cs),15)]
    blocks=[b for b in blocks if len(b)==15]
    if len(blocks)<3:return "SIDEWAYS"
    a,b=blocks[-2],blocks[-1]
    if b[-1]["close"]>a[-1]["close"] and max(x["high"] for x in b)>=max(x["high"] for x in a): return "UP"
    if b[-1]["close"]<a[-1]["close"] and min(x["low"] for x in b)<=min(x["low"] for x in a): return "DOWN"
    return "SIDEWAYS"

def analyze_asset(asset,candles,price=None):
    cs=[_norm(c) for c in candles if isinstance(c,dict)]
    if len(cs)<45:return None
    v=[c["close"] for c in cs]; last=cs[-1]; p=_f(price,last["close"])
    e9=ema(v[-40:],9); e21=ema(v[-40:],21); rr=rsi(v); aa=atr(cs)
    hi=max(c["high"] for c in cs[-30:]); lo=min(c["low"] for c in cs[-30:])
    body=last["close"]-last["open"]; rng=max(last["high"]-last["low"],1e-12)
    upper=last["high"]-max(last["open"],last["close"])
    lower=min(last["open"],last["close"])-last["low"]
    s1="BULLISH" if e9>e21 and p>=e9 else "BEARISH" if e9<e21 and p<=e9 else "MIXED"
    trend=_trend15(cs)

    # Structure, momentum, volatility, price action and candle evidence.
    prev=cs[-2]["close"]
    momentum=p-prev
    avg_rng=sum(max(c["high"]-c["low"],0) for c in cs[-14:])/14
    volatility_ratio=(aa/avg_rng) if avg_rng else 1.0
    resistance=max(c["high"] for c in cs[-20:-1])
    support=min(c["low"] for c in cs[-20:-1])
    breakout_up=p>resistance and body>0
    breakout_down=p<support and body<0
    near_support=(p-support)<=max(aa*0.35,1e-12)
    near_resistance=(resistance-p)<=max(aa*0.35,1e-12)
    bullish_rejection=lower>max(abs(body)*1.2,rng*.35) and p>=last["open"]
    bearish_rejection=upper>max(abs(body)*1.2,rng*.35) and p<=last["open"]
    pattern=("BULLISH_CANDLE" if body>0 and body/rng>=.55 else
             "BEARISH_CANDLE" if body<0 and -body/rng>=.55 else
             "BULLISH_REJECTION" if bullish_rejection else
             "BEARISH_REJECTION" if bearish_rejection else "NEUTRAL")

    def score(direction,strategy):
        s=0.0
        if direction==trend:s+=22
        if (direction=="UP" and s1=="BULLISH") or (direction=="DOWN" and s1=="BEARISH"):s+=18
        if (direction=="UP" and 50<=rr<=68) or (direction=="DOWN" and 32<=rr<=50):s+=12
        if (direction=="UP" and momentum>0) or (direction=="DOWN" and momentum<0):s+=10
        s+=8*min(1,abs(body)/rng)
        if strategy=="BREAKOUT" and ((direction=="UP" and breakout_up) or (direction=="DOWN" and breakout_down)):s+=18
        if strategy in ("PULLBACK","PRICE_ACTION") and ((direction=="UP" and near_support) or (direction=="DOWN" and near_resistance)):s+=10
        if strategy=="REVERSAL" and ((direction=="UP" and rr<35 and bullish_rejection) or (direction=="DOWN" and rr>65 and bearish_rejection)):s+=18
        if strategy=="MEAN_REVERSION" and ((direction=="UP" and rr<30) or (direction=="DOWN" and rr>70)):s+=18
        if strategy=="MOMENTUM" and ((direction=="UP" and momentum>aa*.25) or (direction=="DOWN" and momentum<-aa*.25)):s+=14
        if strategy=="VOLATILITY" and volatility_ratio>=1.15:s+=10
        if strategy=="PRICE_ACTION" and ((direction=="UP" and body>0) or (direction=="DOWN" and body<0)):s+=8
        return round(min(100,s),2)

    # Market-derived self-strategy discovery. This is an additive layer:
    # the original evidence scoring remains intact; the discovered profile only
    # boosts a strategy when the current closed-candle regime supports it.
    self_profile=discover_self_strategy(
        trend=trend,structure=s1,rsi_value=rr,momentum=momentum,atr_value=aa,
        volatility_ratio=volatility_ratio,breakout_up=breakout_up,
        breakout_down=breakout_down,near_support=near_support,
        near_resistance=near_resistance,pattern=pattern
    )
    candidates=[]
    specs=[
        ("TREND_FOLLOWING",5),("MOMENTUM",2),("PULLBACK",2),("BREAKOUT",1),
        ("REVERSAL",3),("MEAN_REVERSION",3),("PRICE_ACTION",3),("VOLATILITY",4)
    ]
    for strategy,default_expiry in specs:
        for direction in ("UP","DOWN"):
            sc=score(direction,strategy)
            if strategy==self_profile["strategy"]:
                sc=min(100.0,sc+self_profile["strength"])
            if sc>=45:
                candidates.append({"direction":direction,"strategy":strategy,"score":round(sc,2),
                                   "expiry_minutes":default_expiry,"self_strategy_version":self_profile["version"]})

    # Higher-timeframe conflict is a rejection, not an automatic opposite signal.
    valid=[x for x in candidates if not (trend=="UP" and x["direction"]=="DOWN")
           and not (trend=="DOWN" and x["direction"]=="UP")]
    if not valid:return None
    best=max(valid,key=lambda x:x["score"])
    if best["score"]<78:return None

    confidence=min(99,int(best["score"]+8))
    reason=(f"{best['strategy']} | 15m={trend} | 1m={s1} | RSI={rr:.1f} | "
            f"momentum={momentum:.6g} | ATR={aa:.6g} | support={support:.6g} | "
            f"resistance={resistance:.6g} | pattern={pattern} | volatility={volatility_ratio:.2f}")
    return {
        "pair":str(asset.get("pair","")),
        "display_name":str(asset.get("display_name") or asset.get("title") or ""),
        "direction":best["direction"],"confidence":confidence,
        "strategy":best["strategy"],"expiry_minutes":best["expiry_minutes"],
        "self_strategy":self_profile["strategy"],"self_strategy_strength":self_profile["strength"],
        "pattern":pattern,"trend_15m":trend,"structure_1m":s1,
        "market_quality":best["score"],"reason":reason,
        "self_strategy_version":best.get("self_strategy_version",""),
        "entry_candle_ts":last["time"],"price":p,
        "support":support,"resistance":resistance,"atr":aa,
        "momentum":momentum,"volatility_ratio":volatility_ratio,
        "evidence":{"trend":trend,"structure":s1,"momentum":momentum,
                    "volatility":volatility_ratio,"breakout_up":breakout_up,
                    "breakout_down":breakout_down,"near_support":near_support,
                    "near_resistance":near_resistance}
    }
