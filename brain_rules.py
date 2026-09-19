"""Candice Brain state, learning, cooldowns, duplicate protection and adaptive scoring."""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from collections import defaultdict
from typing import Any

COOLDOWN_SECONDS = 900
MIN_CONFIDENCE = 90
CYCLE_SECONDS = 300
EXPIRIES = (1, 2, 3, 4, 5, 10, 15)

def utc_now():
    return datetime.now(timezone.utc).timestamp()

@dataclass
class ActiveSignal:
    cycle_id:int
    pair:str
    display_name:str
    direction:str
    expiry_minutes:int
    entry_price:float
    entry_ts:float
    entry_candle_ts:Any
    strategy:str=""
    reason:str=""
    confidence:int=0
    pattern:str=""
    trend_15m:str=""
    structure_1m:str=""

@dataclass
class BrainState:
    cycle_id:int=0
    cycle_signal_sent:bool=False
    sent_keys:set[tuple[str,str]]=field(default_factory=set)
    cooldown_until:dict[str,float]=field(default_factory=dict)
    active_signals:dict[str,ActiveSignal]=field(default_factory=dict)
    last_result:dict[str,Any]|None=None
    # In-process learned memory. It is deliberately separate from raw market data.
    # Recent outcomes receive higher weight; WIN/LOSS/TIE all update memory.
    stats:dict[tuple[str,str,str,int],dict[str,float]]=field(default_factory=dict)
    asset_stats:dict[str,dict[str,float]]=field(default_factory=dict)
    strategy_stats:dict[str,dict[str,float]]=field(default_factory=dict)
    expiry_stats:dict[int,dict[str,float]]=field(default_factory=dict)
    total_results:int=0

    def start_cycle(self,cycle_id):
        if cycle_id != self.cycle_id:
            self.cycle_id=cycle_id
            self.cycle_signal_sent=False
            self.sent_keys.clear()

    def is_in_cooldown(self,pair,now=None):
        now=utc_now() if now is None else now
        u=float(self.cooldown_until.get(str(pair),0) or 0)
        if u<=now:
            self.cooldown_until.pop(str(pair),None)
            return False
        return True

    def filter_candidates(self,assets,now=None):
        now=utc_now() if now is None else now
        return [a for a in assets if a.get("pair") and not a.get("locked")
                and not a.get("locked_trading") and not a.get("disabled")
                and not self.is_in_cooldown(str(a["pair"]),now)]

    def can_send_cycle_signal(self):
        return not self.cycle_signal_sent

    def duplicate_key(self,pair,entry_candle_ts):
        return (str(pair),str(entry_candle_ts))

    def is_duplicate(self,pair,entry_candle_ts,direction=""):
        return self.duplicate_key(pair,entry_candle_ts) in self.sent_keys

    def mark_signal_sent(self,**kw):
        if not self.can_send_cycle_signal():
            raise RuntimeError("Final signal already sent for cycle")
        if int(kw.get("confidence",0)) < MIN_CONFIDENCE:
            raise ValueError("Confidence below 90")
        key=self.duplicate_key(kw["pair"],kw["entry_candle_ts"])
        if key in self.sent_keys:
            raise RuntimeError("Duplicate asset/entry candle")
        self.sent_keys.add(key)
        self.cycle_signal_sent=True
        s=ActiveSignal(
            cycle_id=self.cycle_id,pair=str(kw["pair"]),display_name=str(kw["display_name"]),
            direction=str(kw["direction"]).upper(),expiry_minutes=int(kw["expiry_minutes"]),
            entry_price=float(kw["entry_price"]),entry_ts=float(kw["entry_ts"]),
            entry_candle_ts=kw["entry_candle_ts"],strategy=str(kw.get("strategy","")),
            reason=str(kw.get("reason","")),confidence=int(kw.get("confidence",0)),
            pattern=str(kw.get("pattern","")),trend_15m=str(kw.get("trend_15m","")),
            structure_1m=str(kw.get("structure_1m","")))
        self.active_signals[f"{s.cycle_id}:{s.pair}:{s.entry_ts}"]=s
        return s

    @staticmethod
    def classify_result(direction,entry,exit_price):
        if float(exit_price)==float(entry): return "TIE"
        if str(direction).upper()=="UP":
            return "WIN" if float(exit_price)>float(entry) else "LOSS"
        if str(direction).upper()=="DOWN":
            return "WIN" if float(exit_price)<float(entry) else "LOSS"
        raise ValueError("Invalid direction")

    def _bucket(self,store,key):
        b=store.setdefault(key,{"win":0.0,"loss":0.0,"tie":0.0,"n":0.0,"weighted":0.0})
        return b

    def _record_bucket(self,b,result,weight):
        b[str(result).lower()]+=1.0
        b["n"]+=1.0
        # WIN is positive, LOSS negative, TIE neutral; recency is encoded by weight.
        b["weighted"] += {"WIN":weight,"LOSS":-weight,"TIE":0.0}.get(result,0.0)

    def learn(self,rec):
        result=str(rec.get("result","")).upper()
        pair=str(rec.get("pair",""))
        strategy=str(rec.get("strategy","") or "UNKNOWN")
        expiry=int(rec.get("expiry_minutes") or 0)
        if result not in {"WIN","LOSS","TIE"} or not pair:
            return
        # Exponential recency weighting without retaining raw candle history.
        weight=max(0.25,0.985 ** min(self.total_results,200))
        self._record_bucket(self._bucket(self.asset_stats,pair),result,weight)
        self._record_bucket(self._bucket(self.strategy_stats,strategy),result,weight)
        self._record_bucket(self._bucket(self.expiry_stats,expiry),result,weight)
        key=(pair,strategy,str(rec.get("direction","")),expiry)
        self._record_bucket(self._bucket(self.stats,key),result,weight)
        self.total_results+=1

    def finish_signal(self,key,exit_price,result_ts=None):
        s=self.active_signals.pop(key)
        result=self.classify_result(s.direction,s.entry_price,float(exit_price))
        now=utc_now() if result_ts is None else float(result_ts)
        rec={
            "cycle_id":s.cycle_id,"pair":s.pair,"display_name":s.display_name,
            "direction":s.direction,"expiry_minutes":s.expiry_minutes,
            "entry_price":s.entry_price,"exit_price":float(exit_price),
            "entry_ts":s.entry_ts,"result_ts":now,"strategy":s.strategy,
            "pattern":s.pattern,"trend_15m":s.trend_15m,
            "structure_1m":s.structure_1m,"reason":s.reason,
            "confidence":s.confidence,"result":result
        }
        self.learn(rec)
        if result=="LOSS":
            self.cooldown_until[s.pair]=now+COOLDOWN_SECONDS
        self.last_result=rec
        return rec

    def _rate(self,b):
        n=float(b.get("n",0) or 0)
        if not n:return 0.0
        # Bayesian smoothing prevents a single outcome from dominating.
        return (float(b.get("win",0))+0.5)/(n+1.0)

    def learning_bonus(self,pair,strategy,expiry,direction=""):
        """Return a bounded learned adjustment; neutral until evidence exists."""
        vals=[]
        for key in (
            (pair,strategy,direction,expiry),
        ):
            b=self.stats.get(key)
            if b and b.get("n",0)>=2: vals.append(float(b.get("weighted",0)))
        for store,key in ((self.asset_stats,pair),(self.strategy_stats,strategy),(self.expiry_stats,expiry)):
            b=store.get(key)
            if b and b.get("n",0)>=2: vals.append(float(b.get("weighted",0)))
        if not vals:return 0.0
        # Keep learning subordinate to live technical evidence.
        return max(-8.0,min(8.0,sum(vals)/max(1,len(vals))*1.5))

    def choose_expiry(self,pair,strategy,direction,live_quality=0):
        """Select among 1/2/3/4/5/10/15m using evidence plus live condition."""
        base={"BREAKOUT":1,"PULLBACK":2,"REVERSAL":3,"MEAN_REVERSION":3,
              "MOMENTUM":2,"TREND_FOLLOWING":5,"PRICE_ACTION":3}.get(strategy,3)
        candidates=[]
        for e in EXPIRIES:
            b=self.expiry_stats.get(e,{})
            n=float(b.get("n",0) or 0)
            learned=float(b.get("weighted",0) or 0) if n>=2 else 0.0
            distance=abs(e-base)
            score=learned - distance*0.7
            # Low live quality favors shorter exposure; strong quality permits longer.
            if live_quality>=90: score += min(e,5)*0.25
            elif live_quality<82: score -= max(0,e-3)*0.5
            candidates.append((score,e))
        return max(candidates)[1]

    def adaptive_candidate(self,c):
        x=dict(c)
        pair=str(x.get("pair","")); strategy=str(x.get("strategy",""))
        direction=str(x.get("direction","")).upper()
        x["learning_bonus"]=round(self.learning_bonus(pair,strategy,int(x.get("expiry_minutes") or 0),direction),2)
        x["market_quality"]=max(0.0,min(100.0,float(x.get("market_quality") or 0)+x["learning_bonus"]))
        x["confidence"]=max(0,min(99,int(x.get("confidence") or 0)+int(round(x["learning_bonus"]))))
        if pair and strategy:
            x["expiry_minutes"]=self.choose_expiry(pair,strategy,direction,float(x.get("market_quality") or 0))
        return x

    def prune_expired_cooldowns(self,now=None):
        now=utc_now() if now is None else now
        for p,u in list(self.cooldown_until.items()):
            if float(u)<=now:self.cooldown_until.pop(p,None)

def rank_signal_candidates(candidates):
    q=[x for x in candidates if int(x.get("confidence") or 0)>=MIN_CONFIDENCE
       and str(x.get("direction","")).upper() in {"UP","DOWN"}]
    return sorted(q,key=lambda x:(
        int(x.get("confidence") or 0),
        float(x.get("market_quality") or 0),
        float(x.get("learning_bonus") or 0),
        int(x.get("profitability") or 0)
    ),reverse=True)
