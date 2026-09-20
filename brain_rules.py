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
    self_strategy:str=""
    self_strategy_version:str=""

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
    self_strategy_stats:dict[str,dict[str,float]]=field(default_factory=dict)
    expiry_stats:dict[int,dict[str,float]]=field(default_factory=dict)
    # Pattern/context memory is a second-stage learning layer. It only
    # influences ranking after enough observations exist, so early outcomes
    # cannot swing the live selector.
    pattern_stats:dict[str,dict[str,float]]=field(default_factory=dict)
    context_stats:dict[str,dict[str,float]]=field(default_factory=dict)
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
            structure_1m=str(kw.get("structure_1m","")),self_strategy=str(kw.get("self_strategy","")),self_strategy_version=str(kw.get("self_strategy_version","")))
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
        self_strategy=str(rec.get("self_strategy","") or "UNKNOWN")
        self._record_bucket(self._bucket(self.self_strategy_stats,self_strategy),result,weight)
        self._record_bucket(self._bucket(self.expiry_stats,expiry),result,weight)
        key=(pair,strategy,str(rec.get("direction","")),expiry)
        self._record_bucket(self._bucket(self.stats,key),result,weight)

        # Keep pattern/context evidence separate from the existing learning
        # layer. These features are recorded immediately but gated before they
        # can affect the live ranking.
        pattern=str(rec.get("pattern","") or "UNKNOWN")
        trend=str(rec.get("trend_15m","") or "UNKNOWN")
        structure=str(rec.get("structure_1m","") or "UNKNOWN")
        self._record_bucket(self._bucket(self.pattern_stats,pattern),result,weight)
        self._record_bucket(self._bucket(self.context_stats,f"{trend}|{structure}"),result,weight)
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
            "structure_1m":s.structure_1m,"self_strategy":s.self_strategy,
            "self_strategy_version":s.self_strategy_version,"reason":s.reason,
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

    def learning_bonus(self,pair,strategy,expiry,direction="",pattern="",trend_15m="",structure_1m=""):
        """Return a bounded learned adjustment; conservative until evidence exists."""
        vals=[]
        for key in (
            (pair,strategy,direction,expiry),
        ):
            b=self.stats.get(key)
            if b and b.get("n",0)>=2: vals.append(float(b.get("weighted",0)))
        # Do not inject global expiry-only learning here. It was the
        # cross-strategy source of 5m drift. Expiry learning is already represented
        # by the pair+strategy+direction bucket above.
        for store,key in ((self.asset_stats,pair),(self.strategy_stats,strategy),(self.self_strategy_stats,strategy)):
            b=store.get(key)
            if b and b.get("n",0)>=2: vals.append(float(b.get("weighted",0)))

        base_bonus=(sum(vals)/max(1,len(vals))*1.5) if vals else 0.0

        # Pattern/context evidence is deliberately gated at 8 observations.
        # A few wins/losses must not reshape live selection.
        def gated_rate_bonus(store,key,scale=2.0):
            b=store.get(key) if key else None
            n=float(b.get("n",0) or 0) if b else 0.0
            if n<8:return 0.0
            rate=self._rate(b)
            return max(-scale,min(scale,(rate-0.5)*2*scale))

        pattern_bonus=gated_rate_bonus(self.pattern_stats,str(pattern or ""),2.0)
        context_bonus=gated_rate_bonus(
            self.context_stats,
            f"{trend_15m or 'UNKNOWN'}|{structure_1m or 'UNKNOWN'}",
            2.0
        )
        return max(-8.0,min(8.0,base_bonus+pattern_bonus+context_bonus))

    def choose_expiry(self,pair,strategy,direction,live_quality=0):
        """Choose expiry from strategy-local evidence only.

        A global expiry bucket used to push unrelated strategies toward 5m.
        That caused MOMENTUM candidates to inherit TREND_FOLLOWING-style
        5-minute exposure. Keep expiry selection inside a strategy-specific
        neighborhood and use pair/strategy/direction history when available.
        """
        base={"BREAKOUT":1,"PULLBACK":2,"REVERSAL":3,"MEAN_REVERSION":3,
              "MOMENTUM":2,"TREND_FOLLOWING":5,"PRICE_ACTION":3,
              "VOLATILITY":4}.get(strategy,3)
        allowed={
            "BREAKOUT":(1,2),
            "MOMENTUM":(1,2,3),
            "PULLBACK":(1,2,3),
            "TREND_FOLLOWING":(3,5),
            "REVERSAL":(2,3,4),
            "MEAN_REVERSION":(2,3,4),
            "PRICE_ACTION":(2,3,4),
            "VOLATILITY":(3,4,5),
        }.get(strategy,(2,3,4))
        candidates=[]
        for e in allowed:
            pair_bucket=self.stats.get((str(pair),str(strategy),str(direction).upper(),int(e)))
            score=-abs(e-base)*1.25
            if pair_bucket and float(pair_bucket.get("n",0) or 0)>=2:
                n=float(pair_bucket.get("n",0) or 0)
                weighted=float(pair_bucket.get("weighted",0) or 0)
                score += (weighted/max(n,1.0))*2.5

            # Strategy-wide evidence is only a weak tie-breaker. It cannot move
            # a candidate across unrelated strategy expiry ranges.
            total_n=0.0
            total_weighted=0.0
            for (p,s,d,ee),b in self.stats.items():
                if str(s)==str(strategy) and str(d).upper()==str(direction).upper() and int(ee)==int(e):
                    total_n += float(b.get("n",0) or 0)
                    total_weighted += float(b.get("weighted",0) or 0)
            if total_n>=3:
                score += (total_weighted/total_n)*0.9

            # Strong live evidence can prefer the longer member of the local
            # strategy neighborhood, but never creates a new 5m path.
            if live_quality>=90 and e>base:
                score += 0.20
            elif live_quality<82 and e>base:
                score -= 0.25
            candidates.append((score,e))
        return max(candidates,key=lambda z:z[0])[1]

    def adaptive_candidate(self,c):
        x=dict(c)
        pair=str(x.get("pair","")); strategy=str(x.get("strategy",""))
        direction=str(x.get("direction","")).upper()
        self_strategy=str(x.get("self_strategy") or strategy)
        x["learning_bonus"]=round(self.learning_bonus(
            pair,strategy,int(x.get("expiry_minutes") or 0),direction,
            str(x.get("pattern") or ""),str(x.get("trend_15m") or ""),
            str(x.get("structure_1m") or "")
        ),2)
        self_bucket=self.self_strategy_stats.get(self_strategy,{})
        self_n=float(self_bucket.get("n",0) or 0)
        self_bonus=0.0
        if self_n>=2:
            self_bonus=max(-4.0,min(4.0,float(self_bucket.get("weighted",0) or 0)*1.25))
        x["self_learning_bonus"]=round(self_bonus,2)
        x["market_quality"]=max(0.0,min(100.0,float(x.get("market_quality") or 0)+x["learning_bonus"]+self_bonus))
        x["confidence"]=max(0,min(99,int(x.get("confidence") or 0)+int(round(x["learning_bonus"]+self_bonus))))
        if pair and strategy:
            x["expiry_minutes"]=self.choose_expiry(pair,strategy,direction,float(x.get("market_quality") or 0))
        return x


    def export_learning(self):
        return {
            "total_results": self.total_results,
            "asset_stats": self.asset_stats,
            "strategy_stats": self.strategy_stats,
            "self_strategy_stats": self.self_strategy_stats,
            "expiry_stats": {str(k): v for k, v in self.expiry_stats.items()},
            "stats": {"|".join(map(str,k)): v for k,v in self.stats.items()},
            "pattern_stats": self.pattern_stats,
            "context_stats": self.context_stats,
        }

    def import_learning(self, data):
        if not isinstance(data, dict): return
        self.total_results=int(data.get("total_results",0) or 0)
        self.asset_stats=dict(data.get("asset_stats") or {})
        self.strategy_stats=dict(data.get("strategy_stats") or {})
        self.self_strategy_stats=dict(data.get("self_strategy_stats") or {})
        self.expiry_stats={int(k):v for k,v in (data.get("expiry_stats") or {}).items()}
        rebuilt={}
        for k,v in (data.get("stats") or {}).items():
            parts=str(k).split("|",3)
            if len(parts)==4: rebuilt[(parts[0],parts[1],parts[2],int(parts[3]))]=v
        self.stats=rebuilt
        self.pattern_stats=dict(data.get("pattern_stats") or {})
        self.context_stats=dict(data.get("context_stats") or {})

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
