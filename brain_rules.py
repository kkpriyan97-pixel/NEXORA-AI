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
    account_id:int|None
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
    indicator_context:dict[str,Any]=field(default_factory=dict)

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
    indicator_stats:dict[str,dict[str,float]]=field(default_factory=dict)
    total_results:int=0
    learning_account_id:int|None=None
    batch_results:list[dict[str,Any]]=field(default_factory=list)
    learning_batch_no:int=0
    account_cooldown_until:float=0.0
    last_batch_summary:dict[str,Any]|None=None
    # Post-result AI lessons are stored separately from raw outcome statistics.
    post_result_lessons:dict[str,dict[str,Any]]=field(default_factory=dict)
    last_ai_review:dict[str,Any]|None=None

    def bind_learning_account(self,account_id):
        try: account_id=int(account_id) if account_id is not None else None
        except (TypeError,ValueError): account_id=None
        if account_id != self.learning_account_id:
            self.learning_account_id=account_id
            self.batch_results.clear(); self.last_batch_summary=None
            self.account_cooldown_until=0.0; self.learning_batch_no=0

    def is_account_cooldown(self,now=None):
        now=utc_now() if now is None else float(now)
        return self.account_cooldown_until > now

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
        if self.is_account_cooldown(now): return []
        return [a for a in assets if a.get("pair") and not a.get("locked")
                and not a.get("locked_trading") and not a.get("disabled")
                and not self.is_in_cooldown(str(a["pair"]),now)]

    def can_send_cycle_signal(self,account_id=None):
        if self.is_account_cooldown(): return False
        if account_id is not None and self.learning_account_id is not None and int(account_id)!=int(self.learning_account_id): return False
        return not self.cycle_signal_sent

    def duplicate_key(self,pair,entry_candle_ts):
        return (str(pair),str(entry_candle_ts))

    def is_duplicate(self,pair,entry_candle_ts,direction=""):
        return self.duplicate_key(pair,entry_candle_ts) in self.sent_keys

    def mark_signal_sent(self,**kw):
        if not self.can_send_cycle_signal(kw.get("account_id")):
            raise RuntimeError("Final signal already sent for cycle")
        if int(kw.get("confidence",0)) < MIN_CONFIDENCE:
            raise ValueError("Confidence below 90")
        key=self.duplicate_key(kw["pair"],kw["entry_candle_ts"])
        if key in self.sent_keys:
            raise RuntimeError("Duplicate asset/entry candle")
        self.sent_keys.add(key)
        self.cycle_signal_sent=True
        s=ActiveSignal(
            cycle_id=self.cycle_id,account_id=(int(kw.get("account_id")) if kw.get("account_id") is not None else None),pair=str(kw["pair"]),display_name=str(kw["display_name"]),
            direction=str(kw["direction"]).upper(),expiry_minutes=int(kw["expiry_minutes"]),
            entry_price=float(kw["entry_price"]),entry_ts=float(kw["entry_ts"]),
            entry_candle_ts=kw["entry_candle_ts"],strategy=str(kw.get("strategy","")),
            reason=str(kw.get("reason","")),confidence=int(kw.get("confidence",0)),
            pattern=str(kw.get("pattern","")),trend_15m=str(kw.get("trend_15m","")),
            structure_1m=str(kw.get("structure_1m","")),self_strategy=str(kw.get("self_strategy","")),self_strategy_version=str(kw.get("self_strategy_version","")),
            indicator_context=dict(kw.get("indicator_context") or {}))
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
        try: rec_account=int(rec.get("account_id")) if rec.get("account_id") is not None else None
        except (TypeError,ValueError): rec_account=None
        if self.learning_account_id is None or rec_account != int(self.learning_account_id): return
        self.batch_results.append(dict(rec)); self.batch_results=self.batch_results[-10:]
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
        ind=dict(rec.get("indicator_context") or {})
        dc_state=str(ind.get("donchian_state") or "UNKNOWN")
        dc_exp="EXPANDING" if bool(ind.get("donchian_expansion")) else "FLAT"
        st_cross=str(ind.get("stochastic_cross") or "NEUTRAL")
        st_zone=("OVERSOLD" if bool(ind.get("stochastic_oversold")) else
                 "OVERBOUGHT" if bool(ind.get("stochastic_overbought")) else "MID")
        indicator_key=f"DC:{dc_state}:{dc_exp}|ST:{st_zone}:{st_cross}"
        self._record_bucket(self._bucket(self.indicator_stats,indicator_key),result,weight)
        self.total_results+=1

        if len(self.batch_results) >= 10:
            self.learning_batch_no += 1
            batch=list(self.batch_results[-10:])
            wins=sum(1 for r in batch if r.get("result")=="WIN")
            losses=sum(1 for r in batch if r.get("result")=="LOSS")
            ties=sum(1 for r in batch if r.get("result")=="TIE")
            def grouped(field):
                out={}
                for r in batch:
                    key=str(r.get(field) or "UNKNOWN")
                    b=out.setdefault(key,{"n":0,"win":0,"loss":0,"tie":0})
                    b["n"]+=1; b[str(r.get("result","")).lower()]+=1
                return out
            strategies=grouped("strategy")
            self_strategies=grouped("self_strategy")
            expiries=grouped("expiry_minutes")
            assets=grouped("pair")
            def grouped_indicator(rows):
                out={}
                for r in rows:
                    ind=dict(r.get("indicator_context") or {})
                    key="DC:{}:{}|ST:{}:{}".format(
                        ind.get("donchian_state","UNKNOWN"),
                        "EXPANDING" if ind.get("donchian_expansion") else "FLAT",
                        "OVERSOLD" if ind.get("stochastic_oversold") else ("OVERBOUGHT" if ind.get("stochastic_overbought") else "MID"),
                        ind.get("stochastic_cross","NEUTRAL"))
                    b=out.setdefault(key,{"n":0,"win":0,"loss":0,"tie":0})
                    b["n"]+=1
                    b[str(r.get("result","")).lower()]+=1
                return out
            indicator_contexts=grouped_indicator(batch)
            lessons=[]
            for st,b in sorted(strategies.items(),key=lambda kv:(-kv[1]["n"],kv[0])):
                if b["n"]>=2:
                    lessons.append(f"{st}: {b['win']}W/{b['loss']}L")
            if not lessons:
                lessons.append("No strategy had 2+ observations; keep technical evidence unchanged.")
            self.last_batch_summary={
                "batch_no":self.learning_batch_no,"account_id":self.learning_account_id,
                "signals":10,"wins":wins,"losses":losses,"ties":ties,
                "win_rate":round(100*wins/10,1),"strategies":strategies,
                "self_strategies":self_strategies,"expiries":expiries,
                "assets":assets,"indicator_contexts":indicator_contexts,"lessons":lessons,"details":[{"pair":r.get("pair"),"direction":r.get("direction"),"strategy":r.get("strategy"),"self_strategy":r.get("self_strategy"),"expiry":r.get("expiry_minutes"),"confidence":r.get("confidence"),"result":r.get("result")} for r in batch],"cooldown_seconds":600
            }
            self.account_cooldown_until=utc_now()+600.0
            self.batch_results.clear()

    def consume_batch_summary(self):
        summary=self.last_batch_summary
        self.last_batch_summary=None
        return summary


    @staticmethod
    def lesson_key(rec):
        ind=dict(rec.get("indicator_context") or {})
        bb=str(ind.get("bollinger_signal") or "UNKNOWN").upper()
        bb_pos=str(ind.get("bollinger_position") or "UNKNOWN").upper()
        dc=str(ind.get("donchian_state") or "UNKNOWN").upper()
        st=str(ind.get("stochastic_cross") or "UNKNOWN").upper()
        return "|".join([
            str(rec.get("pair") or ""),
            str(rec.get("direction") or "").upper(),
            str(rec.get("strategy") or "UNKNOWN"),
            str(rec.get("trend_15m") or "UNKNOWN").upper(),
            str(rec.get("structure_1m") or "UNKNOWN").upper(),
            f"BB:{bb}:{bb_pos}", f"DC:{dc}", f"ST:{st}"
        ])

    def apply_ai_review(self,rec,review):
        """Persist a bounded post-result AI lesson for reuse on the same setup."""
        if not isinstance(review,dict): return
        key=self.lesson_key(rec)
        result=str(rec.get("result") or "").upper()
        if result not in {"WIN","LOSS","TIE"}: return
        b=self.post_result_lessons.setdefault(key,{"n":0.0,"win":0.0,"loss":0.0,"tie":0.0,"weighted":0.0,"reviews":[]})
        b[result.lower()]=float(b.get(result.lower(),0) or 0)+1.0
        b["n"]=float(b.get("n",0) or 0)+1.0
        b["weighted"]=float(b.get("weighted",0) or 0)+{"WIN":1.0,"LOSS":-1.0,"TIE":0.0}[result]
        lesson=str(review.get("lesson") or "").strip()[:320]
        action=str(review.get("reuse") or review.get("action") or "").strip()[:240]
        evidence=str(review.get("evidence") or "").strip()[:320]
        if lesson or action or evidence:
            b["reviews"]=(b.get("reviews") or [])[-4:]+[{"result":result,"lesson":lesson,"reuse":action,"evidence":evidence,"ai_provider":str(review.get("provider") or "internal")}]
        self.last_ai_review={"key":key,"pair":rec.get("pair"),"result":result,"lesson":lesson,"reuse":action,"evidence":evidence}

    def post_result_learning_bonus(self,rec):
        b=self.post_result_lessons.get(self.lesson_key(rec))
        if not b: return 0.0
        n=float(b.get("n",0) or 0)
        if n<=0: return 0.0
        return max(-3.0,min(3.0,(float(b.get("weighted",0) or 0)/n)*3.0))

    def finish_signal(self,key,exit_price,result_ts=None):
        s=self.active_signals.pop(key)
        result=self.classify_result(s.direction,s.entry_price,float(exit_price))
        now=utc_now() if result_ts is None else float(result_ts)
        rec={
            "cycle_id":s.cycle_id,"account_id":s.account_id,"pair":s.pair,"display_name":s.display_name,
            "direction":s.direction,"expiry_minutes":s.expiry_minutes,
            "entry_price":s.entry_price,"exit_price":float(exit_price),
            "entry_ts":s.entry_ts,"result_ts":now,"strategy":s.strategy,
            "pattern":s.pattern,"trend_15m":s.trend_15m,
            "structure_1m":s.structure_1m,"self_strategy":s.self_strategy,
            "self_strategy_version":s.self_strategy_version,"reason":s.reason,
            "confidence":s.confidence,"result":result,
            "indicator_context":s.indicator_context
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

    def learning_bonus(self,pair,strategy,expiry,direction="",pattern="",trend_15m="",structure_1m="",indicator_context=None):
        """Return a bounded learned adjustment without cross-strategy expiry drift."""
        vals=[]

        pair_bucket=self.stats.get((str(pair),str(strategy),str(direction).upper(),int(expiry)))
        if pair_bucket and float(pair_bucket.get("n",0) or 0)>=2:
            vals.append(float(pair_bucket.get("weighted",0) or 0))

        asset_bucket=self.asset_stats.get(str(pair))
        if asset_bucket and float(asset_bucket.get("n",0) or 0)>=2:
            vals.append(float(asset_bucket.get("weighted",0) or 0))

        # Strategy reliability is intentionally slow-moving. It can suppress a
        # strategy after repeated poor outcomes, but a handful of results cannot
        # dominate the live technical evidence.
        strategy_bonus=0.0
        sb=self.strategy_stats.get(str(strategy))
        sn=float(sb.get("n",0) or 0) if sb else 0.0
        if sb and sn>=5:
            rate=self._rate(sb)
            strategy_bonus=max(-8.0,min(3.0,(rate-0.5)*20.0))

        base_bonus=(sum(vals)/max(1,len(vals))*1.2) if vals else 0.0

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
        indicator_bonus=0.0
        # Exact indicator-combination learning only; never transfer one
        # successful setup blindly to unrelated indicator states.
        ind=dict(indicator_context or {})
        if ind:
            key="DC:{}:{}|ST:{}:{}".format(
                ind.get("donchian_state","UNKNOWN"),
                "EXPANDING" if ind.get("donchian_expansion") else "FLAT",
                "OVERSOLD" if ind.get("stochastic_oversold") else ("OVERBOUGHT" if ind.get("stochastic_overbought") else "MID"),
                ind.get("stochastic_cross","NEUTRAL"))
            b=self.indicator_stats.get(key)
            n=float(b.get("n",0) or 0) if b else 0.0
            if b and n>=10:
                rate=self._rate(b)
                indicator_bonus=max(-2.0,min(2.0,(rate-0.5)*4.0))
        return max(-8.0,min(8.0,base_bonus+strategy_bonus+pattern_bonus+context_bonus+indicator_bonus))

    def choose_expiry(self,pair,strategy,direction,live_quality=0,allow_5m=False):
        """Choose expiry from strategy-local evidence; 5m is a locked/rare path."""
        strategy=str(strategy)
        direction=str(direction).upper()
        base={
            "BREAKOUT":1,"PULLBACK":2,"REVERSAL":3,"MEAN_REVERSION":3,
            "MOMENTUM":2,"TREND_FOLLOWING":3,"PRICE_ACTION":3,"VOLATILITY":4
        }.get(strategy,3)
        allowed={
            "BREAKOUT":(1,2),
            "MOMENTUM":(1,2,3),
            "PULLBACK":(2,3),
            "TREND_FOLLOWING":(2,3,4),
            "REVERSAL":(2,3,4),
            "MEAN_REVERSION":(2,3,4),
            "PRICE_ACTION":(2,3,4),
            "VOLATILITY":(3,4),
        }.get(strategy,(2,3,4))

        # 5m is not a default. It is unlocked only when BOTH global history and
        # the exact pair+strategy+direction history are strong enough. This
        # prevents a handful of successful 5m trades on one pair from making
        # 5m dominate the whole selector again.
        if strategy=="TREND_FOLLOWING" and allow_5m and float(live_quality)>=92:
            global5=self.expiry_stats.get(5)
            gn=float(global5.get("n",0) or 0) if global5 else 0.0
            grate=self._rate(global5) if global5 else 0.0

            pair5=self.stats.get((str(pair),strategy,direction,5))
            pn=float(pair5.get("n",0) or 0) if pair5 else 0.0
            prate=self._rate(pair5) if pair5 else 0.0

            strat=self.strategy_stats.get(strategy)
            sn=float(strat.get("n",0) or 0) if strat else 0.0
            srate=self._rate(strat) if strat else 0.0

            # Conservative validation thresholds:
            #   20+ global 5m observations, >=60% smoothed win rate
            #   12+ exact pair/strategy/direction observations, >=60%
            #   12+ trend-following observations, >=55%
            # This keeps 5m disabled while the legacy 5m sample remains weak.
            if gn>=20 and grate>=0.60 and pn>=12 and prate>=0.60 and sn>=12 and srate>=0.55:
                allowed=tuple(list(allowed)+[5])

        candidates=[]
        for e in allowed:
            pair_bucket=self.stats.get((str(pair),strategy,direction,int(e)))
            score=-abs(e-base)*1.35
            if pair_bucket and float(pair_bucket.get("n",0) or 0)>=3:
                n=float(pair_bucket.get("n",0) or 0)
                weighted=float(pair_bucket.get("weighted",0) or 0)
                score += (weighted/max(n,1.0))*2.5

            total_n=0.0
            total_weighted=0.0
            for (p,s,d,ee),b in self.stats.items():
                if str(s)==strategy and str(d).upper()==direction and int(ee)==int(e):
                    total_n += float(b.get("n",0) or 0)
                    total_weighted += float(b.get("weighted",0) or 0)
            if total_n>=4:
                score += (total_weighted/total_n)*0.8

            if e==5:
                score += 0.30 if float(live_quality)>=94 else 0.0
            elif live_quality>=90 and e>base:
                score += 0.10
            elif live_quality<82 and e>base:
                score -= 0.20

            candidates.append((score,e))
        return max(candidates,key=lambda z:z[0])[1]
    def adaptive_candidate(self,c):
        x=dict(c)
        pair=str(x.get("pair",""))
        strategy=str(x.get("strategy",""))
        direction=str(x.get("direction","")).upper()
        self_strategy=str(x.get("self_strategy") or strategy)

        x["learning_bonus"]=round(self.learning_bonus(
            pair,strategy,int(x.get("expiry_minutes") or 0),direction,
            str(x.get("pattern") or ""),
            str(x.get("trend_15m") or ""),
            str(x.get("structure_1m") or "")
        ),2)

        # self_strategy_stats is intentionally not double-counted when the
        # router strategy equals the actual candidate strategy. The strategy
        # reliability term above is already the canonical learning signal.
        self_bucket=self.self_strategy_stats.get(self_strategy,{})
        self_n=float(self_bucket.get("n",0) or 0)
        self_bonus=0.0
        if self_strategy != strategy and self_n>=4:
            self_bonus=max(-2.0,min(2.0,float(self_bucket.get("weighted",0) or 0)*0.5))
        x["self_learning_bonus"]=round(self_bonus,2)

        preview=dict(x)
        preview["indicator_context"]=dict(x.get("indicators") or x.get("indicator_context") or {})
        lesson_rec={"pair":pair,"direction":direction,"strategy":strategy,
                    "trend_15m":str(x.get("trend_15m") or ""),
                    "structure_1m":str(x.get("structure_1m") or ""),
                    "indicator_context":preview["indicator_context"]}
        x["post_result_learning_bonus"]=round(self.post_result_learning_bonus(lesson_rec),2)

        technical=float(x.get("market_quality") or x.get("confidence") or 0)
        x["market_quality"]=max(
            0.0,
            min(100.0,technical+x["learning_bonus"]+self_bonus+x["post_result_learning_bonus"])
        )

        # Confidence is the resulting evidence quality; do not manufacture +8
        # points on top of a borderline technical score.
        x["confidence"]=max(0,min(99,int(round(x["market_quality"]))))

        if pair and strategy:
            allow_5m=bool(x.get("five_minute_eligible")) and strategy=="TREND_FOLLOWING"
            x["expiry_minutes"]=self.choose_expiry(
                pair,strategy,direction,float(x.get("market_quality") or 0),
                allow_5m=allow_5m
            )
        return x


    def export_learning(self):
        return {
            "total_results": self.total_results,
            "learning_account_id": self.learning_account_id,
            "batch_results": self.batch_results[-10:],
            "learning_batch_no": self.learning_batch_no,
            "account_cooldown_until": self.account_cooldown_until,
            "asset_stats": self.asset_stats,
            "strategy_stats": self.strategy_stats,
            "self_strategy_stats": self.self_strategy_stats,
            "expiry_stats": {str(k): v for k, v in self.expiry_stats.items()},
            "stats": {"|".join(map(str,k)): v for k,v in self.stats.items()},
            "pattern_stats": self.pattern_stats,
            "context_stats": self.context_stats,
            "indicator_stats": self.indicator_stats,
            "post_result_lessons": self.post_result_lessons,
            "last_ai_review": self.last_ai_review,
        }

    def import_learning(self, data):
        if not isinstance(data, dict): return
        self.total_results=int(data.get("total_results",0) or 0)
        try: self.learning_account_id=int(data.get("learning_account_id")) if data.get("learning_account_id") is not None else None
        except (TypeError,ValueError): self.learning_account_id=None
        self.batch_results=list(data.get("batch_results") or [])[-10:]
        self.learning_batch_no=int(data.get("learning_batch_no",0) or 0)
        self.account_cooldown_until=float(data.get("account_cooldown_until",0) or 0)
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
        self.indicator_stats=dict(data.get("indicator_stats") or {})
        self.post_result_lessons=dict(data.get("post_result_lessons") or {})
        self.last_ai_review=dict(data.get("last_ai_review") or {}) if data.get("last_ai_review") else None

    def prune_expired_cooldowns(self,now=None):
        now=utc_now() if now is None else now
        for p,u in list(self.cooldown_until.items()):
            if float(u)<=now:self.cooldown_until.pop(p,None)

def rank_signal_candidates(candidates):
    q=[x for x in candidates if int(x.get("confidence") or 0)>=MIN_CONFIDENCE
       and str(x.get("direction","")).upper() in {"UP","DOWN"}]
    return sorted(q,key=lambda x:(
        int(x.get("confidence") or 0),
        float(x.get("strategy_margin") or 0),
        float(x.get("direction_agreement") or 0),
        float(x.get("market_quality") or 0),
        float(x.get("learning_bonus") or 0),
        int(x.get("profitability") or 0)
    ),reverse=True)
