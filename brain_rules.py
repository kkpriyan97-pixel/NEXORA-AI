"""Candice Brain state: one final signal per 5-minute cycle, overlapping result tracking."""
from __future__ import annotations
from dataclasses import dataclass,field
from datetime import datetime,timezone
from typing import Any
COOLDOWN_SECONDS=900;MIN_CONFIDENCE=90;CYCLE_SECONDS=300
def utc_now():return datetime.now(timezone.utc).timestamp()
@dataclass
class ActiveSignal:
    cycle_id:int;pair:str;display_name:str;direction:str;expiry_minutes:int;entry_price:float;entry_ts:float;entry_candle_ts:Any;strategy:str="";reason:str="";confidence:int=0;pattern:str="";trend_15m:str="";structure_1m:str=""
@dataclass
class BrainState:
    cycle_id:int=0;cycle_signal_sent:bool=False;sent_keys:set[tuple[str,str]]=field(default_factory=set);cooldown_until:dict[str,float]=field(default_factory=dict);active_signals:dict[str,ActiveSignal]=field(default_factory=dict);last_result:dict[str,Any]|None=None
    def start_cycle(self,cycle_id):
        if cycle_id!=self.cycle_id:self.cycle_id=cycle_id;self.cycle_signal_sent=False;self.sent_keys.clear()
    def is_in_cooldown(self,pair,now=None):
        now=utc_now() if now is None else now;u=float(self.cooldown_until.get(pair,0) or 0)
        if u<=now:self.cooldown_until.pop(pair,None);return False
        return True
    def filter_candidates(self,assets,now=None):
        now=utc_now() if now is None else now
        return [a for a in assets if a.get("pair") and not a.get("locked") and not a.get("locked_trading") and not self.is_in_cooldown(str(a["pair"]),now)]
    def can_send_cycle_signal(self):return not self.cycle_signal_sent
    def duplicate_key(self,pair,entry_candle_ts):return (str(pair),str(entry_candle_ts))
    def is_duplicate(self,pair,entry_candle_ts,direction=""):return self.duplicate_key(pair,entry_candle_ts) in self.sent_keys
    def mark_signal_sent(self,**kw):
        if not self.can_send_cycle_signal():raise RuntimeError("Final signal already sent for cycle")
        if int(kw.get("confidence",0))<MIN_CONFIDENCE:raise ValueError("Confidence below 90")
        key=self.duplicate_key(kw["pair"],kw["entry_candle_ts"])
        if key in self.sent_keys:raise RuntimeError("Duplicate asset/entry candle")
        self.sent_keys.add(key);self.cycle_signal_sent=True
        s=ActiveSignal(cycle_id=self.cycle_id,pair=str(kw["pair"]),display_name=str(kw["display_name"]),direction=str(kw["direction"]).upper(),expiry_minutes=int(kw["expiry_minutes"]),entry_price=float(kw["entry_price"]),entry_ts=float(kw["entry_ts"]),entry_candle_ts=kw["entry_candle_ts"],strategy=str(kw.get("strategy","")),reason=str(kw.get("reason","")),confidence=int(kw.get("confidence",0)),pattern=str(kw.get("pattern","")),trend_15m=str(kw.get("trend_15m","")),structure_1m=str(kw.get("structure_1m","")))
        self.active_signals[f"{s.cycle_id}:{s.pair}:{s.entry_ts}"]=s
        return s
    @staticmethod
    def classify_result(direction,entry,exit_price):
        if float(exit_price)==float(entry):return "TIE"
        if str(direction).upper()=="UP":return "WIN" if float(exit_price)>float(entry) else "LOSS"
        if str(direction).upper()=="DOWN":return "WIN" if float(exit_price)<float(entry) else "LOSS"
        raise ValueError("Invalid direction")
    def finish_signal(self,key,exit_price,result_ts=None):
        s=self.active_signals.pop(key);result=self.classify_result(s.direction,s.entry_price,float(exit_price));now=utc_now() if result_ts is None else float(result_ts)
        rec={"cycle_id":s.cycle_id,"pair":s.pair,"display_name":s.display_name,"direction":s.direction,"expiry_minutes":s.expiry_minutes,"entry_price":s.entry_price,"exit_price":float(exit_price),"entry_ts":s.entry_ts,"result_ts":now,"strategy":s.strategy,"pattern":s.pattern,"trend_15m":s.trend_15m,"structure_1m":s.structure_1m,"reason":s.reason,"confidence":s.confidence,"result":result}
        if result=="LOSS":self.cooldown_until[s.pair]=now+COOLDOWN_SECONDS
        self.last_result=rec;return rec
    def prune_expired_cooldowns(self,now=None):
        now=utc_now() if now is None else now
        for p,u in list(self.cooldown_until.items()):
            if float(u)<=now:self.cooldown_until.pop(p,None)
def rank_signal_candidates(candidates):
    q=[x for x in candidates if int(x.get("confidence") or 0)>=MIN_CONFIDENCE and str(x.get("direction","")).upper() in {"UP","DOWN"}]
    return sorted(q,key=lambda x:(int(x.get("confidence") or 0),float(x.get("market_quality") or 0),int(x.get("profitability") or 0)),reverse=True)
