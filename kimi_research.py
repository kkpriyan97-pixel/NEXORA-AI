"""Research-only Kimi analyst for the M1 world-learning lab."""
from __future__ import annotations
import json, os, re, logging
import httpx
import time

log=logging.getLogger("candice.m1lab")
KIMI_MIN_INTERVAL_SECONDS=max(300.0,min(3600.0,float(os.getenv("KIMI_RESEARCH_MIN_INTERVAL_SECONDS","900"))))
_KIMI_NEXT_ALLOWED=0.0

def configured():
    return os.getenv("KIMI_RESEARCH_ENABLED","true").strip().lower() != "false" and bool(os.getenv("KIMI_RESEARCH_API_KEY","").strip())

async def analyze(stage, evidence):
    global _KIMI_NEXT_ALLOWED
    key=os.getenv("KIMI_RESEARCH_API_KEY","").strip()
    if not configured() or not evidence:
        return []
    now=time.time()
    if now < _KIMI_NEXT_ALLOWED:
        log.info("KIMI_RESEARCH_COOLDOWN remaining=%.1fs",_KIMI_NEXT_ALLOWED-now)
        return []
    _KIMI_NEXT_ALLOWED=now+KIMI_MIN_INTERVAL_SECONDS
    base=os.getenv("KIMI_RESEARCH_API_BASE","https://api.moonshot.ai/v1").strip().rstrip("/")
    model=os.getenv("KIMI_RESEARCH_MODEL","kimi-k2.6").strip()
    timeout=max(5.0,min(20.0,float(os.getenv("KIMI_RESEARCH_TIMEOUT","10"))))
    compact=[]
    for e in evidence[:18]:
        compact.append({"method_id":e.get("method_id"),"domain":e.get("domain"),"language":e.get("language"),"score":e.get("evidence_score"),"evidence":e.get("evidence")})
    prompt=("You are a research-only analyst for a DEMO M1 next-candle lab. Use only supplied evidence. "
            "Do not output a live UP/DOWN signal and do not modify any live technical Brain. "
            "Separate evidence from hypotheses. Treat marketing claims as unverified. Return JSON only with items[]. "
            "Each item: method,hypothesis,entry_context,no_trade_conditions,timeframe,required_data,validation_plan,confidence,language. "
            "Focus on closed-candle M1/M2 behavior, higher-timeframe regime, execution/latency controls, false-signal filters, "
            "and forward/out-of-sample validation. Never claim guaranteed profitability. " +
            "Stage="+str(stage)+" Evidence="+json.dumps(compact,ensure_ascii=False,separators=(",",":")))
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout,connect=min(3.0,timeout)),headers={"Authorization":"Bearer "+key,"Content-Type":"application/json"}) as h:
            r=await h.post(base+"/chat/completions",json={"model":model,"temperature":0,"messages":[{"role":"system","content":"Return JSON only. Research-only. Never trade."},{"role":"user","content":prompt}]})
            r.raise_for_status()
            raw=r.json(); choices=raw.get("choices") or []
            content=((choices[0].get("message") or {}).get("content") if choices else "") or ""
            s=str(content).strip()
            s=re.sub(r"^\s*```(?:json)?\s*|\s*```\s*$","",s,flags=re.I)
            data=json.loads(s)
            items=data.get("items") if isinstance(data,dict) else None
            return items if isinstance(items,list) else []
    except httpx.HTTPStatusError as exc:
        status=exc.response.status_code
        detail=exc.response.text[:160].replace("\n"," ")
        if status==429:
            retry_after=0.0
            try:
                retry_after=float(exc.response.headers.get("Retry-After","0") or 0)
            except (TypeError,ValueError):
                retry_after=0.0
            _KIMI_NEXT_ALLOWED=time.time()+max(KIMI_MIN_INTERVAL_SECONDS,retry_after)
            log.warning("KIMI_RESEARCH_429_BACKOFF retry_after=%s backoff_seconds=%.1f detail=%s",
                        retry_after,KIMI_MIN_INTERVAL_SECONDS,detail)
        else:
            log.warning("KIMI_RESEARCH_ERROR status=%s detail=%s",status,detail)
        return []
    except Exception as exc:
        log.warning("KIMI_RESEARCH_ERROR type=%s message=%s",type(exc).__name__,str(exc)[:180])
        return []
