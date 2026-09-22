"""Research-only Kimi analyst for the M1 world-learning lab."""
from __future__ import annotations
import json, os, re, logging
import httpx
import time

log=logging.getLogger("candice.m1lab")
KIMI_MIN_INTERVAL_SECONDS=max(300.0,min(3600.0,float(os.getenv("KIMI_RESEARCH_MIN_INTERVAL_SECONDS","900"))))
_KIMI_NEXT_ALLOWED=0.0

DEFAULT_RESEARCH_FALLBACKS=("GEMINI","GROQ","OPENROUTER","MISTRAL","OPENAI")
KIMI_PRIMARY_ENABLED=os.getenv("KIMI_RESEARCH_PRIMARY_ENABLED","true").strip().lower()!="false"

def _fallback_providers():
    raw=os.getenv("AI_RESEARCH_FALLBACK_PROVIDERS",",".join(DEFAULT_RESEARCH_FALLBACKS))
    out=[]
    for name in raw.split(","):
        name=name.strip().upper()
        if name and name not in out:
            out.append(name)
    return out

def _provider_configured(name):
    name=str(name or "").strip().upper()
    key=os.getenv(f"{name}_API_KEY","").strip()
    if not key and name=="OPENAI": key=os.getenv("OPENAI_API_KEY","").strip()
    if not key and name=="OPENROUTER": key=os.getenv("OPENROUTER_API_KEY","").strip()
    if not key and name=="GEMINI": key=os.getenv("GEMINI_API_KEY","").strip()
    if not key and name=="GROQ": key=os.getenv("GROQ_API_KEY","").strip()
    if not key and name=="NVIDIA": key=os.getenv("NVIDIA_API_KEY","").strip() or os.getenv("NVIDIA_NIM_API_KEY","").strip()
    base=os.getenv(f"{name}_BASE_URL","").strip().rstrip("/")
    if not base:
        base={"OPENAI":"https://api.openai.com/v1","GEMINI":"https://generativelanguage.googleapis.com/v1beta/openai","GROQ":"https://api.groq.com/openai/v1","NVIDIA":"https://integrate.api.nvidia.com/v1","OPENROUTER":"https://openrouter.ai/api/v1","MISTRAL":"https://api.mistral.ai/v1"}.get(name,"")
    model=os.getenv(f"{name}_MODEL","").strip()
    if not model:
        model={"GEMINI":"gemini-3.8-flash","GROQ":"openai/gpt-oss-20b","NVIDIA":"openai/gpt-oss-20b","OPENROUTER":"openrouter/free","MISTRAL":"mistral-small-latest"}.get(name,"")
    if not model: model=os.getenv("AI_MODEL","").strip()
    return (base,model,key) if base and model and key else None

def primary_enabled():
    return KIMI_PRIMARY_ENABLED

def provider_configured():
    return bool(os.getenv("KIMI_RESEARCH_ENABLED","true").strip().lower()!="false" and os.getenv("KIMI_RESEARCH_API_KEY","").strip())

def fallback_configured():
    return bool(os.getenv("KIMI_RESEARCH_ENABLED","true").strip().lower()!="false" and any(_provider_configured(n) for n in _fallback_providers()))

def configured():
    return bool(os.getenv("KIMI_RESEARCH_ENABLED","true").strip().lower()!="false" and (provider_configured() or fallback_configured()))

async def _ask(base, model, key, prompt, timeout):
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout,connect=min(3.0,timeout)),headers={"Authorization":"Bearer "+key,"Content-Type":"application/json"}) as h:
        r=await h.post(base+"/chat/completions",json={"model":model,"temperature":0,"messages":[{"role":"system","content":"Return JSON only. Research-only. Never trade."},{"role":"user","content":prompt}]})
        r.raise_for_status()
        raw=r.json(); choices=raw.get("choices") or []
        content=((choices[0].get("message") or {}).get("content") if choices else "") or ""
        s=str(content).strip()
        s=re.sub(r"^\s*```(?:json)?\s*|\s*```\s*$","",s,flags=re.I)
        try: data=json.loads(s)
        except json.JSONDecodeError:
            start=s.find("{"); end=s.rfind("}")
            if start<0 or end<=start: return []
            try: data=json.loads(s[start:end+1])
            except json.JSONDecodeError: return []
        items=data.get("items") if isinstance(data,dict) else None
        return items if isinstance(items,list) else []

async def analyze(stage, evidence):
    global _KIMI_NEXT_ALLOWED
    if not configured() or not evidence: return []
    compact=[]
    for e in evidence[:18]:
        compact.append({"method_id":e.get("method_id"),"domain":e.get("domain"),"language":e.get("language"),"score":e.get("evidence_score"),"evidence":e.get("evidence")})
    if not primary_enabled():
        log.info("KIMI_RESEARCH_PRIMARY_DISABLED reason=provider_balance_issue fallback=ENABLED")
    prompt=("You are a research-only analyst for a DEMO M1 next-candle lab. Use only supplied evidence. "
            "Do not output a live UP/DOWN signal and do not modify any live technical Brain. "
            "Separate evidence from hypotheses. Treat marketing claims as unverified. Return JSON only with items[]. "
            "Each item: method,hypothesis,entry_context,no_trade_conditions,timeframe,required_data,validation_plan,confidence,language. "
            "Focus on closed-candle M1/M2 behavior, higher-timeframe regime, execution/latency controls, false-signal filters, "
            "and forward/out-of-sample validation. Never claim guaranteed profitability. "
            "Stage="+str(stage)+" Evidence="+json.dumps(compact,ensure_ascii=False,separators=(",",":")))
    timeout=max(5.0,min(20.0,float(os.getenv("KIMI_RESEARCH_TIMEOUT","10"))))
    key=os.getenv("KIMI_RESEARCH_API_KEY","").strip()
    now=time.time()
    if primary_enabled() and provider_configured() and now>=_KIMI_NEXT_ALLOWED:
        _KIMI_NEXT_ALLOWED=now+KIMI_MIN_INTERVAL_SECONDS
        base=os.getenv("KIMI_RESEARCH_API_BASE","https://api.moonshot.ai/v1").strip().rstrip("/")
        model=os.getenv("KIMI_RESEARCH_MODEL","kimi-k2.6").strip()
        try:
            items=await _ask(base,model,key,prompt,timeout)
            if items:
                log.info("KIMI_RESEARCH_RESULT provider=KIMI model=%s items=%d",model,len(items))
                return items
            log.warning("KIMI_RESEARCH_EMPTY model=%s fallback=ENABLED",model)
        except httpx.HTTPStatusError as exc:
            status=exc.response.status_code; detail=exc.response.text[:160].replace("\n"," ")
            if status==429:
                retry_after=0.0
                try: retry_after=float(exc.response.headers.get("Retry-After","0") or 0)
                except (TypeError,ValueError): retry_after=0.0
                _KIMI_NEXT_ALLOWED=time.time()+max(KIMI_MIN_INTERVAL_SECONDS,retry_after)
                log.warning("KIMI_RESEARCH_PROVIDER_PAUSED status=429 fallback=ENABLED backoff_seconds=%.1f detail=%s",KIMI_MIN_INTERVAL_SECONDS,detail)
            else:
                log.warning("KIMI_RESEARCH_ERROR status=%s fallback=ENABLED detail=%s",status,detail)
        except Exception as exc:
            log.warning("KIMI_RESEARCH_ERROR type=%s fallback=ENABLED message=%s",type(exc).__name__,str(exc)[:180])
    elif primary_enabled() and provider_configured() and now<_KIMI_NEXT_ALLOWED:
        log.info("KIMI_RESEARCH_COOLDOWN remaining=%.1fs fallback=ENABLED",_KIMI_NEXT_ALLOWED-now)

    fallback_timeout=max(4.0,min(10.0,float(os.getenv("KIMI_RESEARCH_FALLBACK_TIMEOUT","7"))))
    for name in _fallback_providers():
        cfg=_provider_configured(name)
        if not cfg: continue
        base,model,key=cfg
        try:
            items=await _ask(base,model,key,prompt,fallback_timeout)
            if items:
                log.info("KIMI_RESEARCH_FALLBACK_SUCCESS provider=%s model=%s items=%d",name,model,len(items))
                return items
            log.warning("KIMI_RESEARCH_FALLBACK_EMPTY provider=%s model=%s",name,model)
        except httpx.HTTPStatusError as exc:
            log.warning("KIMI_RESEARCH_FALLBACK_FAILED provider=%s status=%s detail=%s",name,exc.response.status_code,exc.response.text[:160].replace("\n"," "))
        except Exception as exc:
            log.warning("KIMI_RESEARCH_FALLBACK_FAILED provider=%s type=%s message=%s",name,type(exc).__name__,str(exc)[:160])
    return []
