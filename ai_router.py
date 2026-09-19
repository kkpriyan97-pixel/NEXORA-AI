"""Multi-provider AI router. Primary first, fast fallback on error/timeout."""
from __future__ import annotations
import asyncio,json,os,logging,time,re
from typing import Any
import httpx
from ai_engine import MarketSnapshot,build_ai_request,parse_ai_decision
log=logging.getLogger("candice")
PROVIDER_COOLDOWN={}
PROVIDER_COOLDOWN_SECONDS=120.0
DEFAULT_FALLBACKS=("GEMINI","GROQ","NVIDIA","OPENROUTER","MISTRAL")
PROVIDER_LOCKS={}
ANALYSIS_SEMAPHORE=asyncio.Semaphore(3)
_logged_ready=set()

def _providers():
    names=[]
    primary=os.getenv("AI_PROVIDER","OPENAI").strip().upper()
    if primary:names.append(primary)
    for n in os.getenv("AI_FALLBACK_PROVIDERS",",".join(DEFAULT_FALLBACKS)).split(","):
        n=n.strip().upper()
        if n and n not in names:names.append(n)
    return names

def _cfg(name):
    key=os.getenv(f"{name}_API_KEY","").strip()
    if not key and name=="OPENAI":key=os.getenv("OPENAI_API_KEY","").strip()
    if not key and name=="OPENROUTER":key=os.getenv("OPENROUTER_API_KEY","").strip()
    if not key and name=="GEMINI":key=os.getenv("GEMINI_API_KEY","").strip()
    if not key and name=="GROQ":key=os.getenv("GROQ_API_KEY","").strip()
    if not key and name=="NVIDIA":key=os.getenv("NVIDIA_API_KEY","").strip() or os.getenv("NVIDIA_NIM_API_KEY","").strip()
    base=os.getenv(f"{name}_BASE_URL","").strip().rstrip("/")
    if not base:
        base={"OPENAI":"https://api.openai.com/v1","GEMINI":"https://generativelanguage.googleapis.com/v1beta/openai","GROQ":"https://api.groq.com/openai/v1","NVIDIA":"https://integrate.api.nvidia.com/v1","OPENROUTER":"https://openrouter.ai/api/v1","MISTRAL":"https://api.mistral.ai/v1"}.get(name,"")
    # Provider-specific models must override the global AI_MODEL. A global model
    # such as gpt-5.6 is not valid for Gemini/Groq/etc.
    model=os.getenv(f"{name}_MODEL","").strip()
    if not model:
        model={"GEMINI":"gemini-3.8-flash","GROQ":"openai/gpt-oss-20b","NVIDIA":"openai/gpt-oss-20b"}.get(name,"")
    if not model:
        model=os.getenv("AI_MODEL","").strip()
    if not key or not base or not model:return None
    return base,model,key

def _provider_lock(name):
    lock=PROVIDER_LOCKS.get(name)
    if lock is None:
        lock=asyncio.Lock()
        PROVIDER_LOCKS[name]=lock
    return lock

def _content_json(content:Any)->dict[str,Any]:
    if isinstance(content,dict):
        return content
    if isinstance(content,list):
        parts=[]
        for item in content:
            if isinstance(item,dict) and item.get("text"):
                parts.append(str(item["text"]))
            elif isinstance(item,str):
                parts.append(item)
        content=" ".join(parts)
    s=str(content or "").strip()
    if not s:
        return {}
    s=re.sub(r"^\s*```(?:json)?\s*|\s*```\s*$","",s,flags=re.I)
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        start=s.find("{")
        end=s.rfind("}")
        if start>=0 and end>start:
            return json.loads(s[start:end+1])
        raise

async def analyze_with_fallback(snapshot:MarketSnapshot)->dict[str,Any]|None:
    request=build_ai_request(snapshot)
    prompt=("You are Candice Brain. Analyze only supplied live OHLC/market evidence. "
            "Do not invent data. Return a single JSON object only with direction UP or DOWN, "
            "confidence 0-100, and reason. This is DEMO read-only; never trade.\n"+
            json.dumps(request,ensure_ascii=False,separators=(",",":")))
    last=None
    http_timeout=min(4.0,max(2.0,float(os.getenv("AI_HTTP_TIMEOUT","3.5"))))
    connect_timeout=min(1.0,http_timeout)

    async with ANALYSIS_SEMAPHORE:
        log.info("AI_FALLBACK_CHAIN providers=%s",",".join(_providers()))
        for name in _providers():
            cfg=_cfg(name)
            if not cfg:
                if name not in _logged_ready:
                    log.warning("AI_PROVIDER_NOT_CONFIGURED provider=%s",name)
                    _logged_ready.add(name)
                continue
            if name not in _logged_ready:
                log.info("AI_PROVIDER_READY provider=%s model=%s",name,cfg[1])
                _logged_ready.add(name)
            lock=_provider_lock(name)
            async with lock:
                now=time.time()
                cooldown_until=PROVIDER_COOLDOWN.get(name,0)
                if now < cooldown_until:
                    log.info("AI_PROVIDER_COOLDOWN provider=%s remaining=%.1fs",name,cooldown_until-now)
                    continue
                base,model,key=cfg
                payload={
                    "model":model,
                    "temperature":0,
                    "messages":[
                        {"role":"system","content":"Return only one JSON object with direction, confidence, reason."},
                        {"role":"user","content":prompt}
                    ]
                }
                try:
                    async with httpx.AsyncClient(timeout=httpx.Timeout(http_timeout,connect=connect_timeout)) as h:
                        r=await h.post(
                            base+"/chat/completions",
                            headers={"Authorization":f"Bearer {key}","Content-Type":"application/json"},
                            json=payload
                        )
                        r.raise_for_status()
                        body=r.json()
                        content=body["choices"][0]["message"]["content"]
                        data=_content_json(content)
                        d=parse_ai_decision(data,snapshot)
                        if d:
                            return {
                                "decision":"SIGNAL",
                                "direction":d.direction,
                                "confidence":d.confidence,
                                "reason":d.reason,
                                "display_name":d.display_name,
                                "pair":d.pair,
                                "provider":name
                            }
                        last=RuntimeError(f"{name} returned no valid direction")
                        log.warning("AI_PROVIDER_EMPTY_DECISION provider=%s",name)
                except httpx.HTTPStatusError as e:
                    last=e
                    status=e.response.status_code
                    detail=e.response.text[:160].replace("\n"," ")
                    log.warning("AI_PROVIDER_FAILED provider=%s status=%s detail=%s",name,status,detail)
                    if status==429:
                        PROVIDER_COOLDOWN[name]=time.time()+PROVIDER_COOLDOWN_SECONDS
                    continue
                except Exception as e:
                    last=e
                    log.warning("AI_PROVIDER_FAILED provider=%s type=%s message=%s",name,type(e).__name__,str(e)[:160])
                    continue
    if last:
        raise RuntimeError(f"All configured AI providers failed: {type(last).__name__}")
    log.warning("AI_NO_CONFIGURED_PROVIDER providers=%s",",".join(_providers()))
    return None
