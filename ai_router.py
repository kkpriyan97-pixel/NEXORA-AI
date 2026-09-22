"""Multi-provider AI router. Primary first, fast fallback on error/timeout."""
from __future__ import annotations
import asyncio,json,os,logging,time,re
from typing import Any
import httpx
from ai_engine import MarketSnapshot,build_ai_request,parse_ai_decision
log=logging.getLogger("candice")
PROVIDER_COOLDOWN={}
PROVIDER_COOLDOWN_SECONDS=120.0
TRANSIENT_COOLDOWN_SECONDS=10.0
DEFAULT_FALLBACKS=("GEMINI","GROQ","NVIDIA","OPENROUTER","MISTRAL")
PROVIDER_LOCKS={}
ANALYSIS_SEMAPHORE=asyncio.Semaphore(3)
REVIEW_SEMAPHORE=asyncio.Semaphore(1)
REVIEW_PROVIDER_COOLDOWN={}
REVIEW_TRANSIENT_COOLDOWN_SECONDS=15.0
REVIEW_429_COOLDOWN_SECONDS=900.0
LIVE_EXTERNAL_AI_ENABLED=os.getenv("AI_EXTERNAL_LIVE_ENABLED","false").strip().lower()=="true"
BACKGROUND_EXTERNAL_ENABLED=os.getenv("AI_EXTERNAL_BACKGROUND_ENABLED","false").strip().lower()=="true"
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
    # External LLMs are NEVER part of the live signal critical path by default.
    # A provider-side 429/timeout must not consume the final signal window.
    # Candice local Brain remains authoritative; this function is opt-in only.
    if not LIVE_EXTERNAL_AI_ENABLED:
        log.info("AI_LIVE_EXTERNAL_DISABLED reason=signal_cycle_isolation")
        return None
    request=build_ai_request(snapshot)
    is_verifier=bool(getattr(snapshot,"technical_context",None))
    if is_verifier:
        prompt=("You are the independent verification layer for Candice Brain. "
                "The local Brain already produced the candidate shown in technical_context. "
                "Independently validate that exact candidate using only the supplied closed-candle "
                "OHLC and technical evidence. Do not simply echo it. A contradictory direction "
                "must be returned when the evidence supports the opposite direction; do not invent "
                "missing data. The supplied technical_context may also contain a multi_timeframe "
                "map covering a real tick-derived 5s micro-candle plus complete 1m through 15m frames. "
                "Use that multi-timeframe evidence as a confirmation layer: do not ignore a strong short-frame "
                "contradiction, and do not treat an unavailable frame as invented data. "
                "For BREAKOUT candidates, independently verify real breakout strength "
                "and continuation evidence, not direction alone: meaningful ATR displacement, strong "
                "breakout body, momentum/efficiency, and matching Donchian or level confirmation "
                "must support the setup. Treat a marginal/weak breakout as uncertain rather than "
                "agreeing with it. For MOMENTUM candidates in a SIDEWAYS 15m regime, require "
                "strong displacement plus repeated directional candles and a directional pattern; "
                "do not approve a setup merely because one candle points in the requested direction. "
                "Return uncertain/low confidence when the evidence is mixed or range-bound. Return one JSON object only with direction UP or DOWN, confidence "
                "0-100, and a short reason. This is DEMO read-only; never trade.\n"+
                json.dumps(request,ensure_ascii=False,separators=(",",":")))
    else:
        prompt=("You are Candice Brain. Analyze only supplied live OHLC/market evidence. "
                "Do not invent data. Return a single JSON object only with direction UP or DOWN, "
                "confidence 0-100, and reason. This is DEMO read-only; never trade.\n"+
                json.dumps(request,ensure_ascii=False,separators=(",",":")))
    last=None
    http_timeout=min(3.0,max(1.5,float(os.getenv("AI_HTTP_TIMEOUT","2.0"))))
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
                    elif status in (408,425,500,502,503,504):
                        # Do not hammer a transiently overloaded provider during the same
                        # 40-second live window; immediately continue to the next provider.
                        PROVIDER_COOLDOWN[name]=time.time()+TRANSIENT_COOLDOWN_SECONDS
                    elif status==413:
                        # Payload-size errors are deterministic for this provider/request.
                        # Short cooldown prevents repeated 413s from consuming the window.
                        PROVIDER_COOLDOWN[name]=time.time()+TRANSIENT_COOLDOWN_SECONDS
                    continue
                except Exception as e:
                    last=e
                    log.warning("AI_PROVIDER_FAILED provider=%s type=%s message=%s",name,type(e).__name__,str(e)[:160])
                    # Timeout/network failures are transient. Cool the provider briefly so
                    # the next candidate can reach another fallback instead of repeating the
                    # same stalled request.
                    if isinstance(e,(httpx.TimeoutException,httpx.NetworkError)):
                        PROVIDER_COOLDOWN[name]=time.time()+TRANSIENT_COOLDOWN_SECONDS
                    continue
    if last:
        raise RuntimeError(f"All configured AI providers failed: {type(last).__name__}")
    log.warning("AI_NO_CONFIGURED_PROVIDER providers=%s",",".join(_providers()))
    return None


async def review_result_with_fallback(rec:dict[str,Any])->dict[str,Any]:
    """Background post-result audit.

    External LLM review is disabled by default. A local audit keeps the durable
    learning queue useful without allowing provider quotas/rate limits to touch
    live execution or result processing.
    """
    if not BACKGROUND_EXTERNAL_ENABLED:
        result=str(rec.get("result") or "").upper()
        strategy=str(rec.get("strategy") or "UNKNOWN")
        trend=str(rec.get("trend_15m") or "UNKNOWN")
        direction=str(rec.get("direction") or "UNKNOWN")
        return {
            "lesson":(
                f"{strategy} result={result} in trend={trend}; reuse only the same "
                f"closed-candle context and keep existing Brain gates unchanged."
            )[:320],
            "reuse":"Recheck the same context before reuse; post-result review must not rewrite direction.",
            "evidence":f"Local outcome evidence: {direction} / {strategy} / {result}.",
            "confidence":80,
            "provider":"LOCAL_RESULT_AUDIT",
        }
    ind=dict(rec.get("indicator_context") or {})
    payload={
        "task":"Post-result audit for Candice Brain. Do not generate a new trade signal. Explain what the completed result teaches and what exact lesson should be reused when the same market context appears again.",
        "outcome":{
            "result":str(rec.get("result") or ""),
            "direction":str(rec.get("direction") or ""),
            "entry":rec.get("entry_price"),
            "exit":rec.get("exit_price"),
            "expiry_minutes":rec.get("expiry_minutes"),
            "asset":str(rec.get("display_name") or rec.get("pair") or ""),
            "strategy":str(rec.get("strategy") or ""),
            "self_strategy":str(rec.get("self_strategy") or ""),
            "confidence":rec.get("confidence"),
            "trend_15m":str(rec.get("trend_15m") or ""),
            "structure_1m":str(rec.get("structure_1m") or ""),
            "pattern":str(rec.get("pattern") or ""),
        },
        "indicators":ind,
        "required_output":{
            "lesson":"one concise reusable lesson grounded only in supplied evidence",
            "reuse":"what the Brain should check or change when the same context appears again",
            "evidence":"the concrete indicator/context conflict or confirmation",
            "confidence":"0-100"
        }
    }
    prompt=("You are the post-result learning module of Candice Brain. "
            "Use only the supplied completed-trade evidence. Never invent missing indicators. "
            "Do not recommend a trade or claim future profitability. Return one JSON object only.\n"+
            json.dumps(payload,ensure_ascii=False,separators=(",",":")))
    try:
        configured_timeout=float(os.getenv("AI_REVIEW_HTTP_TIMEOUT","8.0"))
    except (TypeError,ValueError):
        configured_timeout=8.0
    http_timeout=max(3.0,min(15.0,configured_timeout))
    connect_timeout=min(2.0,http_timeout)
    async with REVIEW_SEMAPHORE:
        providers=_providers()
        log.info("AI_POST_RESULT_REVIEW_ATTEMPT providers=%s pair=%s result=%s",
                 ",".join(providers),rec.get("pair"),rec.get("result"))
        last=None
        for name in providers:
            cfg=_cfg(name)
            if not cfg:
                continue
            cooldown_until=REVIEW_PROVIDER_COOLDOWN.get(name,0.0)
            if time.time() < cooldown_until:
                log.info("AI_POST_RESULT_PROVIDER_COOLDOWN provider=%s remaining=%.1fs",
                         name,cooldown_until-time.time())
                continue
            base,model,key=cfg
            try:
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(http_timeout,connect=connect_timeout)
                ) as h:
                    r=await h.post(
                        base+"/chat/completions",
                        headers={"Authorization":f"Bearer {key}","Content-Type":"application/json"},
                        json={"model":model,"temperature":0,"messages":[
                            {"role":"system","content":"Return only JSON with lesson, reuse, evidence, confidence."},
                            {"role":"user","content":prompt},
                        ]},
                    )
                    r.raise_for_status()
                    data=_content_json(r.json()["choices"][0]["message"]["content"])
                    lesson=str(data.get("lesson") or "").strip()
                    if not lesson:
                        raise ValueError("provider returned empty lesson")
                    try:
                        confidence=max(0,min(100,int(float(data.get("confidence") or 0))))
                    except (TypeError,ValueError):
                        confidence=0
                    log.info("AI_POST_RESULT_REVIEW_SUCCESS provider=%s pair=%s result=%s confidence=%s",
                             name,rec.get("pair"),rec.get("result"),confidence)
                    return {
                        "lesson":lesson[:320],
                        "reuse":str(data.get("reuse") or "").strip()[:240],
                        "evidence":str(data.get("evidence") or "").strip()[:320],
                        "confidence":confidence,
                        "provider":name,
                    }
            except httpx.HTTPStatusError as e:
                last=e
                status=e.response.status_code
                detail=e.response.text[:160].replace("\n"," ")
                log.warning("AI_POST_RESULT_REVIEW_FAILED provider=%s status=%s detail=%s",
                            name,status,detail)
                if status==429:
                    # Respect provider-side backoff; the durable queue owns retries.
                    retry_after=0.0
                    try:
                        retry_after=float(e.response.headers.get("Retry-After","0") or 0)
                    except (TypeError,ValueError):
                        retry_after=0.0
                    REVIEW_PROVIDER_COOLDOWN[name]=time.time()+max(REVIEW_429_COOLDOWN_SECONDS,retry_after)
                elif status in (408,425,500,502,503,504,413):
                    REVIEW_PROVIDER_COOLDOWN[name]=time.time()+REVIEW_TRANSIENT_COOLDOWN_SECONDS
            except (httpx.TimeoutException,httpx.NetworkError) as e:
                last=e
                REVIEW_PROVIDER_COOLDOWN[name]=time.time()+REVIEW_TRANSIENT_COOLDOWN_SECONDS
                log.warning("AI_POST_RESULT_REVIEW_FAILED provider=%s type=%s message=%s",
                            name,type(e).__name__,str(e)[:140])
            except Exception as e:
                last=e
                log.warning("AI_POST_RESULT_REVIEW_FAILED provider=%s type=%s message=%s",
                            name,type(e).__name__,str(e)[:140])
    raise RuntimeError(
        f"Post-result AI review unavailable across configured providers: "
        f"{type(last).__name__ if last else 'no_provider'}"
    )
