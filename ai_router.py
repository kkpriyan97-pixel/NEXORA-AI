"""Multi-provider AI router. Primary first, immediate fallback on error/timeout."""
from __future__ import annotations
import json,os
from typing import Any
import httpx
from ai_engine import MarketSnapshot,build_ai_request,parse_ai_decision

def _providers():
    names=[]
    primary=os.getenv("AI_PROVIDER","OPENAI").strip().upper()
    if primary:names.append(primary)
    for n in os.getenv("AI_FALLBACK_PROVIDERS","OPENROUTER,MISTRAL").split(","):
        n=n.strip().upper()
        if n and n not in names:names.append(n)
    return names

def _cfg(name):
    key=os.getenv(f"{name}_API_KEY","").strip()
    if not key and name=="OPENAI":key=os.getenv("OPENAI_API_KEY","").strip()
    if not key and name=="OPENROUTER":key=os.getenv("OPENROUTER_API_KEY","").strip()
    base=os.getenv(f"{name}_BASE_URL","").strip().rstrip("/")
    if not base:
        base={"OPENAI":"https://api.openai.com/v1","OPENROUTER":"https://openrouter.ai/api/v1","MISTRAL":"https://api.mistral.ai/v1"}.get(name,"")
    model=os.getenv(f"{name}_MODEL","").strip() or os.getenv("AI_MODEL","").strip()
    if not key or not base or not model:return None
    return base,model,key

async def analyze_with_fallback(snapshot:MarketSnapshot)->dict[str,Any]|None:
    request=build_ai_request(snapshot)
    prompt=("You are Candice Brain. Analyze only supplied live OHLC/market evidence. "
            "Do not invent data. Return JSON only: direction UP/DOWN or empty, "
            "confidence 0-100, reason. This is DEMO read-only; never trade.\n"+
            json.dumps(request,ensure_ascii=False,separators=(",",":")))
    last=None
    for name in _providers():
        cfg=_cfg(name)
        if not cfg:continue
        base,model,key=cfg
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(8.0,connect=4.0)) as h:
                r=await h.post(base+"/chat/completions",headers={"Authorization":f"Bearer {key}","Content-Type":"application/json"},json={"model":model,"temperature":0,"response_format":{"type":"json_object"},"messages":[{"role":"system","content":"Return only JSON with direction, confidence, reason."},{"role":"user","content":prompt}]})
                r.raise_for_status()
                body=r.json();content=body["choices"][0]["message"]["content"]
                d=parse_ai_decision(content,snapshot)
                if d:return {"decision":"SIGNAL","direction":d.direction,"confidence":d.confidence,"reason":d.reason,"display_name":d.display_name,"pair":d.pair,"provider":name}
        except Exception as e:
            last=e
            continue
    if last: raise RuntimeError(f"All configured AI providers failed: {last}")
    return None
