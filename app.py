import asyncio,json,logging,os,time
from typing import Any
from telegram import Update
from telegram.ext import ContextTypes
import httpx
from olymptrade_ws import OlympTradeClient
from olymptrade_ws.olympconfig import parameters
from brain_rules import BrainState,rank_signal_candidates
from candice_brain import analyze_asset
from ai_engine import snapshot_from_asset
from ai_router import analyze_with_fallback

logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
log=logging.getLogger("candice")
BRAIN=BrainState()
STATE={"status":"starting","assets":[],"prices":{},"price_source":{},"candles":{},"analyses":{},"network":{},"read_only":True,"cycle":0,"last_cycle":None}
CANDLE_FETCH_SEM=asyncio.Semaphore(2)
CANDLE_FETCH_LAST={}
CANDLE_FETCH_INTERVAL=60.0
TICK_RESUB_SEM=asyncio.Semaphore(3)
TICK_RESUB_TIMEOUT=1.5
LIVE_TICK_MAX_AGE=5.0
QUOTE_SNAPSHOT_MAX_AGE=6.0
QUOTE_SNAPSHOT_REFRESH=3.5
QUOTE_SNAPSHOT_SEM=asyncio.Semaphore(3)
QUOTE_SNAPSHOT_LAST={}
AI_REVIEW_CACHE={}
AI_REVIEW_TTL=90.0
AI_REVIEW_FAIL_TTL=20.0
AI_PROVIDER_COOLDOWN={}
AI_REVIEW_TIMEOUT=2.4
CLIENT=None
LOCK=asyncio.Lock()

def pair_name(x):
    return str(x.get("pair") or x.get("p") or x.get("symbol") or x.get("instrument") or x.get("id") or "")

def display_name(x):
    # Preserve the exact account-facing asset name returned by the
    # authenticated account feed; prefer display fields over internal names.
    for k in ("title","display_name","displayName","name"):
        if isinstance(x.get(k),str) and x[k].strip():return x[k].strip()
    return ""

def event_records(client,event_id):
    out=[]
    try:cached=client.get_cached_events(event_id)
    except Exception:cached=[]
    for m in cached or []:
        d=m.get("d") if isinstance(m,dict) else None
        if isinstance(d,list):out.extend(x for x in d if isinstance(x,dict))
    return out

def _norm_text(v):
    if v is None: return ""
    if isinstance(v,(dict,list)): return json.dumps(v,ensure_ascii=False).lower()
    return str(v).strip().lower()

def is_flex_time_asset(x):
    """
    Keep the Flex Time universe from OlympTrade metadata without hardcoding
    individual asset names/symbols. Explicit non-Flex products (for example
    Quickler/5-second trading) are excluded; otherwise authenticated market
    assets remain eligible. This preserves newly added Flex assets.
    """
    if not isinstance(x,dict): return False
    fields=("trading_mode","trade_mode","mode","product","category",
            "instrument_type","expiration_type","expiration_mode","type","name",
            "title","display_name","displayName")
    text=" ".join(_norm_text(x.get(k)) for k in fields)
    explicit_flex=any(k in text for k in (
        "flex time","flex_time","flex-time","fixed time","fixed_time"
    ))
    explicit_quickler=any(k in text for k in (
        "quickler","5 second","5-second","5 seconds","5_seconds"
    ))
    # Keep Quickler in the authenticated AVAILABLE-ASSET list because it is
    # visibly available in the user's Flex account. It is marked
    # signal_eligible=False below because Olymptrade documents Quickler as a
    # special 5-second FT product, while Candice's current signal engine uses
    # 1-minute analysis with 2/3/5/15-minute expiries.
    # Do not hide an account-visible asset merely because it is not compatible
    # with the current signal duration policy.
    return True

def build_assets(client,raw):
    # IMPORTANT: raw is the authenticated Flex-Time/availability feed chosen
    # by market_worker. Do not widen it with the general instrument catalogue.
    # The feed is dynamic: open/closed assets can change at any moment.
    prof={}
    for x in raw or []:
        if not isinstance(x,dict): continue
        p=pair_name(x)
        v=x.get("profitability")
        if p and isinstance(v,(int,float)): prof[p]=int(v)

    out=[]; seen=set()
    for x in raw or []:
        if not isinstance(x,dict) or not is_flex_time_asset(x): continue
        p=pair_name(x)
        if not p or p in seen: continue

        # Only currently OPEN/tradable Flex assets enter the Brain.
        # Never hard-code the historical 76 REAL + 37 OTC count; the platform
        # is authoritative and this count is expected to change with time.
        if x.get("disabled") is True or x.get("locked") is True or x.get("locked_trading") is True:
            continue
        seen.add(p)

        title=display_name(x) or p
        # Olymptrade's Quickler asset uses ticker ULTRA_X. Keep the
        # platform-facing name in Candice's asset universe instead of exposing
        # the internal ticker as if it were a different asset.
        if p.upper() == "ULTRA_X":
            title = "Quickler"
        v=prof.get(p,x.get("profitability",0))
        try: profitability=int(v)
        except Exception: profitability=0
        quickler = "quickler" in " ".join(
            _norm_text(x.get(k)) for k in
            ("pair","symbol","name","title","display_name","displayName",
             "product","category","instrument_type","expiration_type","expiration_mode")
        )
        out.append({
            "pair":p,"display_name":title,"title":title,
            "signal_asset_label":title,
            "profitability":profitability,
            "locked":False,"locked_trading":False,"disabled":False,
            "mode":"OTC" if "_OTC" in p.upper() else "REAL",
            "trading_mode":"FLEX_TIME",
            "signal_eligible":not quickler
        })
    return out

async def telegram(text, chat_id=None):
    token=os.getenv("TELEGRAM_BOT_TOKEN","").strip()
    chat=str(chat_id or STATE.get("telegram_chat_id") or os.getenv("TELEGRAM_CHAT_ID","")).strip()
    if not token or not chat:
        log.warning("TELEGRAM_NOT_CONFIGURED")
        return False
    try:
        async with httpx.AsyncClient(timeout=8) as h:
            r=await h.post(f"https://api.telegram.org/bot{token}/sendMessage",json={"chat_id":chat,"text":text,"parse_mode":"HTML"})
            if r.status_code >= 400:
                try: detail=r.json()
                except Exception: detail={"description":r.text[:200]}
                log.warning("TELEGRAM_SEND_FAILED status=%s description=%s",r.status_code,detail.get("description"))
                return False
            return True
    except Exception as e:
        log.warning("TELEGRAM_SEND_FAILED type=%s message=%s",type(e).__name__,str(e)[:200]);return False

async def telegram_background(text_msg,label):
    try:
        sent=await asyncio.wait_for(telegram(text_msg),timeout=5.0)
        log.info("TELEGRAM_SIGNAL_DELIVERY label=%s sent=%s",label,sent)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.warning("TELEGRAM_SIGNAL_DELIVERY_FAILED label=%s type=%s message=%s",label,type(e).__name__,str(e)[:160])

def _tick_records(value):
    # OlympTrade tick payloads have appeared as either a list of records or a
    # nested dict/list. Walk the payload instead of assuming one exact shape.
    if isinstance(value,dict):
        yield value
        for v in value.values():
            if isinstance(v,(dict,list)):
                yield from _tick_records(v)
    elif isinstance(value,list):
        for v in value:
            if isinstance(v,(dict,list)):
                yield from _tick_records(v)

async def on_tick(message):
    received_at=time.time()
    updated=0
    for t in _tick_records(message.get("d")):
        p=str(t.get("p") or t.get("pair") or t.get("symbol") or t.get("instrument") or "")
        q=t.get("q")
        if q is None:
            q=t.get("price")
        if q is None:
            q=t.get("value")
        if q is None:
            q=t.get("v")
        ts=t.get("t")
        if p and q is not None:
            try:
                broker_ts=float(ts) if ts is not None else received_at
                # Keep broker time for audit, but use local receive time for
                # freshness decisions because small broker/local clock skew can
                # otherwise make a genuinely live tick look stale/future.
                STATE["prices"][p]=(float(q),broker_ts,received_at)
                STATE["price_source"][p]="tick"
                updated+=1
            except Exception:
                pass
    if updated:
        last_log=STATE.get("_tick_state_log_at",0.0)
        if received_at-last_log>=10.0:
            STATE["_tick_state_log_at"]=received_at
            log.info("TICK_STATE_READY updated=%d tracked=%d",updated,len(STATE["prices"]))

def tick_received_at(pair):
    rec=STATE["prices"].get(pair)
    if not rec or len(rec)<1:return None
    try:
        # New records store local receipt time in slot 2. Older records remain
        # compatible and fall back to their broker timestamp.
        return float(rec[2]) if len(rec)>=3 and rec[2] is not None else float(rec[1])
    except Exception:
        return None

async def ensure_candidate_ticks(pairs):
    client=CLIENT
    if not client or not pairs:return 0
    unique=[]
    seen=set()
    for p in pairs:
        p=str(p or "")
        if p and p not in seen:
            seen.add(p);unique.append(p)

    successes=0
    async def one(pair):
        nonlocal successes
        async with TICK_RESUB_SEM:
            try:
                await asyncio.wait_for(client.market.subscribe_ticks(pair),timeout=TICK_RESUB_TIMEOUT)
                successes+=1
            except Exception as e:
                log.debug("TICK_RESUBSCRIBE_FAILED pair=%s type=%s message=%s",
                          pair,type(e).__name__,str(e)[:120])

    await asyncio.gather(*(one(p) for p in unique),return_exceptions=True)
    if successes:
        log.info("TICK_RESUBSCRIBE_ATTEMPT pairs=%d succeeded=%d",len(unique),successes)
        await asyncio.sleep(0.15)
    return successes

async def ensure_candidate_quotes(pairs):
    client=CLIENT
    if not client or not pairs:
        return 0

    now=time.time()
    unique=[]
    seen=set()
    for p in pairs:
        p=str(p or "")
        if not p or p in seen:
            continue
        seen.add(p)
        if has_fresh_live_price(p,now,LIVE_TICK_MAX_AGE):
            continue
        if now-QUOTE_SNAPSHOT_LAST.get(p,0.0) < QUOTE_SNAPSHOT_REFRESH:
            continue
        unique.append(p)

    if not unique:
        return 0

    fetched=0
    async def one(pair):
        nonlocal fetched
        QUOTE_SNAPSHOT_LAST[pair]=time.time()
        async with QUOTE_SNAPSHOT_SEM:
            try:
                snap=await asyncio.wait_for(client.market.get_live_snapshot(pair),timeout=1.4)
                if not snap or snap.get("price") is None:
                    return
                received=time.time()
                STATE["prices"][pair]=(float(snap["price"]),float(snap.get("timestamp",received)),received)
                STATE["price_source"][pair]="snapshot_5s"
                fetched+=1
            except Exception as e:
                log.debug("QUOTE_SNAPSHOT_FAILED pair=%s type=%s message=%s",
                          pair,type(e).__name__,str(e)[:120])

    await asyncio.gather(*(one(p) for p in unique),return_exceptions=True)
    if fetched:
        log.info("QUOTE_SNAPSHOT_REFRESH requested=%d received=%d",len(unique),fetched)
    return fetched

async def refresh_candles(force=False):
    client=CLIENT;assets=list(STATE["assets"])
    if not client:return
    now=time.time()
    due=[a for a in assets if force or now-CANDLE_FETCH_LAST.get(a["pair"],0)>=CANDLE_FETCH_INTERVAL]
    async def one(a):
        p=a["pair"]
        if not a.get("signal_eligible",True):
            return
        async with CANDLE_FETCH_SEM:
            try:
                await asyncio.sleep(0.35)
                cs=await client.market.get_candles(p,size=60,count=60)
                normalized=[]
                if isinstance(cs,list):
                    for item in cs:
                        if isinstance(item,dict) and isinstance(item.get("candles"),list):
                            normalized.extend(x for x in item["candles"] if isinstance(x,dict))
                        elif isinstance(item,dict) and any(k in item for k in ("open","o","high","h","low","l","close","c")):
                            normalized.append(item)
                if normalized:
                    try:
                        normalized.sort(key=lambda x: float(x.get("time",x.get("t",0))))
                    except Exception:
                        pass
                    STATE["candles"][p]=normalized
                CANDLE_FETCH_LAST[p]=time.time()
            except Exception as e:
                log.warning("CANDLE_REFRESH_THROTTLED_OR_FAILED pair=%s %s",p,e)
                CANDLE_FETCH_LAST[p]=time.time()
    await asyncio.gather(*(one(a) for a in due))
    for a in assets:
        if not a.get("signal_eligible",True):
            continue
        p=a["pair"];price=STATE["prices"].get(p,(None,None))[0]
        an=analyze_asset(a,STATE["candles"].get(p,[]),price)
        if an:
            an["profitability"]=a["profitability"];STATE["analyses"][p]=an
        else:STATE["analyses"].pop(p,None)
    log.info("LIVE_ANALYSIS_REFRESH assets=%d signal_eligible=%d fetched=%d qualified=%d",
             len(assets),sum(1 for a in assets if a.get("signal_eligible",True)),
             len(due),len(STATE["analyses"]))

def has_fresh_live_price(pair,reference_ts=None,max_age=LIVE_TICK_MAX_AGE):
    rec=STATE["prices"].get(pair)
    if not rec or len(rec)<1 or rec[0] is None:return False
    received=tick_received_at(pair)
    if received is None:return False
    ref=time.time() if reference_ts is None else float(reference_ts)
    try:age=ref-received
    except Exception:return False
    return 0 <= age <= float(max_age)

def live_price_age(pair,reference_ts=None):
    received=tick_received_at(pair)
    if received is None:return None
    ref=time.time() if reference_ts is None else float(reference_ts)
    try:return max(0.0,ref-received)
    except Exception:return None

async def final_candidate(use_cached_only=False,require_live_price=False):
    BRAIN.prune_expired_cooldowns()
    eligible=[
        a for a in BRAIN.filter_candidates(STATE["assets"])
        if a.get("signal_eligible",True)
    ]
    raw=[STATE["analyses"][a["pair"]].copy() for a in eligible if a["pair"] in STATE["analyses"]]
    raw=[BRAIN.adaptive_candidate(x) for x in raw]
    raw=rank_signal_candidates(raw)
    if not raw:return None
    # AI reviews the strongest technical candidates in parallel. Sequential reviews
    # consumed the final 40-second window (3-4 seconds per provider call), so one
    # candidate could reach the target while the remaining reviews were still running.
    # When a live price is required, prioritize only currently fresh-tick
    # candidates before spending the final 30-second window on AI review.
    # This guarantees that a stale top-ranked asset cannot block the next
    # qualified asset that has a usable live price.
    if require_live_price:
        live_raw=[x for x in raw if has_fresh_live_price(x["pair"],time.time(),LIVE_TICK_MAX_AGE)]
        if not live_raw:
            # Do not call the library tick-resubscription API here: the deployed
            # OlympTrade library sends events 12/280, which the broker currently
            # rejects with invalid_request. Use the read-only short-interval
            # quote snapshot instead; it is timestamped locally and never
            # fabricates a price.
            retry_pairs=[x["pair"] for x in raw[:8]]
            await ensure_candidate_quotes(retry_pairs)
            live_raw=[x for x in raw if has_fresh_live_price(x["pair"],time.time(),QUOTE_SNAPSHOT_MAX_AGE)]
            log.info("LIVE_PRICE_GUARD qualified=%d fresh=%d snapshot_requested=%d",
                     len(raw),len(live_raw),len(retry_pairs))
        if not live_raw:
            return None
        top=live_raw[:20]
    else:
        top=raw[:20]
    now=time.time()
    reviewed=[]

    async def review_one(x):
        cs=STATE["candles"].get(x["pair"],[])
        price=STATE["prices"].get(x["pair"],(x.get("price"),None))[0]
        asset=next((a for a in eligible if a["pair"]==x["pair"]),None)
        if not asset:
            return None
        snap=snapshot_from_asset(asset,cs,price,now)
        cache_key=(x["pair"],str(x.get("entry_candle_ts")),x.get("direction"))
        cached=AI_REVIEW_CACHE.get(cache_key)
        ttl=AI_REVIEW_TTL if cached and cached[1] else AI_REVIEW_FAIL_TTL
        if cached and time.time()-cached[0] < ttl:
            d=cached[1]
        elif use_cached_only:
            d=None
        else:
            d=None
            try:
                d=await asyncio.wait_for(analyze_with_fallback(snap),timeout=AI_REVIEW_TIMEOUT)
                AI_REVIEW_CACHE[cache_key]=(time.time(),d)
            except Exception as e:
                log.warning("AI_REVIEW_FAILED pair=%s type=%s message=%s",x["pair"],type(e).__name__,str(e)[:120])
                AI_REVIEW_CACHE[cache_key]=(time.time(),None)
        if d and int(d.get("confidence",0))>=90:
            y=x.copy()
            y.update({"confidence":int(d["confidence"]),"reason":d.get("reason") or x["reason"],"ai_provider":d.get("provider")})
            return y
        # When every external LLM provider is unavailable, preserve the live
        # evidence-first Candice Brain decision instead of losing the whole
        # 5-minute cycle. This fallback never bypasses the 90% Brain threshold,
        # the live-candle evidence, the 15m conflict gate, or DEMO/read-only mode.
        if not use_cached_only and not d and int(x.get("confidence") or 0) >= 90:
            y=x.copy()
            y.update({
                "confidence":int(x.get("confidence") or 0),
                "reason":x.get("reason") or "Candice local Brain verified live market evidence",
                "ai_provider":"CANDICE_LOCAL_BRAIN"
            })
            # Persist the completed local decision in the same short-lived
            # cache used by the exact-boundary check. The previous version
            # cached None on provider failure, so the valid local result could
            # disappear before the 5-minute boundary.
            AI_REVIEW_CACHE[cache_key]=(time.time(),{
                "decision":"SIGNAL",
                "direction":y["direction"],
                "confidence":y["confidence"],
                "reason":y["reason"],
                "display_name":y["display_name"],
                "pair":y["pair"],
                "provider":"CANDICE_LOCAL_BRAIN"
            })
            log.warning("AI_EXTERNAL_FALLBACK_LOCAL pair=%s confidence=%s strategy=%s",
                        x["pair"],x.get("confidence"),x.get("strategy"))
            return y
        return None

    results=await asyncio.gather(*(review_one(x) for x in top),return_exceptions=True)
    for x,r in zip(top,results):
        if isinstance(r,Exception):
            log.warning("AI_REVIEW_TASK_FAILED pair=%s type=%s message=%s",x["pair"],type(r).__name__,str(r)[:120])
        elif r:
            reviewed.append(r)
    ranked=rank_signal_candidates(reviewed)
    if require_live_price:
        ranked=[x for x in ranked if has_fresh_live_price(x["pair"],time.time(),LIVE_TICK_MAX_AGE)]
    return ranked[0] if ranked else None

async def result_watch(key):
    s=BRAIN.active_signals.get(key)
    if not s:return
    await asyncio.sleep(max(0,s.expiry_minutes*60-(time.time()-s.entry_ts)))
    price=STATE["prices"].get(s.pair,(None,None))[0]
    if price is None:
        cs=STATE["candles"].get(s.pair,[])
        if cs:price=float(cs[-1].get("close",cs[-1].get("c",s.entry_price)))
    if price is None:return
    rec=BRAIN.finish_signal(key,price)
    label=rec["display_name"]
    icon={"WIN":"🟢","LOSS":"🔴","TIE":"🟡"}[rec["result"]]
    await telegram(f"━━━━━━━━━━━━━━━━━━━━\n🎯 CANDICE AI RESULT\n━━━━━━━━━━━━━━━━━━━━\n\n📊 ASSET: {label}\n➡️ DIRECTION: {rec['direction']}\n\n💰 ENTRY: {rec['entry_price']}\n💰 EXIT: {rec['exit_price']}\n⏱️ EXPIRY: {rec['expiry_minutes']} MIN\n\n{icon} {rec['result']}\n\n🧠 STRATEGY: {rec['strategy']}\n📈 15M TREND: {rec['trend_15m']}\n🕯️ 1M STRUCTURE: {rec['structure_1m']}\n\n🧠 Brain learning recorded\n━━━━━━━━━━━━━━━━━━━━")
    log.info("RESULT pair=%s result=%s exit=%s cooldown=%s",rec["pair"],rec["result"],rec["exit_price"],rec["result"]=="LOSS")

async def cycle_loop():
    # Internal analysis starts 75s before the 5-minute boundary.
    # The user-facing signal is emitted exactly 30s before entry.
    # No signal is ever emitted at/after the entry boundary.
    PRE_ANALYSIS_LEAD=75.0
    SIGNAL_LEAD=30.0
    last_target=0

    async def send_cycle_signal(candidate,target):
        if not candidate or not BRAIN.can_send_cycle_signal():
            return False

        p=candidate["pair"]
        # The quote is refreshed immediately before this function is called.
        # Keep this send path non-blocking so the 30s signal deadline is not
        # consumed by another network request.
        now=time.time()
        if not has_fresh_live_price(p,now,QUOTE_SNAPSHOT_MAX_AGE):
            log.info("NO_VALID_SIGNAL_AT_SEND cycle=%s pair=%s reason=quote_not_fresh",
                     int(target//300),p)
            return False

        entry=STATE["prices"].get(p,(None,None))[0]
        if entry is None:
            log.info("NO_VALID_SIGNAL_AT_SEND cycle=%s pair=%s reason=price_missing",
                     int(target//300),p)
            return False

        confidence=int(candidate.get("confidence") or 0)
        if confidence < 90:
            log.info("NO_VALID_SIGNAL_AT_SEND cycle=%s pair=%s reason=confidence_%s",
                     int(target//300),p,confidence)
            return False

        ts=target-SIGNAL_LEAD
        if time.time() > ts+0.25:
            log.info("NO_VALID_SIGNAL_AT_SEND cycle=%s pair=%s reason=deadline_passed",
                     int(target//300),p)
            return False

        s=BRAIN.mark_signal_sent(
            pair=p,display_name=candidate["display_name"],
            direction=candidate["direction"],expiry_minutes=candidate["expiry_minutes"],
            entry_price=entry,entry_ts=target,
            entry_candle_ts=candidate["entry_candle_ts"],
            strategy=candidate["strategy"],reason=candidate["reason"],confidence=confidence
        )
        key=f"{s.cycle_id}:{s.pair}:{s.entry_ts}"
        msg=(f"🚨 CANDICE AI SIGNAL\\n\\n"
             f"👋 Market setup detected!\\n\\n"
             f"📊 {s.display_name}\\n\\n"
             f"<b>{'🔻 DOWN' if s.direction.upper() == 'DOWN' else '🟢 UP'}</b>\\n"
             f"<b>⏱️ {s.expiry_minutes} MIN EXPIRY</b>\\n\\n"
             f"🕒 {time.strftime('%H:%M:%S',time.localtime(ts))} UAE\\n"
             f"🎯 Entry → {time.strftime('%H:%M:%S',time.localtime(target))}\\n\\n"
             f"💰 {s.entry_price}\\n"
             f"🎯 Confidence → {s.confidence}%\\n\\n"
             f"📈 Trend → {s.trend_15m}\\n"
             f"🕯️ Structure → {s.structure_1m}\\n"
             f"🧠 Strategy → {s.strategy}\\n\\n"
             f"🟢 DEMO • READ ONLY\\n"
             f"🤖 CANDICE BRAIN")
        log.info(
            "FINAL_SIGNAL cycle=%s pair=%s direction=%s confidence=%s price_source=%s "
            "signal_utc=%s target_utc=%s lead_seconds=%.3f",
            int(target//300),s.pair,s.direction,s.confidence,
            STATE["price_source"].get(s.pair,"unknown"),
            time.strftime("%H:%M:%S.%f",time.gmtime(ts))[:-3],
            time.strftime("%H:%M:%S.%f",time.gmtime(target))[:-3],
            target-time.time()
        )
        asyncio.create_task(telegram_background(msg,f"{s.cycle_id}:{s.pair}:{s.entry_ts}"))
        asyncio.create_task(result_watch(key))
        return True

    while True:
        now=time.time()
        target=(int(now)//300+1)*300
        if target<=last_target:
            target=last_target+300
        analysis_start=target-PRE_ANALYSIS_LEAD
        signal_at=target-SIGNAL_LEAD

        await asyncio.sleep(max(0,analysis_start-time.time()))
        cycle_id=int(target//300)
        BRAIN.start_cycle(cycle_id)
        STATE["cycle"]=cycle_id
        last_target=target
        log.info(
            "CYCLE_WINDOW_START cycle=%s analysis_start_utc=%s signal_utc=%s target_utc=%s "
            "analysis_start_uae=%s signal_uae=%s target_uae=%s",
            cycle_id,
            time.strftime("%H:%M:%S",time.gmtime(analysis_start)),
            time.strftime("%H:%M:%S",time.gmtime(signal_at)),
            time.strftime("%H:%M:%S",time.gmtime(target)),
            time.strftime("%H:%M:%S",time.gmtime(analysis_start+4*3600)),
            time.strftime("%H:%M:%S",time.gmtime(signal_at+4*3600)),
            time.strftime("%H:%M:%S",time.gmtime(target+4*3600))
        )

        candidate=None
        # Use the 45s before the signal deadline for analysis. Once the deadline
        # is reached, stop recomputing so no slow AI/provider call can push the
        # signal past the required 30s lead time.
        while time.time() < signal_at-0.75:
            try:
                await refresh_candles()
            except Exception as e:
                log.warning("CYCLE_CANDLE_REFRESH_FAILED cycle=%s type=%s message=%s",
                            cycle_id,type(e).__name__,str(e)[:160])
            remaining=max(0,signal_at-time.time())
            if remaining <= 0.75:
                break
            try:
                new_candidate=await asyncio.wait_for(
                    final_candidate(require_live_price=True),
                    timeout=max(0.75,remaining-0.20)
                )
                if new_candidate is not None:
                    candidate=new_candidate
            except asyncio.TimeoutError:
                log.warning("CYCLE_PRE_SIGNAL_EVALUATION_TIMEOUT cycle=%s remaining=%.2f",
                            cycle_id,max(0,signal_at-time.time()))
            await asyncio.sleep(min(2.0,max(0,signal_at-time.time())))

        # Refresh the chosen quote shortly before the signal deadline, then wait
        # for the exact target-30s timestamp.
        # Refresh the selected quote about 2 seconds before the user-facing
        # signal deadline. This keeps the final send path fast and guarantees
        # the entry price is recent without waiting at target time.
        pre_quote_at=signal_at-2.0
        if candidate:
            await asyncio.sleep(max(0,pre_quote_at-time.time()))
            try:
                await ensure_candidate_quotes([candidate["pair"]])
            except Exception as e:
                log.warning("CYCLE_PRE_SIGNAL_QUOTE_REFRESH_FAILED cycle=%s type=%s message=%s",
                            cycle_id,type(e).__name__,str(e)[:120])

        await asyncio.sleep(max(0,signal_at-time.time()))
        sent=False
        if candidate and time.time() <= signal_at+0.20:
            try:
                sent=await send_cycle_signal(candidate,target)
            except Exception as e:
                log.exception("FINAL_SIGNAL_BUILD_FAILED cycle=%s type=%s message=%s",
                              cycle_id,type(e).__name__,str(e)[:160])

        if not sent:
            log.info("NO_VALID_FINAL_SETUP cycle=%s reason=no_candidate_ready_30s_before_entry",
                     cycle_id)

        # Keep the loop aligned to the next 5-minute boundary. Result tracking
        # uses the stored exact entry timestamp, so no extra boundary evaluation
        # is performed here.
        await asyncio.sleep(max(0,target+0.25-time.time()))

async def audit_outbound_network():
    """
    Record the actual public egress identity used by the Render process.
    This is an audit/guard only; it does not spoof or bypass location controls.
    Set REQUIRE_UAE_EGRESS=1 when the deployment is expected to have UAE egress.
    """
    try:
        async with httpx.AsyncClient(timeout=6) as h:
            r=await h.get("https://ipapi.co/json/")
            r.raise_for_status()
            d=r.json()
        ip=str(d.get("ip") or "")
        country=str(d.get("country_code") or "").upper()
        STATE["network"]={
            "public_ip":ip,
            "country":country,
            "region":str(d.get("region") or ""),
            "city":str(d.get("city") or ""),
            "org":str(d.get("org") or "")
        }
        log.info("OUTBOUND_NETWORK public_ip=%s country=%s region=%s city=%s org=%s",
                 ip,country,STATE["network"]["region"],STATE["network"]["city"],STATE["network"]["org"])
        if os.getenv("REQUIRE_UAE_EGRESS","0").strip()=="1" and country!="AE":
            raise RuntimeError(f"UAE egress required but detected country={country or 'UNKNOWN'} ip={ip or 'UNKNOWN'}")
        return country
    except Exception as e:
        log.warning("OUTBOUND_NETWORK_AUDIT_FAILED type=%s message=%s",type(e).__name__,str(e)[:180])
        if os.getenv("REQUIRE_UAE_EGRESS","0").strip()=="1":
            raise
        return ""

async def market_worker():
    global CLIENT
    while True:
        token=os.getenv("OLYMPTRADE_ACCESS_TOKEN","").strip()
        if not token:STATE["status"]="waiting_for_token";await asyncio.sleep(30);continue
        client=OlympTradeClient(access_token=token,log_raw_messages=False);CLIENT=client;client.register_callback(parameters.E_TICK_UPDATE,on_tick)
        try:
            STATE["status"]="connecting"
            await audit_outbound_network()
            await client.start()
            STATE["status"]="connected"
            # Session initialization is what causes broker event 55 to arrive.
            # Start it without waiting for the library's slow account-info fallback.
            init_task=asyncio.create_task(client.initialize_session())
            demo_found=False
            for _ in range(20):
                await asyncio.sleep(0.25)
                for m in client.get_cached_events(55):
                    d=m.get("d") if isinstance(m,dict) else None
                    if isinstance(d,list):
                        for a in d:
                            if isinstance(a,dict) and a.get("group")=="demo" and a.get("account_id") is not None:
                                client.account_id=a.get("account_id")
                                client.account_group="demo"
                                demo_found=True
                                break
                    if demo_found: break
                if demo_found: break
            if demo_found:
                log.info("DEMO_ACCOUNT_SELECTED account_id=%s",client.account_id)
                if not init_task.done():
                    init_task.cancel()
                try: await init_task
                except asyncio.CancelledError: pass
            else:
                try: await init_task
                except Exception as e: log.warning("SESSION_INIT_AFTER_EVENT55_FAILED %s",e)
                for m in client.get_cached_events(55):
                    d=m.get("d") if isinstance(m,dict) else None
                    if isinstance(d,list):
                        for a in d:
                            if isinstance(a,dict) and a.get("group")=="demo" and a.get("account_id") is not None:
                                client.account_id=a.get("account_id")
                                client.account_group="demo"
                                demo_found=True
                                break
                    if demo_found: break
            if not demo_found:
                log.error("DEMO_ACCOUNT_NOT_FOUND_IN_EVENT_55")
                raise RuntimeError("DEMO account id not available from broker event 55")
            raw=await client.market.get_available_assets(client.account_id)
            # IMPORTANT: the authenticated account-scoped asset response is the
            # source of truth. Do NOT replace it with cached event 182/global
            # Flex metadata: that stream can contain region/account-ineligible
            # instruments (for example India-specific OTC products).
            source=raw or []
            assets=build_assets(client,source)
            STATE["assets"]=assets;STATE["status"]="live_read_only"
            real_n=sum(a["mode"]=="REAL" for a in assets); otc_n=sum(a["mode"]=="OTC" for a in assets)
            log.info("ACCOUNT_ASSET_SOURCE account_id=%s source_count=%d open_real=%d open_otc=%d open_total=%d",
                     client.account_id,len(source),real_n,otc_n,len(assets))
            log.info("ALL_ACCOUNT_OPEN_ASSETS_READY count=%d",len(assets))
            # Do not bulk-call MarketAPI.subscribe_ticks(). The current broker
            # endpoint rejects its event-12/280 requests. Live event-1 ticks
            # already arrive from the authenticated session; missing quotes are
            # handled by the read-only snapshot fallback in final_candidate().
            log.info("TICK_SUBSCRIPTION_MODE disabled_reason=broker_event_12_280_rejected")
            await refresh_candles(force=True)
            while True:await asyncio.sleep(30)
        except Exception as e:
            STATE["status"]="error";log.exception("MARKET_WORKER_ERROR %s",e);await asyncio.sleep(15)
        finally:
            try:await client.stop()
            except Exception:pass
            CLIENT=None

async def telegram_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "✅ NEXORA AI is online.\\n\\n"
        "Candice Brain: LIVE\\n"
        "Mode: DEMO / Read-only"
    )


async def health(reader,writer):
    try:
        raw=await reader.read(65536)
        head,_,body=raw.partition(b"\r\n\r\n")
        first=head.split(b"\r\n",1)[0].decode("latin1","ignore")
        parts=first.split(" ")
        path=parts[1] if len(parts)>1 else "/"
        headers={}
        for line in head.decode("latin1","ignore").split("\r\n")[1:]:
            if ":" in line:
                k,v=line.split(":",1);headers[k.strip().lower()]=v.strip()
        webhook_secret=os.getenv("TELEGRAM_WEBHOOK_SECRET","").strip()
        if path.startswith("/health"):
            body_out=json.dumps({"service":"CANDICE-AI","status":STATE["status"],"read_only":True,"asset_count":len(STATE["assets"]),"qualified":len(STATE["analyses"]),"cycle":STATE["cycle"],"active_results":len(BRAIN.active_signals),"network":STATE.get("network",{})}).encode()
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n"+body_out)
            await writer.drain()
            return
        if path.startswith("/telegram/webhook") and webhook_secret and headers.get("x-telegram-bot-api-secret-token") != webhook_secret:
            writer.write(b"HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n")
            await writer.drain()
            return
        if path.startswith("/telegram/webhook") and not body:
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\nNEXORA Telegram webhook is ready")
            await writer.drain()
            return
        if path.startswith("/telegram/webhook") and body:
            try:
                upd=json.loads(body.decode("utf-8"))
                msg=upd.get("message") or upd.get("edited_message") or {}
                txt=str(msg.get("text") or "").strip()
                chat_id=(msg.get("chat") or {}).get("id")
                if chat_id is not None:
                    STATE["telegram_chat_id"]=chat_id
                    log.info("TELEGRAM_CHAT_ID_CAPTURED chat_id=%s",chat_id)
                if txt.lower().startswith("/start") and chat_id is not None:
                    sent=await telegram("✅ NEXORA AI is online.\n\nCandice Brain: LIVE\nMode: DEMO / Read-only", chat_id=chat_id)
                    log.info("TELEGRAM_START_RECEIVED chat_id=%s sent=%s",chat_id,sent)
            except Exception as e:
                log.warning("TELEGRAM_WEBHOOK_PARSE_FAILED %s",e)
        body_out=json.dumps({"service":"CANDICE-AI","status":STATE["status"],"read_only":True,"asset_count":len(STATE["assets"]),"qualified":len(STATE["analyses"]),"cycle":STATE["cycle"],"active_results":len(BRAIN.active_signals),"network":STATE.get("network",{})}).encode()
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"+body_out);await writer.drain()
    finally:writer.close()

async def configure_telegram_webhook():
    token=os.getenv("TELEGRAM_BOT_TOKEN","").strip()
    if not token:
        log.warning("TELEGRAM_NOT_CONFIGURED"); return
    url=os.getenv("TELEGRAM_WEBHOOK_URL","https://priyanithan-zflv.onrender.com/telegram/webhook").strip()
    secret=os.getenv("TELEGRAM_WEBHOOK_SECRET","").strip()
    try:
        payload={"url":url}
        if secret: payload["secret_token"]=secret
        async with httpx.AsyncClient(timeout=10) as h:
            await h.post(f"https://api.telegram.org/bot{token}/deleteWebhook",json={"drop_pending_updates":False})
            r=await h.post(f"https://api.telegram.org/bot{token}/setWebhook",json=payload)
            r.raise_for_status()
            info=await h.get(f"https://api.telegram.org/bot{token}/getWebhookInfo")
            try: data=info.json().get("result",{})
            except Exception: data={}
            log.info("TELEGRAM_WEBHOOK_READY url=%s pending=%s last_error=%s",data.get("url",""),data.get("pending_update_count",0),str(data.get("last_error_message",""))[:160])
    except Exception as e:
        log.warning("TELEGRAM_WEBHOOK_SETUP_FAILED %s",e)

async def main():
    port=int(os.getenv("PORT","10000"));server=await asyncio.start_server(health,"0.0.0.0",port)
    await configure_telegram_webhook()
    await asyncio.gather(market_worker(),cycle_loop(),server.serve_forever())
if __name__=="__main__":asyncio.run(main())
