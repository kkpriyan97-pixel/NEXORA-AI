import asyncio,json,logging,os,time
from typing import Any
from telegram import Update
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
STATE={"status":"starting","assets":[],"prices":{},"candles":{},"analyses":{},"read_only":True,"cycle":0,"last_cycle":None}
CANDLE_FETCH_SEM=asyncio.Semaphore(2)
CANDLE_FETCH_LAST={}
CANDLE_FETCH_INTERVAL=60.0
AI_REVIEW_CACHE={}
AI_REVIEW_TTL=12.0
AI_REVIEW_TIMEOUT=4.0
CLIENT=None
LOCK=asyncio.Lock()

def pair_name(x):
    return str(x.get("pair") or x.get("p") or x.get("symbol") or x.get("instrument") or x.get("id") or "")

def display_name(x):
    for k in ("title","name","display_name","displayName"):
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
    # OlympTrade's asset list contains the normal Flex Time instruments
    # alongside special products. Prefer explicit metadata when available.
    if explicit_quickler and not explicit_flex:
        return False
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
        v=prof.get(p,x.get("profitability",0))
        try: profitability=int(v)
        except Exception: profitability=0
        out.append({
            "pair":p,"display_name":title,"title":title,
            "signal_asset_label":f"{title} ({p})",
            "profitability":profitability,
            "locked":False,"locked_trading":False,"disabled":False,
            "mode":"OTC" if "_OTC" in p.upper() else "REAL",
            "trading_mode":"FLEX_TIME"
        })
    return out

async def telegram(text, chat_id=None):
    token=os.getenv("TELEGRAM_BOT_TOKEN","").strip()
    chat=str(chat_id or os.getenv("TELEGRAM_CHAT_ID","")).strip()
    if not token or not chat:
        log.warning("TELEGRAM_NOT_CONFIGURED")
        return False
    try:
        async with httpx.AsyncClient(timeout=8) as h:
            r=await h.post(f"https://api.telegram.org/bot{token}/sendMessage",json={"chat_id":chat,"text":text})
            if r.status_code >= 400:
                try: detail=r.json()
                except Exception: detail={"description":r.text[:200]}
                log.warning("TELEGRAM_SEND_FAILED status=%s description=%s",r.status_code,detail.get("description"))
                return False
            return True
    except Exception as e:
        log.warning("TELEGRAM_SEND_FAILED type=%s message=%s",type(e).__name__,str(e)[:200]);return False

async def on_tick(message):
    for t in message.get("d",[]) or []:
        if not isinstance(t,dict):continue
        p=str(t.get("p") or t.get("pair") or "")
        q=t.get("q");ts=t.get("t")
        if p and q is not None:
            try:STATE["prices"][p]=(float(q),float(ts) if ts is not None else time.time())
            except Exception:pass

async def refresh_candles(force=False):
    client=CLIENT;assets=list(STATE["assets"])
    if not client:return
    now=time.time()
    due=[a for a in assets if force or now-CANDLE_FETCH_LAST.get(a["pair"],0)>=CANDLE_FETCH_INTERVAL]
    async def one(a):
        p=a["pair"]
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
        p=a["pair"];price=STATE["prices"].get(p,(None,None))[0]
        an=analyze_asset(a,STATE["candles"].get(p,[]),price)
        if an:
            an["profitability"]=a["profitability"];STATE["analyses"][p]=an
        else:STATE["analyses"].pop(p,None)
    log.info("LIVE_ANALYSIS_REFRESH assets=%d fetched=%d qualified=%d",len(assets),len(due),len(STATE["analyses"]))

async def final_candidate():
    BRAIN.prune_expired_cooldowns()
    eligible=BRAIN.filter_candidates(STATE["assets"])
    raw=[STATE["analyses"][a["pair"]].copy() for a in eligible if a["pair"] in STATE["analyses"]]
    raw=rank_signal_candidates(raw)
    if not raw:return None
    # AI reviews the strongest technical candidates in parallel. Sequential reviews
    # consumed the final 40-second window (3-4 seconds per provider call), so one
    # candidate could reach the target while the remaining reviews were still running.
    top=raw[:8]
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
        if cached and time.time()-cached[0] < AI_REVIEW_TTL:
            d=cached[1]
        else:
            d=None
            try:
                d=await asyncio.wait_for(analyze_with_fallback(snap),timeout=AI_REVIEW_TIMEOUT)
                if d: AI_REVIEW_CACHE[cache_key]=(time.time(),d)
            except Exception as e:
                log.warning("AI_REVIEW_FAILED pair=%s type=%s message=%s",x["pair"],type(e).__name__,str(e)[:120])
        if d and int(d.get("confidence",0))>=90:
            y=x.copy()
            y.update({"confidence":int(d["confidence"]),"reason":d.get("reason") or x["reason"],"ai_provider":d.get("provider")})
            return y
        return None

    results=await asyncio.gather(*(review_one(x) for x in top),return_exceptions=True)
    for x,r in zip(top,results):
        if isinstance(r,Exception):
            log.warning("AI_REVIEW_TASK_FAILED pair=%s type=%s message=%s",x["pair"],type(r).__name__,str(r)[:120])
        elif r:
            reviewed.append(r)
    return rank_signal_candidates(reviewed)[0] if reviewed else None

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
    label=f"{rec['display_name']} ({rec['pair']})"
    icon={"WIN":"🟢","LOSS":"🔴","TIE":"🟡"}[rec["result"]]
    await telegram(f"━━━━━━━━━━━━━━━━━━━━\n🎯 CANDICE AI RESULT\n━━━━━━━━━━━━━━━━━━━━\n\n📊 ASSET: {label}\n➡️ DIRECTION: {rec['direction']}\n\n💰 ENTRY: {rec['entry_price']}\n💰 EXIT: {rec['exit_price']}\n⏱️ EXPIRY: {rec['expiry_minutes']} MIN\n\n{icon} {rec['result']}\n\n🧠 STRATEGY: {rec['strategy']}\n📈 15M TREND: {rec['trend_15m']}\n🕯️ 1M STRUCTURE: {rec['structure_1m']}\n\n🧠 Brain learning recorded\n━━━━━━━━━━━━━━━━━━━━")
    log.info("RESULT pair=%s result=%s exit=%s cooldown=%s",rec["pair"],rec["result"],rec["exit_price"],rec["result"]=="LOSS")

async def cycle_loop():
    # Exactly one decision cycle at a time. The final 40 seconds are a live
    # evaluation window; the entry is captured only at the exact 5-minute boundary.
    last_target=0
    while True:
        now=time.time(); target=(int(now)//300+1)*300
        if target<=last_target: target=last_target+300
        start=target-40
        await asyncio.sleep(max(0,start-time.time()))
        log.info("CYCLE_WINDOW_START cycle=%s start_utc=%s target_utc=%s start_uae=%s target_uae=%s",int(target//300),time.strftime("%H:%M:%S",time.gmtime(start)),time.strftime("%H:%M:%S",time.gmtime(target)),time.strftime("%H:%M:%S",time.gmtime(start+4*3600)),time.strftime("%H:%M:%S",time.gmtime(target+4*3600)))
        BRAIN.start_cycle(int(target//300)); STATE["cycle"]=int(target//300); last_target=target
        candidate=None
        while time.time()<target:
            await refresh_candles()
            try:
                candidate=await asyncio.wait_for(final_candidate(),timeout=max(1.0,target-time.time()))
            except asyncio.TimeoutError:
                log.warning("CYCLE_FINAL_EVALUATION_TIMEOUT cycle=%s remaining=%.2f",target//300,max(0,target-time.time())); candidate=None
            await asyncio.sleep(min(2,max(0,target-time.time())))
        # Exact target: refresh price/candles once more, then use fresh tick price.
        await refresh_candles()
        try:
            candidate=await asyncio.wait_for(final_candidate(),timeout=3.0)
        except asyncio.TimeoutError:
            log.warning("CYCLE_TARGET_FINAL_CHECK_TIMEOUT cycle=%s",target//300); candidate=None
        if candidate and BRAIN.can_send_cycle_signal():
            p=candidate["pair"]; entry=STATE["prices"].get(p,(None,None))[0]
            if entry is not None:
                ts=target
                s=BRAIN.mark_signal_sent(pair=p,display_name=candidate["display_name"],direction=candidate["direction"],expiry_minutes=candidate["expiry_minutes"],entry_price=entry,entry_ts=ts,entry_candle_ts=candidate["entry_candle_ts"],strategy=candidate["strategy"],reason=candidate["reason"],confidence=candidate["confidence"])
                key=f"{s.cycle_id}:{s.pair}:{s.entry_ts}"
                msg=(f"━━━━━━━━━━━━━━━━━━━━\\n🎯 CANDICE AI • LIVE MARKET\\n━━━━━━━━━━━━━━━━━━━━\\n\\n"
                     f"📊 ASSET: {s.display_name} ({s.pair})\\n➡️ DIRECTION: {s.direction}\\n\\n"
                     f"🕒 SIGNAL: {time.strftime('%H:%M:%S',time.localtime(ts))} UAE\\n"
                     f"🎯 TARGET: {time.strftime('%H:%M:%S',time.localtime(target))} UAE\\n"
                     f"⏳ SIGNAL COUNTDOWN: 00:00\\n\\n⏱️ EXPIRY: {s.expiry_minutes} MIN\\n"
                     f"💰 ENTRY: {s.entry_price}\\n\\n📈 15M TREND: {s.trend_15m}\\n"
                     f"🕯️ 1M STRUCTURE: {s.structure_1m}\\n🧠 STRATEGY: {s.strategy}\\n"
                     f"🎯 CONFIDENCE: {s.confidence}%\\n🟢 ACCOUNT: DEMO\\n\\n🧠 {s.reason}\\n━━━━━━━━━━━━━━━━━━━━")
                await telegram(msg); asyncio.create_task(result_watch(key))
                log.info("FINAL_SIGNAL cycle=%s pair=%s direction=%s confidence=%s",target//300,p,s.direction,s.confidence)
        else:
            log.info("NO_VALID_FINAL_SETUP cycle=%s",target//300)
        await asyncio.sleep(0.5)

async def market_worker():
    global CLIENT
    while True:
        token=os.getenv("OLYMPTRADE_ACCESS_TOKEN","").strip()
        if not token:STATE["status"]="waiting_for_token";await asyncio.sleep(30);continue
        client=OlympTradeClient(access_token=token,log_raw_messages=False);CLIENT=client;client.register_callback(parameters.E_TICK_UPDATE,on_tick)
        try:
            STATE["status"]="connecting";await client.start();STATE["status"]="connected"
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
            # Use the same authenticated profitability/availability stream that
            # produced the original working Flex comparison (historically seen
            # as 76 REAL + 37 OTC). This is the source-of-truth universe.
            flex_raw=event_records(client,182)
            source=flex_raw if flex_raw else raw
            assets=build_assets(client,source)
            STATE["assets"]=assets;STATE["status"]="live_read_only"
            real_n=sum(a["mode"]=="REAL" for a in assets); otc_n=sum(a["mode"]=="OTC" for a in assets)
            log.info("FLEX_UNIVERSE_SOURCE event=182 source_count=%d open_real=%d open_otc=%d open_total=%d",len(source),real_n,otc_n,len(assets))
            log.info("ALL_FLEX_OPEN_ASSETS_READY count=%d",len(assets))
            for a in assets:
                try:await client.market.subscribe_ticks(a["pair"])
                except Exception as e:log.debug("TICK_SUBSCRIBE_FAILED %s %s",a["pair"],e)
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
                if txt.lower().startswith("/start") and chat_id is not None:
                    sent=await telegram("✅ NEXORA AI is online.\n\nCandice Brain: LIVE\nMode: DEMO / Read-only", chat_id=chat_id)
                    log.info("TELEGRAM_START_RECEIVED chat_id=%s sent=%s",chat_id,sent)
            except Exception as e:
                log.warning("TELEGRAM_WEBHOOK_PARSE_FAILED %s",e)
        body_out=json.dumps({"service":"CANDICE-AI","status":STATE["status"],"read_only":True,"asset_count":len(STATE["assets"]),"qualified":len(STATE["analyses"]),"cycle":STATE["cycle"],"active_results":len(BRAIN.active_signals)}).encode()
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
