import asyncio,json,logging,os,time
from typing import Any
import httpx
from olymptrade_ws import OlympTradeClient
from olymptrade_ws.olympconfig import parameters
from brain_rules import BrainState,rank_signal_candidates
from candice_brain import analyze_asset
from ai_engine import snapshot_from_asset
from ai_router import analyze_with_fallback

logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s")
log=logging.getLogger("candice")
BRAIN=BrainState()
STATE={"status":"starting","assets":[],"prices":{},"candles":{},"analyses":{},"read_only":True,"cycle":0,"last_cycle":None}
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

def build_assets(client,raw):
    prof={}
    for x in event_records(client,182):
        p=pair_name(x);v=x.get("profitability")
        if p and isinstance(v,(int,float)):prof[p]=(int(v),x)
    out=[]
    for x in raw:
        p=pair_name(x)
        if not p or p not in prof:continue
        title=display_name(x) or display_name(prof[p][1])
        if not title:continue
        out.append({"pair":p,"display_name":title,"title":title,"signal_asset_label":f"{title} ({p})","profitability":prof[p][0],"locked":x.get("locked") is True,"locked_trading":x.get("locked_trading") is True,"mode":"OTC" if "_OTC" in p.upper() else "REAL"})
    return out

async def telegram(text):
    token=os.getenv("TELEGRAM_BOT_TOKEN","").strip();chat=os.getenv("TELEGRAM_CHAT_ID","").strip()
    if not token or not chat:
        log.warning("TELEGRAM_NOT_CONFIGURED")
        return False
    try:
        async with httpx.AsyncClient(timeout=8) as h:
            r=await h.post(f"https://api.telegram.org/bot{token}/sendMessage",json={"chat_id":chat,"text":text})
            r.raise_for_status();return True
    except Exception as e:
        log.warning("TELEGRAM_SEND_FAILED %s",e);return False

async def on_tick(message):
    for t in message.get("d",[]) or []:
        if not isinstance(t,dict):continue
        p=str(t.get("p") or t.get("pair") or "")
        q=t.get("q");ts=t.get("t")
        if p and q is not None:
            try:STATE["prices"][p]=(float(q),float(ts) if ts is not None else time.time())
            except Exception:pass

async def refresh_candles():
    client=CLIENT;assets=STATE["assets"]
    if not client:return
    sem=asyncio.Semaphore(8)
    async def one(a):
        async with sem:
            try:
                cs=await client.market.get_candles(a["pair"],size=60,count=60)
                if cs:STATE["candles"][a["pair"]]=cs
                p=STATE["prices"].get(a["pair"],(None,None))[0]
                an=analyze_asset(a,STATE["candles"].get(a["pair"],[]),p)
                if an:
                    an["profitability"]=a["profitability"];STATE["analyses"][a["pair"]]=an
                else:STATE["analyses"].pop(a["pair"],None)
            except Exception as e:log.debug("CANDLE_REFRESH_FAILED %s %s",a["pair"],e)
    await asyncio.gather(*(one(a) for a in assets))
    log.info("LIVE_ANALYSIS_REFRESH assets=%d qualified=%d",len(assets),len(STATE["analyses"]))

async def final_candidate():
    BRAIN.prune_expired_cooldowns()
    eligible=BRAIN.filter_candidates(STATE["assets"])
    raw=[STATE["analyses"][a["pair"]].copy() for a in eligible if a["pair"] in STATE["analyses"]]
    raw=rank_signal_candidates(raw)
    if not raw:return None
    # AI reviews the strongest technical candidates; fallback provider is automatic.
    reviewed=[]
    for x in raw[:8]:
        cs=STATE["candles"].get(x["pair"],[])
        price=STATE["prices"].get(x["pair"],(x.get("price"),None))[0]
        snap=snapshot_from_asset(next(a for a in eligible if a["pair"]==x["pair"]),cs,price,time.time())
        try:
            d=await analyze_with_fallback(snap)
            if d and int(d.get("confidence",0))>=90:
                x.update({"confidence":int(d["confidence"]),"reason":d.get("reason") or x["reason"],"ai_provider":d.get("provider")});reviewed.append(x)
        except Exception as e:log.warning("AI_REVIEW_FAILED pair=%s %s",x["pair"],e)
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
    while True:
        now=time.time();next_boundary=(int(now)//300+1)*300
        target=next_boundary
        start=target-40
        await asyncio.sleep(max(0,start-time.time()))
        cycle_id=target//300
        BRAIN.start_cycle(int(cycle_id));STATE["cycle"]=int(cycle_id)
        # Keep the 40-second window live: re-rank immediately before signal.
        candidate=await final_candidate()
        if candidate:
            p=candidate["pair"];entry=STATE["prices"].get(p,(None,None))[0]
            if entry is not None and BRAIN.can_send_cycle_signal():
                ts=time.time();s=BRAIN.mark_signal_sent(pair=p,display_name=candidate["display_name"],direction=candidate["direction"],expiry_minutes=candidate["expiry_minutes"],entry_price=entry,entry_ts=ts,entry_candle_ts=candidate["entry_candle_ts"],strategy=candidate["strategy"],reason=candidate["reason"],confidence=candidate["confidence"])
                key=f"{s.cycle_id}:{s.pair}:{s.entry_ts}"
                target_dt=time.strftime("%H:%M:%S",time.localtime(target))
                msg=f"━━━━━━━━━━━━━━━━━━━━\n🎯 CANDICE AI • LIVE MARKET\n━━━━━━━━━━━━━━━━━━━━\n\n📊 ASSET: {s.display_name} ({s.pair})\n➡️ DIRECTION: {s.direction}\n\n🕒 SIGNAL: {time.strftime('%H:%M:%S',time.localtime(ts))} UAE\n🎯 TARGET: {target_dt} UAE\n⏳ SIGNAL COUNTDOWN: 00:40\n\n⏱️ EXPIRY: {s.expiry_minutes} MIN\n💰 ENTRY: {s.entry_price}\n\n📈 15M TREND: {s.trend_15m}\n🕯️ 1M STRUCTURE: {s.structure_1m}\n🧠 STRATEGY: {s.strategy}\n🎯 CONFIDENCE: {s.confidence}%\n🟢 ACCOUNT: DEMO\n\n🧠 {s.reason}\n━━━━━━━━━━━━━━━━━━━━"
                await telegram(msg);asyncio.create_task(result_watch(key));log.info("FINAL_SIGNAL cycle=%s pair=%s direction=%s confidence=%s",cycle_id,p,s.direction,s.confidence)
        else:log.info("NO_VALID_FINAL_SETUP cycle=%s",cycle_id)
        # refresh full market evidence for the next cycle
        await refresh_candles()

async def market_worker():
    global CLIENT
    while True:
        token=os.getenv("OLYMPTRADE_ACCESS_TOKEN","").strip()
        if not token:STATE["status"]="waiting_for_token";await asyncio.sleep(30);continue
        client=OlympTradeClient(access_token=token,log_raw_messages=False);CLIENT=client;client.register_callback(parameters.E_TICK_UPDATE,on_tick)
        try:
            STATE["status"]="connecting";await client.start();STATE["status"]="connected"
            await asyncio.sleep(4)
            for m in client.get_cached_events(55):
                d=m.get("d") if isinstance(m,dict) else None
                if isinstance(d,list):
                    for a in d:
                        if isinstance(a,dict) and a.get("group")=="demo":client.account_id=a.get("account_id");client.account_group="demo";break
                if client.account_id:break
            raw=await client.market.get_available_assets(client.account_id);assets=build_assets(client,raw)
            STATE["assets"]=assets;STATE["status"]="live_read_only"
            log.info("ALL_ASSETS_READY count=%d",len(assets))
            for a in assets:
                try:await client.market.subscribe_ticks(a["pair"])
                except Exception as e:log.debug("TICK_SUBSCRIBE_FAILED %s %s",a["pair"],e)
            await refresh_candles()
            if not any(x.done() for x in []):pass
            while True:await asyncio.sleep(30)
        except Exception as e:
            STATE["status"]="error";log.exception("MARKET_WORKER_ERROR %s",e);await asyncio.sleep(15)
        finally:
            try:await client.stop()
            except Exception:pass
            CLIENT=None

async def health(reader,writer):
    try:
        await reader.read(2048)
        body=json.dumps({"service":"CANDICE-AI","status":STATE["status"],"read_only":True,"asset_count":len(STATE["assets"]),"qualified":len(STATE["analyses"]),"cycle":STATE["cycle"],"active_results":len(BRAIN.active_signals)}).encode()
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"+body);await writer.drain()
    finally:writer.close()

async def main():
    port=int(os.getenv("PORT","10000"));server=await asyncio.start_server(health,"0.0.0.0",port)
    await asyncio.gather(market_worker(),cycle_loop(),server.serve_forever())
if __name__=="__main__":asyncio.run(main())
