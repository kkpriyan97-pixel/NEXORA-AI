import asyncio,json,logging,os,time
from datetime import datetime,timezone
from zoneinfo import ZoneInfo
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

LEARNING_DB_URL=os.getenv("DATABASE_URL","").strip()

async def load_persistent_learning():
    if not LEARNING_DB_URL:
        log.warning("LEARNING_DB_NOT_CONFIGURED")
        return
    try:
        import psycopg
        def _load():
            with psycopg.connect(LEARNING_DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("CREATE TABLE IF NOT EXISTS candice_brain_learning (id SMALLINT PRIMARY KEY, state JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())")
                    cur.execute("SELECT state FROM candice_brain_learning WHERE id=1")
                    row=cur.fetchone()
                    if row: return row[0]
            return None
        data=await asyncio.to_thread(_load)
        if data:
            BRAIN.import_learning(data)
            log.info("LEARNING_STATE_LOADED total_results=%s",BRAIN.total_results)
        else:
            log.info("LEARNING_STATE_INITIALIZED total_results=0")
    except Exception as e:
        log.warning("LEARNING_STATE_LOAD_FAILED type=%s message=%s",type(e).__name__,str(e)[:180])

async def save_persistent_learning():
    if not LEARNING_DB_URL:
        return
    try:
        import psycopg
        data=BRAIN.export_learning()
        def _save():
            with psycopg.connect(LEARNING_DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("CREATE TABLE IF NOT EXISTS candice_brain_learning (id SMALLINT PRIMARY KEY, state JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())")
                    cur.execute("INSERT INTO candice_brain_learning(id,state) VALUES(1,%s) ON CONFLICT(id) DO UPDATE SET state=EXCLUDED.state,updated_at=NOW()",(json.dumps(data,separators=(",",":")),))
                db.commit()
        await asyncio.to_thread(_save)
        log.info("LEARNING_STATE_SAVED total_results=%s",BRAIN.total_results)
    except Exception as e:
        log.warning("LEARNING_STATE_SAVE_FAILED type=%s message=%s",type(e).__name__,str(e)[:180])
BRAIN=BrainState()
UAE_TZ=ZoneInfo("Asia/Dubai")

def uae_time(ts):
    return datetime.fromtimestamp(float(ts),tz=UAE_TZ).strftime("%H:%M:%S")
STATE={"status":"starting","assets":[],"prices":{},"price_source":{},"candles":{},"analyses":{},"network":{},"read_only":True,"cycle":0,"last_cycle":None,"account_id":None,"account_group":"demo","feed_source":"authenticated_websocket"}
CANDLE_FETCH_SEM=asyncio.Semaphore(8)
CANDLE_FETCH_LAST={}
CANDLE_FETCH_INTERVAL=60.0
CANDLE_FETCH_RETRIES=3
CANDLE_FETCH_RETRY_DELAY=0.25
# Some account assets are visible in the account feed but may not expose a
# fresh 1-minute candle through this read-only candle endpoint. Back off only
# those assets after a rejected first dataset so they cannot consume the
# exact signal window; previously healthy assets keep the existing retry path.
CANDLE_UNAVAILABLE_UNTIL={}
CANDLE_UNAVAILABLE_BACKOFF=300.0
CANDLE_GOOD_ONCE=set()
TICK_RESUB_SEM=asyncio.Semaphore(3)
TICK_RESUB_TIMEOUT=1.5
LIVE_TICK_MAX_AGE=5.0
QUOTE_SNAPSHOT_MAX_AGE=6.0
QUOTE_SNAPSHOT_REFRESH=3.5
QUOTE_SNAPSHOT_SEM=asyncio.Semaphore(32)
QUOTE_SNAPSHOT_LAST={}
AI_REVIEW_CACHE={}
AI_REVIEW_TTL=90.0
AI_REVIEW_FAIL_TTL=20.0
# Preserve a fully reviewed candidate for the short exact-boundary window.
# This prevents a transient provider/cache refresh from erasing a valid setup
# after it has already passed the Brain + live-price gates.
CANDIDATE_CACHE={}
CANDIDATE_CACHE_TTL=75.0
AI_PROVIDER_COOLDOWN={}
AI_REVIEW_TIMEOUT=2.4
# Rotating account-wide live quote scan. It does not touch Brain timing; it only
# keeps current account prices warm for analysis/candidate selection.
ACCOUNT_LIVE_SCAN_BATCH=16
ACCOUNT_LIVE_SCAN_INTERVAL=1.0
ACCOUNT_LIVE_SCAN_CURSOR=0
# Account-wide event-1 tick subscription manager. Subscriptions are read-only;
# the worker only requests market quotes and never places/modifies trades.
ACCOUNT_TICK_SUB_SEM=asyncio.Semaphore(4)
ACCOUNT_TICK_SUB_BATCH=8
ACCOUNT_TICK_SUB_RETRY=900.0
ACCOUNT_TICK_SUBSCRIBED=set()
ACCOUNT_TICK_LAST_ATTEMPT={}
CLIENT=None
LOCK=asyncio.Lock()

def extract_account_ids(value, group="demo"):
    """Collect account IDs from authenticated account/balance payloads without logging secrets."""
    found=[]
    def walk(v):
        if isinstance(v,dict):
            g=v.get("group",v.get("account_group",v.get("accountGroup")))
            aid=v.get("account_id",v.get("accountId"))
            if aid is not None and (g is None or str(g).lower()==str(group).lower()):
                try: found.append(int(aid))
                except Exception: pass
            for child in v.values():
                if isinstance(child,(dict,list)):
                    walk(child)
        elif isinstance(v,list):
            for child in v:
                if isinstance(child,(dict,list)):
                    walk(child)
    walk(value)
    return sorted(set(found))

def extract_identity_fields(value):
    """Return only identity-like numeric fields for safe authentication diagnostics."""
    out={}
    keys={"account_id","accountId","user_id","userId","uid","customer_id","customerId","client_id","clientId"}
    def walk(v,path="root"):
        if isinstance(v,dict):
            for k,val in v.items():
                if k in keys:
                    try:
                        out[f"{path}.{k}"]=int(val)
                    except (TypeError,ValueError):
                        pass
                if isinstance(val,(dict,list)):
                    walk(val,f"{path}.{k}")
        elif isinstance(v,list):
            for idx,val in enumerate(v):
                if isinstance(val,(dict,list)):
                    walk(val,f"{path}[{idx}]")
    walk(value)
    return out

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

# The following is the exact OPEN asset set from the account screenshots the user
# supplied. Closed/hidden assets are deliberately not included. The broker feed
# remains the source for live prices, but it is NOT allowed to widen this universe.
SCREENSHOT_OPEN_ASSETS={
    # OTC currency/metals
    "eurcad otc","audnzd otc","gbpjpy otc","cadchf otc","chfjpy otc",
    "gbpaud otc","eurjpy otc","eurchf otc","eurnzd otc","gbpchf otc",
    "nzdcad otc","nzdjpy otc","gbpnzd otc","nzdchf otc","silver otc",
    "euraud otc","eurusd otc","audusd otc","usdchf otc","gold otc",
    "usdcad otc","nzdusd otc","audcad otc","gbpusd otc","gbpcad otc",
    "usdjpy otc","audchf otc","cadjpy otc","eurgbp otc","audjpy otc",
    # Composite / index assets visible in the screenshots
    "asia composite index","europe composite index","football champions 2026 index",
    "compound index","halal market axis","quickler","stable tick index",
    "arabian general index","oasis index","qahwa index",
}

def asset_key(value):
    text=_norm_text(value)
    return " ".join(text.replace("/"," ").replace("_"," ").split())

def screenshot_asset_allowed(x):
    if not isinstance(x,dict): return False
    p=pair_name(x)
    title=display_name(x)
    # Match the account-facing name first; pair is only a fallback because
    # some composite products expose internal tickers instead of their UI name.
    if asset_key(title) in SCREENSHOT_OPEN_ASSETS:
        return True
    return asset_key(p) in SCREENSHOT_OPEN_ASSETS

def is_flex_time_asset(x):
    if not isinstance(x,dict): return False
    # The authenticated account asset feed is the source of truth.
    # Do not apply the old screenshot allow-list here: the user wants every
    # asset currently available on the Olymp Trade account to be visible to
    # Candice, using the account-facing name exactly as returned.
    return True

def build_assets(client,raw):
    # Account-scoped assets are authoritative. Keep every currently available
    # asset, while preserving the exact account-facing display name.
    prof={}
    for x in raw or []:
        if not isinstance(x,dict): continue
        p=pair_name(x)
        v=x.get("profitability")
        if p and isinstance(v,(int,float)): prof[p]=int(v)

    out=[]; seen=set(); rejected=[]
    for x in raw or []:
        if not isinstance(x,dict) or not is_flex_time_asset(x):
            p=pair_name(x) if isinstance(x,dict) else ""
            if p: rejected.append(p)
            continue
        p=pair_name(x)
        if not p or p in seen: continue
        if x.get("disabled") is True or x.get("locked") is True or x.get("locked_trading") is True:
            continue
        seen.add(p)
        # Preserve the exact name shown by the authenticated account.
        # Only fall back to the internal pair when the broker provides no
        # human-readable account-facing name.
        title=display_name(x) or p
        v=prof.get(p,x.get("profitability",0))
        try: profitability=int(v)
        except Exception: profitability=0
        quickler=(p.upper()=="ULTRA_X" or "quickler" in " ".join(
            _norm_text(x.get(k)) for k in
            ("pair","symbol","name","title","display_name","displayName",
             "product","category","instrument_type","expiration_type","expiration_mode")
        ))
        out.append({
            "pair":p,"display_name":title,"title":title,
            "signal_asset_label":title,"profitability":profitability,
            "locked":False,"locked_trading":False,"disabled":False,
            "mode":"OTC" if "_OTC" in p.upper() else "REAL",
            "trading_mode":"FLEX_TIME","signal_eligible":not quickler
        })
    log.info("ACCOUNT_ASSET_FILTER raw=%d accepted=%d rejected=%d",len(raw or []),len(out),len(rejected))
    if rejected:
        log.info("ACCOUNT_ASSET_REJECTED sample=%s",rejected[:25])
    # Audit the exact account-facing names that Candice accepted.
    log.info("ACCOUNT_ASSET_NAMES %s",[a["display_name"] for a in out])
    return out

async def sync_account_assets(client, reason="periodic"):
    """Refresh account-visible assets directly from the authenticated WebSocket session."""
    if not client or not client.account_id:
        return False
    try:
        response=await asyncio.wait_for(
            client.send_request(parameters.E_ASSET_PROFITABILITY,[{"account_id":client.account_id}],
                                requires_response=True,timeout=8.0),
            timeout=9.0
        )
        raw=response.get("d") if isinstance(response,dict) else None
    except Exception as e:
        log.warning("ACCOUNT_ASSET_SYNC_FAILED account_id=%s reason=%s type=%s message=%s",
                    client.account_id,reason,type(e).__name__,str(e)[:160])
        return False

    if not isinstance(raw,list) or not raw:
        log.warning("ACCOUNT_ASSET_SYNC_EMPTY account_id=%s reason=%s keep_count=%d",
                    client.account_id,reason,len(STATE["assets"]))
        return False

    assets=build_assets(client,raw)
    if not assets:
        log.warning("ACCOUNT_ASSET_SYNC_ZERO account_id=%s reason=%s keep_count=%d",
                    client.account_id,reason,len(STATE["assets"]))
        return False

    old={a["pair"] for a in STATE["assets"]}
    new={a["pair"] for a in assets}
    added=sorted(new-old)
    removed=sorted(old-new)
    STATE["assets"]=assets
    STATE["account_id"]=client.account_id
    STATE["account_group"]="demo"
    STATE["feed_source"]="authenticated_websocket:event_182"

    for pair in removed:
        STATE["prices"].pop(pair,None)
        STATE["price_source"].pop(pair,None)
        STATE["candles"].pop(pair,None)
        STATE["analyses"].pop(pair,None)
        QUOTE_SNAPSHOT_LAST.pop(pair,None)
        CANDLE_FETCH_LAST.pop(pair,None)
        CANDLE_UNAVAILABLE_UNTIL.pop(pair,None)
        CANDLE_GOOD_ONCE.discard(pair)

    log.info("ACCOUNT_ASSET_SYNC source=authenticated_websocket:event_182 account_id=%s reason=%s raw=%d accepted=%d added=%d removed=%d total=%d",
             client.account_id,reason,len(raw),len(assets),len(added),len(removed),len(assets))
    if added: log.info("ACCOUNT_ASSET_ADDED sample=%s",added[:25])
    if removed: log.info("ACCOUNT_ASSET_REMOVED sample=%s",removed[:25])
    return True

async def on_asset_update(message):
    """Apply event-183 updates only to assets already admitted by the account-scoped scan."""
    raw=message.get("d") if isinstance(message,dict) else None
    if not isinstance(raw,list) or not raw:
        return
    incoming=build_assets(CLIENT,raw)
    if not incoming:
        return
    current={a["pair"]:a for a in STATE["assets"]}
    updated=0
    ignored=0
    for a in incoming:
        p=a["pair"]
        if p in current:
            current[p]=a
            updated+=1
        else:
            # event 183 is an update stream, not an authority to widen the account universe.
            ignored+=1
    STATE["assets"]=list(current.values())
    STATE["feed_source"]="authenticated_websocket:event_183"
    log.info("ACCOUNT_ASSET_FEED_UPDATE source=authenticated_websocket:event_183 updated=%d ignored_unknown=%d visible=%d",
             updated,ignored,len(STATE["assets"]))


async def scan_account_live_feed():
    """Audit account-wide live coverage from the authenticated event-1 tick stream only."""
    assets=list(STATE["assets"])
    if not CLIENT or not assets:
        return
    now=time.time()
    total=len(assets)
    fresh=sum(1 for a in assets if has_fresh_live_price(a["pair"],now,LIVE_TICK_MAX_AGE))
    missing=total-fresh
    log.info("ACCOUNT_LIVE_FEED_SCAN source=authenticated_websocket:event_1 assets=%d fresh_tick=%d missing=%d",
             total,fresh,missing)


async def account_live_feed_worker():
    while True:
        try:
            await scan_account_live_feed()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("ACCOUNT_LIVE_FEED_WORKER_ERROR type=%s message=%s",type(e).__name__,str(e)[:160])
        await asyncio.sleep(1.0)


async def ensure_account_tick_subscriptions():
    """Subscribe each signal-eligible account asset to the authenticated event-1 quote stream."""
    client=CLIENT
    if not client or not client.connection.is_connected:
        return
    assets=[a for a in list(STATE["assets"]) if a.get("signal_eligible",True) and a.get("pair")]
    now=time.time()
    pairs=[]
    for a in assets:
        p=a["pair"]
        if p in ACCOUNT_TICK_SUBSCRIBED:
            continue
        if now-ACCOUNT_TICK_LAST_ATTEMPT.get(p,0.0) < ACCOUNT_TICK_SUB_RETRY:
            continue
        ACCOUNT_TICK_LAST_ATTEMPT[p]=now
        pairs.append(p)
        if len(pairs)>=ACCOUNT_TICK_SUB_BATCH:
            break
    if not pairs:
        return

    accepted=0
    rejected=[]
    async def one(pair):
        nonlocal accepted
        async with ACCOUNT_TICK_SUB_SEM:
            try:
                await asyncio.wait_for(client.market.subscribe_ticks(pair),timeout=6.0)
                ACCOUNT_TICK_SUBSCRIBED.add(pair)
                accepted+=1
                log.info("ACCOUNT_TICK_SUBSCRIBE pair=%s status=accepted",pair)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                rejected.append((pair,type(e).__name__,str(e)[:120]))
                log.warning("ACCOUNT_TICK_SUBSCRIBE pair=%s status=rejected type=%s message=%s",
                            pair,type(e).__name__,str(e)[:120])

    results=await asyncio.gather(*(one(p) for p in pairs),return_exceptions=True)
    for p,r in zip(pairs,results):
        if isinstance(r,asyncio.CancelledError):
            rejected.append((p,"CancelledError","subscription task cancelled"))
        elif isinstance(r,BaseException):
            rejected.append((p,type(r).__name__,str(r)[:120]))
    log.info("ACCOUNT_TICK_SUBSCRIBE_BATCH requested=%d accepted=%d rejected=%d active=%d",
             len(pairs),accepted,len(rejected),len(ACCOUNT_TICK_SUBSCRIBED))
    if rejected:
        log.info("ACCOUNT_TICK_SUBSCRIBE_REJECTED sample=%s",rejected[:12])

async def account_tick_subscription_worker():
    while True:
        try:
            if CLIENT and CLIENT.connection.is_connected:
                await ensure_account_tick_subscriptions()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("ACCOUNT_TICK_SUBSCRIPTION_WORKER_ERROR type=%s message=%s",
                        type(e).__name__,str(e)[:160])
        await asyncio.sleep(5.0)

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
    """Refresh candidate quotes from the authenticated event-1 stream, never from a snapshot API."""
    client=CLIENT
    if not client or not pairs:
        return 0
    await ensure_candidate_ticks(pairs)
    await asyncio.sleep(0.20)
    fresh=sum(1 for p in pairs if has_fresh_live_price(p,time.time(),LIVE_TICK_MAX_AGE))
    log.info("LIVE_PRICE_EVENT1_REFRESH requested=%d fresh=%d",len(set(pairs)),fresh)
    return fresh


def _candle_epoch(c):
    """Return candle timestamp in epoch seconds, accepting seconds or milliseconds."""
    try:
        v=float(c.get("time",c.get("t")))
        if v>10000000000: v/=1000.0
        return v
    except Exception:
        return None

def _closed_candles(candles,reference_ts=None):
    """Keep only completed 1-minute candles; never analyze the live candle."""
    boundary=(int(time.time() if reference_ts is None else reference_ts)//60)*60
    out=[]
    for c in candles or []:
        ts=_candle_epoch(c)
        if ts is not None and ts < boundary:out.append(c)
    return out

def _candle_closed_at(c):
    """Return the actual close time of a 1-minute candle.

    The broker's candle timestamp marks the candle START, not its close. A candle
    stamped 09:57:00 closes at 09:58:00. Freshness must therefore be measured
    from start+60s, while the closed-candle gate above remains unchanged.
    """
    ts=_candle_epoch(c)
    return None if ts is None else ts+60.0

def _candle_data_stale(pair,reference_ts=None):
    now=time.time() if reference_ts is None else float(reference_ts)
    closed=_closed_candles(STATE["candles"].get(pair,[]),now)
    if not closed:return True
    closed_at=_candle_closed_at(closed[-1])
    return closed_at is None or (now-closed_at)>75.0

async def refresh_candles(force=False):
    client=CLIENT;assets=list(STATE["assets"])
    if not client:return
    now=time.time()
    due=[a for a in assets
         if (force or now-CANDLE_FETCH_LAST.get(a["pair"],0)>=CANDLE_FETCH_INTERVAL or _candle_data_stale(a["pair"],now))
         and now>=CANDLE_UNAVAILABLE_UNTIL.get(a["pair"],0.0)]

    async def one(a):
        p=a["pair"]
        if not a.get("signal_eligible",True):return
        async with CANDLE_FETCH_SEM:
            last_reason="unknown"
            for attempt in range(1,CANDLE_FETCH_RETRIES+1):
                try:
                    await asyncio.sleep(0.15 if attempt==1 else CANDLE_FETCH_RETRY_DELAY)
                    cs=await asyncio.wait_for(
                        client.market.get_candles(p,size=60,count=60),
                        timeout=2.0
                    )
                    normalized=[]
                    if isinstance(cs,list):
                        for item in cs:
                            if isinstance(item,dict) and isinstance(item.get("candles"),list):
                                normalized.extend(x for x in item["candles"] if isinstance(x,dict))
                            elif isinstance(item,dict) and any(k in item for k in ("open","o","high","h","low","l","close","c")):
                                normalized.append(item)
                    if normalized:
                        try:normalized.sort(key=lambda x: float(x.get("time",x.get("t",0))))
                        except Exception:pass

                        # Never replace a good live candle set with an older broker
                        # response. The broker can occasionally return a cached
                        # candle page even though the websocket session is live.
                        reference=time.time()
                        closed=_closed_candles(normalized,reference)
                        newest=_candle_epoch(closed[-1]) if closed else None
                        closed_at=_candle_closed_at(closed[-1]) if closed else None
                        age=(reference-closed_at) if closed_at is not None else None
                        if newest is not None and age is not None and 0 <= age <= 75.0 and len(closed)>=45:
                            STATE["candles"][p]=normalized
                            CANDLE_FETCH_LAST[p]=time.time()
                            CANDLE_GOOD_ONCE.add(p)
                            CANDLE_UNAVAILABLE_UNTIL.pop(p,None)
                            if attempt>1:
                                log.info("CANDLE_REFRESH_RECOVERED pair=%s attempt=%d closed=%d newest_age=%.1f",
                                         p,attempt,len(closed),age)
                            return
                        last_reason=f"stale_response closed={len(closed)} newest_age={age}"
                    else:
                        last_reason="empty_response"
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    last_reason=f"{type(e).__name__}:{str(e)[:100]}"
                if attempt<CANDLE_FETCH_RETRIES:
                    await asyncio.sleep(CANDLE_FETCH_RETRY_DELAY)

            # Keep the previous good dataset instead of overwriting it with stale
            # data. Mark the fetch due again so the next cycle retries promptly.
            CANDLE_FETCH_LAST[p]=0.0
            if p not in CANDLE_GOOD_ONCE:
                CANDLE_UNAVAILABLE_UNTIL[p]=time.time()+CANDLE_UNAVAILABLE_BACKOFF
            log.warning("CANDLE_REFRESH_REJECTED pair=%s attempts=%d reason=%s",
                        p,CANDLE_FETCH_RETRIES,last_reason)

    await asyncio.gather(*(one(a) for a in due),return_exceptions=True)

    reference=time.time()
    for a in assets:
        if not a.get("signal_eligible",True):continue
        p=a["pair"]
        price=STATE["prices"].get(p,(None,None))[0]
        closed=_closed_candles(STATE["candles"].get(p,[]),reference)
        an=analyze_asset(a,closed,price)
        if an:
            an["profitability"]=a["profitability"]
            STATE["analyses"][p]=an
        else:
            STATE["analyses"].pop(p,None)

    stale_count=sum(
        1 for a in assets
        if a.get("signal_eligible",True) and _candle_data_stale(a["pair"],reference)
    )
    log.info("LIVE_ANALYSIS_REFRESH assets=%d signal_eligible=%d fetched=%d stale=%d qualified=%d",
             len(assets),sum(1 for a in assets if a.get("signal_eligible",True)),
             len(due),stale_count,len(STATE["analyses"]))

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
    analyzed=[STATE["analyses"][a["pair"]].copy() for a in eligible if a["pair"] in STATE["analyses"]]
    adapted=[BRAIN.adaptive_candidate(x) for x in analyzed]
    raw=rank_signal_candidates(adapted)
    if not raw:
        if adapted:
            top_debug=max(adapted,key=lambda x:(int(x.get("confidence") or 0),float(x.get("market_quality") or 0)))
            log.info("CANDIDATE_GATE_REJECTED analyzed=%d top_pair=%s top_confidence=%s top_quality=%s strategy=%s",
                     len(adapted),top_debug.get("pair"),top_debug.get("confidence"),
                     top_debug.get("market_quality"),top_debug.get("strategy"))
        else:
            log.info("CANDIDATE_GATE_REJECTED analyzed=0 reason=no_closed_candle_setup")
        return None
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
        closed=_closed_candles(cs,now)
        if len(closed)<45:
            return None
        snap=snapshot_from_asset(asset,closed,price,now)
        cache_key=(x["pair"],str(x.get("entry_candle_ts")),x.get("direction"))

        # Candice Brain is the PRIMARY decision engine. External LLM providers
        # are validation only and must never block a qualified local setup.
        # This prevents an OpenAI/Gemini quota outage from deleting the
        # 30-second signal window.
        local_confidence=int(x.get("confidence") or 0)
        if local_confidence>=90:
            y=x.copy()
            y.update({
                "confidence":local_confidence,
                "reason":x.get("reason") or "Candice local Brain verified closed-candle evidence",
                "ai_provider":"CANDICE_LOCAL_BRAIN_PRIMARY"
            })
            CANDIDATE_CACHE[cache_key]=(time.time(),y.copy())
            log.info("BRAIN_PRIMARY_CANDIDATE pair=%s confidence=%s strategy=%s",
                     x["pair"],local_confidence,x.get("strategy"))
            return y

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
            CANDIDATE_CACHE[cache_key]=(time.time(),y.copy())
            return y
        # When every external LLM provider is unavailable, preserve the live
        # evidence-first Candice Brain decision instead of losing the whole
        # 5-minute cycle. This fallback never bypasses the 90% Brain threshold,
        # the live-candle evidence, the 15m conflict gate, or DEMO/read-only mode.
        # An external provider can return a valid JSON decision but with
        # confidence below Candice's 90% send threshold. Treat that the same as
        # provider unavailability for the final gate: the evidence-first local
        # Brain result remains eligible if it already passed the same 90%
        # technical threshold. Do not downgrade a qualified market setup merely
        # because an external provider produced a low-confidence review.
        external_confidence=int(d.get("confidence",0)) if isinstance(d,dict) else 0
        if not use_cached_only and external_confidence < 90 and int(x.get("confidence") or 0) >= 90:
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
            CANDIDATE_CACHE[cache_key]=(time.time(),y.copy())
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
    if ranked:
        return ranked[0]

    # Exact-boundary resilience: if this cycle already completed a valid Brain
    # + AI/local review for the same candle, reuse that completed decision for a
    # few seconds instead of re-running the provider at the deadline. The live
    # quote is still required, so this cannot send on stale market data.
    cached_candidates=[]
    now_cache=time.time()
    for x in top:
        key=(x["pair"],str(x.get("entry_candle_ts")),x.get("direction"))
        cached=CANDIDATE_CACHE.get(key)
        if not cached:
            continue
        cached_at,candidate=cached
        if now_cache-cached_at > CANDIDATE_CACHE_TTL:
            CANDIDATE_CACHE.pop(key,None)
            continue
        if int(candidate.get("confidence") or 0) < 90:
            continue
        if require_live_price and not has_fresh_live_price(candidate["pair"],now_cache,QUOTE_SNAPSHOT_MAX_AGE):
            continue
        cached_candidates.append(candidate.copy())
    cached_ranked=rank_signal_candidates(cached_candidates)
    if cached_ranked:
        log.info("FINAL_CANDIDATE_CACHE_FALLBACK count=%d pair=%s confidence=%s",
                 len(cached_ranked),cached_ranked[0]["pair"],cached_ranked[0].get("confidence"))
        return cached_ranked[0]
    log.info("FINAL_CANDIDATE_NONE raw=%d top=%d reviewed=%d require_live=%s",
             len(raw),len(top),len(reviewed),require_live_price)
    return None

async def result_watch(key):
    s=BRAIN.active_signals.get(key)
    if not s:return
    await asyncio.sleep(max(0,s.expiry_minutes*60-(time.time()-s.entry_ts)))

    # Result verification is based only on a completed candle.
    expiry_price=None
    expiry_source=""
    client=CLIENT

    for attempt in range(1,5):
        try:
            if client:
                raw=await asyncio.wait_for(
                    client.market.get_candles(s.pair,size=60,count=60),
                    timeout=2.0
                )
                normalized=[]
                if isinstance(raw,list):
                    for item in raw:
                        if isinstance(item,dict) and isinstance(item.get("candles"),list):
                            normalized.extend(x for x in item["candles"] if isinstance(x,dict))
                        elif isinstance(item,dict) and any(k in item for k in ("open","o","high","h","low","l","close","c")):
                            normalized.append(item)
                if normalized:
                    try:normalized.sort(key=lambda x: float(x.get("time",x.get("t",0))))
                    except Exception:pass
                    closed=_closed_candles(normalized,time.time())
                    if closed:
                        expiry_price=float(closed[-1].get("close",closed[-1].get("c")))
                        STATE["candles"][s.pair]=normalized
                        expiry_source="candle-closed"
                        break
        except Exception as e:
            log.warning("RESULT_CANDLE_READ_FAILED pair=%s attempt=%d type=%s message=%s",
                        s.pair,attempt,type(e).__name__,str(e)[:120])
        if attempt<4:
            await asyncio.sleep(0.5)

    if expiry_price is None:
        log.warning("RESULT_PENDING_NO_CLOSED_CANDLE pair=%s entry=%s",s.pair,s.entry_price)
        if key in BRAIN.active_signals:
            await asyncio.sleep(1.0)
            return await result_watch(key)
        return

    rec=BRAIN.finish_signal(key,expiry_price)
    await save_persistent_learning()
    label=rec["display_name"]
    direction_icon="⬆️" if rec["direction"]=="UP" else "⬇️"
    result_icon={"WIN":"✅","LOSS":"🔴","TIE":"🟡"}[rec["result"]]
    await telegram(
        f"📊 TRADE RESULT\n"
        f"\n"
        f"📈 {label}\n"
        f"\n"
        f"{direction_icon} {rec['direction']}\n"
        f"\n"
        f"💰 Entry: {rec['entry_price']}\n"
        f"🏁 Expiry: {rec['exit_price']}\n"
        f"⏱️ Duration: {rec['expiry_minutes']} MIN\n"
        f"🧠 Brain Strategy: {rec['strategy'] or '—'}\n"
        f"🧬 Self Strategy: {rec.get('self_strategy') or '—'} ({rec.get('self_strategy_version') or '—'})\n"
        f"🎯 Brain Confidence: {rec['confidence']}%\n"
        f"📈 15m Trend: {rec['trend_15m'] or '—'}\n"
        f"🕯️ 1m Structure: {rec['structure_1m'] or '—'}\n"
        f"🔎 Verification: candle-closed\n"
        f"\n"
        f"{result_icon} {rec['result']}\n"
        f"\n"
        f"⚠️ RESULT ONLY — AUTO TRADE OFF"
    )
    log.info(
        "RESULT pair=%s result=%s strategy=%s self_strategy=%s self_version=%s confidence=%s trend=%s structure=%s pattern=%s entry=%s exit=%s source=%s cooldown=%s",
        rec["pair"],rec["result"],rec["strategy"],rec.get("self_strategy",""),rec.get("self_strategy_version",""),rec["confidence"],rec["trend_15m"],
        rec["structure_1m"],rec["pattern"],rec["entry_price"],rec["exit_price"],
        expiry_source,rec["result"]=="LOSS"
    )

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
            strategy=candidate["strategy"],reason=candidate["reason"],confidence=confidence,
            pattern=str(candidate.get("pattern") or ""),
            trend_15m=str(candidate.get("trend_15m") or ""),
            structure_1m=str(candidate.get("structure_1m") or ""),
            self_strategy=str(candidate.get("self_strategy") or ""),
            self_strategy_version=str(candidate.get("self_strategy_version") or "")
        )
        key=f"{s.cycle_id}:{s.pair}:{s.entry_ts}"
        msg=(f"🚨 CANDICE AI SIGNAL\n\n"
             f"👋 Market setup detected!\n\n"
             f"📊 {s.display_name}\n\n"
             f"<b>{'🔻 DOWN' if s.direction.upper() == 'DOWN' else '🟢 UP'}</b>\n"
             f"<b>⏱️ {s.expiry_minutes} MIN EXPIRY</b>\n\n"
             f"🕒 {uae_time(ts)} UAE\n"
             f"🎯 Entry → {uae_time(target)}\n\n"
             f"💰 {s.entry_price}\n"
             f"🎯 Confidence → {s.confidence}%\n\n"
             f"📈 Trend → {s.trend_15m or '—'}\n"
             f"🕯️ Structure → {s.structure_1m or '—'}\n"
             f"🧠 Strategy → {s.strategy}\n\n"
             f"🟢 DEMO • READ ONLY\n"
             f"🤖 CANDICE BRAIN")
        log.info(
            "FINAL_SIGNAL cycle=%s pair=%s direction=%s strategy=%s confidence=%s price_source=%s "
            "trend=%s structure=%s pattern=%s self_strategy=%s self_version=%s expiry=%s entry_candle=%s signal_utc=%s target_utc=%s lead_seconds=%.3f",
            int(target//300),s.pair,s.direction,s.strategy or "UNKNOWN",s.confidence,
            STATE["price_source"].get(s.pair,"unknown"),
            s.trend_15m or "UNKNOWN",s.structure_1m or "UNKNOWN",s.pattern or "UNKNOWN",
            getattr(s,"self_strategy","UNKNOWN"),getattr(s,"self_strategy_version","UNKNOWN"),
            s.expiry_minutes,s.entry_candle_ts,
            datetime.fromtimestamp(ts,tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3],
            datetime.fromtimestamp(target,tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3],
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
        # Refresh closed-candle analysis at most once per UTC minute. The previous
        # loop refreshed all 28 assets every ~2s; stale broker responses then
        # consumed the entire pre-signal window and the valid candidate never
        # reached the exact 30s send point.
        refreshed_minute=None
        # Use the pre-signal window for analysis, but never let repeated candle
        # fetches block the exact 30-second signal deadline.
        while time.time() < signal_at-0.75:
            current_minute=int(time.time())//60
            if refreshed_minute != current_minute:
                try:
                    await refresh_candles()
                    refreshed_minute=current_minute
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

        # final_candidate(require_live_price=True) already checked the quote freshness.
        # Avoid another blocking network request here so the exact 30-second deadline
        # remains deterministic.
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
        try:
            expected_account_id=int(os.getenv("OLYMPTRADE_ACCOUNT_ID","128175463").strip())
        except (TypeError,ValueError):
            STATE["status"]="invalid_account_id"
            log.error("INVALID_OLYMPTRADE_ACCOUNT_ID")
            await asyncio.sleep(30)
            continue
        # Bind the intended demo account at client construction time and let
        # the library validate it against the authenticated e:55 session.
        client=OlympTradeClient(
            access_token=token,
            log_raw_messages=False,
            account_id=expected_account_id,
            account_group="demo",
        )
        CLIENT=client
        ACCOUNT_TICK_SUBSCRIBED.clear()
        ACCOUNT_TICK_LAST_ATTEMPT.clear()
        client.register_callback(parameters.E_TICK_UPDATE,on_tick)
        client.register_callback(parameters.E_ASSET_PROFITABILITY_UPDATE,on_asset_update)
        try:
            STATE["status"]="connecting"
            # Network geolocation is diagnostic only and can be rate-limited;
            # it must never block authenticated market connectivity.
            await client.start()
            STATE["status"]="connected"
            await client.initialize_session()

            # e:55 is the authenticated account/balance source. Do not use the
            # speculative e:1068 request as an identity oracle.
            event55_accounts=[]
            for msg in client.get_cached_events(parameters.E_BALANCE_UPDATE):
                event55_accounts.extend(extract_account_ids(msg,"demo"))
            if not event55_accounts:
                event55_accounts.extend(
                    extract_account_ids(client.current_balance,"demo")
                )

            demo_accounts=sorted(set(event55_accounts))
            log.info(
                "TOKEN_DEMO_ACCOUNTS_EXPOSED source=event55 count=%d ids=%s",
                len(demo_accounts),demo_accounts
            )

            if expected_account_id not in demo_accounts:
                # Diagnostic only: compare identity-like fields from the
                # authenticated account/balance and user-info events. This
                # distinguishes a true account mismatch from an ID-semantic
                # mismatch without logging tokens or financial payloads.
                event55_identity={}
                event55_records=[]
                for msg in client.get_cached_events(parameters.E_BALANCE_UPDATE):
                    event55_identity.update(extract_identity_fields(msg))
                    data=msg.get("d") if isinstance(msg,dict) else None
                    if isinstance(data,list):
                        for idx,rec in enumerate(data):
                            if isinstance(rec,dict):
                                event55_records.append({
                                    "index":idx,
                                    "account_id":rec.get("account_id",rec.get("accountId")),
                                    "group":rec.get("group",rec.get("account_group",rec.get("accountGroup"))),
                                    "keys":sorted(str(k) for k in rec.keys()),
                                })
                event110_identity={}
                for msg in client.get_cached_events(parameters.E_USER_INFO):
                    event110_identity.update(extract_identity_fields(msg))

                # Search all already-received authenticated events for the
                # configured ID without logging unrelated payload values.
                target_hits=[]
                def find_target(v,path="root",event_code=None):
                    if isinstance(v,dict):
                        for k,val in v.items():
                            if str(val)==str(expected_account_id):
                                target_hits.append(f"e{event_code}:{k}@{path}")
                            if isinstance(val,(dict,list)):
                                find_target(val,f"{path}.{k}",event_code)
                    elif isinstance(v,list):
                        for idx,val in enumerate(v):
                            if isinstance(val,(dict,list)):
                                find_target(val,f"{path}[{idx}]",event_code)
                            elif str(val)==str(expected_account_id):
                                target_hits.append(f"e{event_code}:list_item@{path}[{idx}]")
                for event_code,msgs in client._event_cache.items():
                    for msg in msgs:
                        find_target(msg,"root",event_code)
                        if target_hits:
                            # Keep the diagnostic bounded; only event/path metadata
                            # is recorded, never the message payload itself.
                            if len(target_hits)>20:
                                target_hits=target_hits[:20]
                                break
                    if len(target_hits)>=20:
                        break

                log.error(
                    "AUTH_IDENTITY_FIELDS expected=%s event55=%s event110=%s",
                    expected_account_id,event55_identity,event110_identity
                )
                target_record_context=[]
                for msg in client.get_cached_events(parameters.E_BALANCE_UPDATE):
                    data=msg.get("d") if isinstance(msg,dict) else None
                    if isinstance(data,list):
                        for idx,rec in enumerate(data):
                            if not isinstance(rec,dict):
                                continue
                            matches=[]
                            for k,val in rec.items():
                                if str(val)==str(expected_account_id):
                                    matches.append(str(k))
                            if matches:
                                target_record_context.append({
                                    "index":idx,
                                    "account_id":rec.get("account_id",rec.get("accountId")),
                                    "group":rec.get("group",rec.get("account_group",rec.get("accountGroup"))),
                                    "matched_keys":matches,
                                    "keys":sorted(str(k) for k in rec.keys())
                                })
                log.error("AUTH_TARGET_ID_PRESENT=%s hit_count=%d", bool(target_hits), len(target_hits))
                log.error("AUTH_TARGET_RECORD_CONTEXT %s", target_record_context[:10])
                log.error(
                    "TOKEN_ACCOUNT_BINDING_FAILED expected_account_id=%s exposed_demo_accounts=%s",
                    expected_account_id,demo_accounts
                )
                STATE["account_id"]=None
                STATE["account_group"]="demo"
                STATE["status"]="account_token_mismatch"
                raise RuntimeError(
                    f"Authenticated session does not expose configured demo account {expected_account_id}"
                )

            client.account_id=expected_account_id
            client.account_group="demo"
            STATE["account_id"]=expected_account_id
            STATE["account_group"]="demo"
            STATE["status"]="authenticated_account_verified"
            log.info(
                "DEMO_ACCOUNT_SELECTED account_id=%s group=demo source=event55_verified",
                client.account_id
            )

            if not await sync_account_assets(client,reason="initial"):
                raise RuntimeError("Authenticated account asset scan returned no usable assets")
            STATE["status"]="live_read_only"
            assets=list(STATE["assets"])
            real_n=sum(a["mode"]=="REAL" for a in assets); otc_n=sum(a["mode"]=="OTC" for a in assets)
            log.info("ACCOUNT_ASSET_SOURCE account_id=%s source_count=%d open_real=%d open_otc=%d open_total=%d",
                     client.account_id,len(assets),real_n,otc_n,len(assets))
            log.info("ALL_ACCOUNT_OPEN_ASSETS_READY count=%d",len(assets))
            # Individual event-12 subscriptions are managed by the dedicated
            # read-only worker below. Event-1 ticks are preferred when delivered;
            # the snapshot scanner remains a timestamped fallback for assets
            # that do not emit an event-1 tick.
            log.info("TICK_SUBSCRIPTION_MODE authenticated_event1 preferred; no live snapshot API fallback")
            await refresh_candles(force=True)
            last_asset_sync=time.time()
            while True:
                await asyncio.sleep(15)
                now_sync=time.time()
                if now_sync-last_asset_sync>=60.0:
                    if await sync_account_assets(client,reason="periodic"):
                        last_asset_sync=now_sync
                    else:
                        log.warning("ACCOUNT_ASSET_SYNC_RETAINED count=%d",len(STATE["assets"]))
        except Exception as e:
            STATE["status"]=("account_token_mismatch" if "Access token does not expose configured demo account" in str(e) else "error");log.exception("MARKET_WORKER_ERROR %s",e);await asyncio.sleep(30)
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
            body_out=json.dumps({"service":"CANDICE-AI","status":STATE["status"],"read_only":True,"asset_count":len(STATE["assets"]),"qualified":len(STATE["analyses"]),"cycle":STATE["cycle"],"active_results":len(BRAIN.active_signals),"account_id":STATE.get("account_id"),"account_group":STATE.get("account_group"),"feed_source":STATE.get("feed_source"),"network":STATE.get("network",{})}).encode()
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
    await load_persistent_learning()
    port=int(os.getenv("PORT","10000"));server=await asyncio.start_server(health,"0.0.0.0",port)
    await configure_telegram_webhook()
    await asyncio.gather(market_worker(),account_live_feed_worker(),cycle_loop(),server.serve_forever())
if __name__=="__main__":asyncio.run(main())


