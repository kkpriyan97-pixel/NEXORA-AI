"""Independent daily demo practice laboratory.

It reads the authenticated market snapshot but never writes to the live Signal
Brain, signal cycle state, or live result state. A demo order can be placed only
after an admin presses ACCEPT in Telegram.
"""
from __future__ import annotations

import asyncio
import copy
import json
import os
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from candice_brain import analyze_asset
from strategy_knowledge import record_practice_result, save_error

DB_URL = os.getenv("DATABASE_URL","").strip()
UAE = ZoneInfo("Asia/Dubai")
DURATION_SECONDS = max(60, min(300, int(os.getenv("LEARNING_PRACTICE_DURATION_SECONDS","60"))))
AMOUNT = max(0.01, float(os.getenv("LEARNING_DEMO_AMOUNT","1")))
WINDOW_MINUTES = 120
START_HOUR = max(0, min(23, int(os.getenv("LEARNING_PRACTICE_START_HOUR_UAE","9"))))
LOOP_SECONDS = max(30, min(120, int(os.getenv("LEARNING_PRACTICE_INTERVAL_SECONDS","60"))))
MIN_CONFIDENCE = max(75, min(96, int(os.getenv("LEARNING_PRACTICE_MIN_CONFIDENCE","82"))))
APPROVAL_TTL = 45.0
_pending = {}
_open = {}
_cfg = {}

def configure(*, snapshot_provider, client_provider, send_message, answer_callback, admin_id):
    _cfg.update(
        snapshot_provider=snapshot_provider,
        client_provider=client_provider,
        send_message=send_message,
        answer_callback=answer_callback,
        admin_id=str(admin_id or ""),
    )

def _now_uae():
    return datetime.now(timezone.utc).astimezone(UAE)

def _window_start(day):
    return datetime(day.year,day.month,day.day,START_HOUR,0,0,tzinfo=UAE)

def practice_active(now=None):
    now=now or _now_uae()
    start=_window_start(now)
    end=start+timedelta(minutes=WINDOW_MINUTES)
    return start <= now < end

def status():
    n=_now_uae()
    start=_window_start(n)
    end=start+timedelta(minutes=WINDOW_MINUTES)
    active=start<=n<end
    return {
        "enabled":os.getenv("LEARNING_PRACTICE_ENABLED","true").strip().lower()!="false",
        "active":active,
        "start_uae":start.isoformat(),
        "end_uae":end.isoformat(),
        "duration_seconds":DURATION_SECONDS,
        "approval_required":True,
        "demo_only":True,
        "pending":len(_pending),
        "open_trades":len(_open),
    }

def _research_strategy_hint(snapshot):
    """Map recent research terms into an existing executable strategy family."""
    hints=[]
    try:
        if not DB_URL:
            return hints
        import psycopg
        def read():
            with psycopg.connect(DB_URL,connect_timeout=5) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT method_id,evidence_json
                        FROM nexora_m1_evidence
                        ORDER BY created_at DESC LIMIT 30
                    """)
                    return cur.fetchall()
        rows=asyncio.run(asyncio.to_thread(read))
    except Exception:
        return hints
    aliases=(
        ("breakout", "BREAKOUT"),("break out","BREAKOUT"),("trend following","TREND_FOLLOWING"),
        ("momentum","MOMENTUM"),("pullback","PULLBACK"),("retest","PULLBACK"),
        ("reversal","REVERSAL"),("mean reversion","MEAN_REVERSION"),
        ("price action","PRICE_ACTION"),("volatility","VOLATILITY"),("vwap","MOMENTUM"),
    )
    seen=set()
    for mid,payload in rows:
        s=(str(mid)+" "+json.dumps(payload,ensure_ascii=False)).lower()
        for term,strategy in aliases:
            if term in s and strategy not in seen:
                hints.append((strategy,str(mid)))
                seen.add(strategy)
    return hints

async def _load_research_hints():
    if not DB_URL:
        return []
    import psycopg
    aliases=(
        ("breakout","BREAKOUT"),("break out","BREAKOUT"),("trend following","TREND_FOLLOWING"),
        ("momentum","MOMENTUM"),("pullback","PULLBACK"),("retest","PULLBACK"),
        ("reversal","REVERSAL"),("mean reversion","MEAN_REVERSION"),
        ("price action","PRICE_ACTION"),("volatility","VOLATILITY"),("vwap","MOMENTUM"),
    )
    try:
        def read():
            with psycopg.connect(DB_URL,connect_timeout=5) as db:
                with db.cursor() as cur:
                    cur.execute("SELECT method_id,evidence_json FROM nexora_m1_evidence ORDER BY created_at DESC LIMIT 30")
                    return cur.fetchall()
        rows=await asyncio.to_thread(read)
    except Exception:
        return []
    hints=[];seen=set()
    for mid,payload in rows:
        raw=json.dumps(payload,ensure_ascii=False) if not isinstance(payload,str) else payload
        s=(str(mid)+" "+raw).lower()
        for term,strategy in aliases:
            if term in s and strategy not in seen:
                hints.append((strategy,str(mid)));seen.add(strategy)
    return hints

def _analyze_snapshot_sync(assets,candles,prices,preferred,hints,limit):
    candidates=[]
    now=time.time()
    # Practice is deliberately capped and rotated so the learning laboratory
    # cannot monopolize the CPU or starve the live signal scheduler.
    for asset in list(assets)[:max(1,int(limit or 8))]:
        pair=str(asset.get("pair") or "")
        if not pair or not asset.get("signal_eligible",True):
            continue
        cs=[]
        for c in candles.get(pair,[]) or []:
            if not isinstance(c,dict): continue
            try:
                t=float(c.get("time",c.get("t")))
                if t>20_000_000_000:t/=1000
                if t+60>now:continue
                cs.append(c)
            except Exception:continue
        if len(cs)<45:continue
        try:
            price=(prices.get(pair) or [None,None])[0]
            brain=analyze_asset(asset,cs,price)
        except Exception:
            continue
        if not brain:continue
        strategy=str(brain.get("strategy") or "").upper()
        if strategy not in preferred:continue
        conf=int(brain.get("confidence") or 0)
        if conf<MIN_CONFIDENCE:continue
        source=next((sid for st,sid in hints if st==strategy),"CORE")
        technique={
            "trend_15m":brain.get("trend_15m"),
            "structure_1m":brain.get("structure_1m"),
            "pattern":brain.get("pattern"),
            "donchian_state":(brain.get("indicators") or {}).get("donchian_state"),
            "donchian_expansion":(brain.get("indicators") or {}).get("donchian_expansion"),
            "stochastic_cross":(brain.get("indicators") or {}).get("stochastic_cross"),
        }
        candidates.append((conf,float(brain.get("market_quality") or 0),brain,source,technique))
    if not candidates:return None
    candidates.sort(key=lambda x:(x[0],x[1]),reverse=True)
    _,_,brain,source,technique=candidates[0]
    return {
        "pair":brain["pair"],
        "display_name":brain.get("display_name") or brain["pair"],
        "direction":str(brain.get("direction") or "").upper(),
        "strategy":str(brain.get("strategy") or "").upper(),
        "confidence":int(brain.get("confidence") or 0),
        "reference_price":brain.get("price"),
        "entry_candle_ts":brain.get("entry_candle_ts"),
        "source":source,
        "technique":technique,
        "reason":str(brain.get("reason") or "")[:600],
    }

async def _build_candidate():
    provider=_cfg.get("snapshot_provider")
    if not provider:
        return None
    try:
        snap=provider()
        assets=list(snap.get("assets") or [])
        candles=copy.deepcopy(snap.get("candles") or {})
        prices=copy.deepcopy(snap.get("prices") or {})
    except Exception:
        return None
    hints=await _load_research_hints()
    preferred=[x[0] for x in hints] or [
        "TREND_FOLLOWING","MOMENTUM","BREAKOUT","PULLBACK",
        "REVERSAL","MEAN_REVERSION","PRICE_ACTION","VOLATILITY"
    ]
    batch=int(os.getenv("LEARNING_ASSET_BATCH","8"))
    return await asyncio.to_thread(
        _analyze_snapshot_sync,assets,candles,prices,preferred,hints,batch
    )

async def _send_request(candidate):
    token=os.urandom(12).hex()
    expires=time.time()+APPROVAL_TTL
    rec=dict(candidate)
    rec.update(token=token,expires_at=expires,created_at=time.time(),status="PENDING")
    _pending[token]=rec
    markup={"inline_keyboard":[
        [{"text":"✅ ACCEPT DEMO","callback_data":f"learn:accept:{token}"},
         {"text":"❌ REJECT","callback_data":f"learn:reject:{token}"}]
    ]}
    text_msg=(
        "🧠 CANDICE • 2H LEARNING PRACTICE\n\n"
        f"📊 {rec['display_name']}\n"
        f"{'⬆️ UP' if rec['direction']=='UP' else '⬇️ DOWN'}\n"
        f"⏱️ {DURATION_SECONDS//60} MIN\n\n"
        f"🧩 Strategy → {rec['strategy']}\n"
        f"🔬 Research → {rec['source']}\n"
        f"🎯 Confidence → {rec['confidence']}%\n"
        f"💰 Reference → {rec.get('reference_price')}\n\n"
        "⚠️ DEMO LEARNING ONLY\n"
        "Human ACCEPT is required before any demo order."
    )
    try:
        await _cfg["send_message"](text_msg, reply_markup=markup,
                                   chat_id=_cfg.get("admin_id") or None)
        return True
    except Exception:
        _pending.pop(token,None)
        return False

async def _place_demo(rec, actor_id):
    if str(actor_id) != str(_cfg.get("admin_id","")):
        return False,"ADMIN_ONLY",None
    if time.time()>float(rec.get("expires_at",0)):
        return False,"APPROVAL_EXPIRED",None
    client=_cfg.get("client_provider",lambda:None)()
    if not client or not getattr(client.connection,"is_connected",False):
        return False,"BROKER_NOT_CONNECTED",None
    if str(getattr(client,"account_group","")).lower()!="demo":
        return False,"DEMO_ACCOUNT_REQUIRED",None
    account_id=getattr(client,"account_id",None)
    if account_id is None:
        return False,"DEMO_ACCOUNT_ID_MISSING",None
    pair=str(rec.get("pair") or "")
    direction=str(rec.get("direction") or "").lower()
    if direction not in {"up","down"} or not pair:
        return False,"INVALID_DIRECTION_OR_PAIR",None
    try:
        # The group is hard-coded to demo and verified again immediately before
        # the broker request. No real/live group is accepted by this layer.
        result=await asyncio.wait_for(
            client.trade.place_order(
                pair=pair,amount=AMOUNT,direction=direction,
                duration=DURATION_SECONDS,account_id=int(account_id),
                group="demo",category="digital",is_flex=True
            ),
            timeout=5.0
        )
    except Exception as e:
        return False,"DEMO_ORDER_EXCEPTION",None
    if not isinstance(result,dict):
        return False,"DEMO_ORDER_REJECTED",None
    trade_id=result.get("id") or result.get("trade_id")
    if not trade_id:
        return False,"DEMO_ORDER_NO_ID",None

    # Prefer the broker's own accepted-entry fields. If the API omits them,
    # take a fresh account snapshot immediately after acceptance; never rely
    # on the older candidate reference for result learning.
    entry_price=None
    for k in ("open_price","openPrice","entry_price","entryPrice","price","rate","cur_open"):
        try:
            if result.get(k) is not None:
                entry_price=float(result.get(k))
                break
        except (TypeError,ValueError):
            pass
    if entry_price is None:
        try:
            snap=_cfg.get("snapshot_provider",lambda:{})()
            live=(snap.get("prices") or {}).get(pair)
            if live and live[0] is not None:
                entry_price=float(live[0])
        except Exception:
            entry_price=None
    if entry_price is None:
        entry_price=float(rec.get("reference_price") or 0.0)

    now=time.time()
    rec=dict(rec)
    rec.update({
        "status":"OPEN","trade_id":str(trade_id),"accepted_by":int(actor_id),
        "placed_at":now,"entry_ts":now,"entry_price":entry_price,
    })
    _open[str(trade_id)]=rec
    return True,"PLACED",rec

async def handle_callback(query):
    if not isinstance(query,dict):return False
    qid=str(query.get("id") or "")
    data=str(query.get("data") or "")
    frm=query.get("from") or {}
    uid=str(frm.get("id") or "")
    if not data.startswith("learn:"):return False
    parts=data.split(":")
    token=parts[2] if len(parts)>=3 else ""
    rec=_pending.get(token)
    if not rec:
        await _cfg["answer_callback"](qid,"Expired or already used")
        return True
    if uid != str(_cfg.get("admin_id","")):
        await _cfg["answer_callback"](qid,"Admin only")
        return True
    if time.time()>float(rec.get("expires_at",0)):
        _pending.pop(token,None)
        await _cfg["answer_callback"](qid,"Expired")
        return True
    action=parts[1]
    _pending.pop(token,None)
    if action=="reject":
        await _cfg["answer_callback"](qid,"Demo practice rejected")
        await _cfg["send_message"]("❌ Demo learning trade rejected by human verification.",
                                    chat_id=_cfg.get("admin_id") or None)
        return True
    if action!="accept":
        await _cfg["answer_callback"](qid,"Invalid action")
        return True
    await _cfg["answer_callback"](qid,"Accepted — placing demo trade")
    ok,reason,placed=await _place_demo(rec,uid)
    if not ok:
        await _cfg["send_message"](f"❌ DEMO LEARNING ORDER BLOCKED\n\nReason → {reason}",
                                    chat_id=_cfg.get("admin_id") or None)
        await save_error(rec.get("strategy","UNKNOWN"),reason,rec.get("technique") or {})
        return True
    await _cfg["send_message"](
        f"✅ DEMO LEARNING ORDER ACCEPTED\n\n📊 {placed['display_name']}\n"
        f"{'⬆️ UP' if placed['direction']=='UP' else '⬇️ DOWN'}\n"
        f"💰 Amount → {AMOUNT}\n💵 Entry → {placed.get('entry_price')}\n⏱️ Duration → {DURATION_SECONDS//60} MIN\n"
        f"🧩 Strategy → {placed['strategy']}\n🆔 Demo Trade → {placed['trade_id']}",
        chat_id=_cfg.get("admin_id") or None
    )
    return True

async def handle_trade_update(message):
    if not isinstance(message,dict):return
    event=message.get("e")
    for item in (message.get("d") or []):
        if not isinstance(item,dict):continue
        tid=str(item.get("id") or "")
        if not tid or tid not in _open:continue
        if event==26:
            rec=_open.pop(tid)
            status=str(item.get("status") or "").upper()
            pnl=item.get("balance_change")
            result="WIN" if status in {"WIN","WON","PROFIT"} or (pnl is not None and float(pnl)>0) else "LOSS" if status in {"LOSS","LOST"} or (pnl is not None and float(pnl)<0) else "TIE"
            await _record_result(rec,result,"BROKER_EVENT_26",item.get("curs_close"),pnl)

async def _record_result(rec,result,source,exit_price=None,pnl=None):
    ctx=dict(rec.get("technique") or {})
    error_code="" if result=="WIN" else "LOSS_CONTEXT"
    await record_practice_result(rec.get("strategy","UNKNOWN"),result,
                                 pair=rec.get("pair",""),confidence=rec.get("confidence",0),
                                 context=ctx,error_code=error_code)
    _open.pop(str(rec.get("trade_id") or ""),None)
    await _cfg["send_message"](
        "🧠 DEMO LEARNING RESULT\n\n"
        f"📊 {rec.get('display_name')}\n"
        f"{'🟢 WIN' if result=='WIN' else '🔴 LOSS' if result=='LOSS' else '🟡 TIE'}\n"
        f"🧩 Strategy → {rec.get('strategy')}\n"
        f"🔎 Result source → {source}\n"
        f"📦 Learning DB → UPDATED\n"
        f"💾 Raw practice log → bounded to 500 records",
        chat_id=_cfg.get("admin_id") or None
    )

async def _fallback_watch(rec):
    await asyncio.sleep(DURATION_SECONDS+2)
    tid=str(rec.get("trade_id") or "")
    if tid not in _open:return
    provider=_cfg.get("snapshot_provider")
    try:
        snap=provider()
        candles=snap.get("candles") or {}
        pair=rec.get("pair")
        cs=[]
        for c in candles.get(pair,[]) or []:
            try:
                t=float(c.get("time",c.get("t")))
                if t>20_000_000_000:t/=1000
                if t+60<=time.time():cs.append(c)
            except Exception:continue
        cs.sort(key=lambda x:float(x.get("time",x.get("t",0))))
        if not cs:return
        entry=float(rec.get("entry_price") or rec.get("reference_price") or 0.0)
        exitp=float(cs[-1].get("close",cs[-1].get("c")))
        if exitp>entry:result="WIN" if rec["direction"]=="UP" else "LOSS"
        elif exitp<entry:result="WIN" if rec["direction"]=="DOWN" else "LOSS"
        else:result="TIE"
        await _record_result(rec,result,"CLOSED_CANDLE_FALLBACK",exitp,None)
    except Exception:
        await save_error(rec.get("strategy","UNKNOWN"),"RESULT_FALLBACK_FAILED",rec.get("technique") or {})

async def run_forever():
    if os.getenv("LEARNING_PRACTICE_ENABLED","true").strip().lower()=="false":
        return
    last_day=None
    while True:
        try:
            now=_now_uae()
            day=now.date()
            if practice_active(now) and last_day!=day and not _pending and not _open:
                candidate=await _build_candidate()
                if candidate:
                    if await _send_request(candidate):
                        last_day=day
                        log_msg=(f"PRACTICE_WINDOW_STARTED day={day} start={START_HOUR:02d}:00 duration=2h "
                                 f"strategy={candidate['strategy']} pair={candidate['pair']} source={candidate['source']}")
                        print(log_msg)
                else:
                    # Keep checking within the 2h window; a valid research strategy
                    # can appear later as candles change.
                    pass
            # While the 2h window is active, allow another candidate only after the
            # previous request/order has completed. This prevents Telegram spam and
            # excessive demo orders.
            if practice_active(now) and not _pending and not _open:
                candidate=await _build_candidate()
                if candidate:
                    await _send_request(candidate)
            for tid,rec in list(_open.items()):
                asyncio.create_task(_fallback_watch(rec)) if not rec.get("_watch_started") else None
                rec["_watch_started"]=True
            # Expire stale approvals.
            for token,rec in list(_pending.items()):
                if time.time()>float(rec.get("expires_at",0)):
                    _pending.pop(token,None)
                    await save_error(rec.get("strategy","UNKNOWN"),"HUMAN_APPROVAL_TIMEOUT",rec.get("technique") or {})
            await asyncio.sleep(LOOP_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"LEARNING_PRACTICE_ERROR type={type(exc).__name__} message={str(exc)[:160]}")
            await asyncio.sleep(LOOP_SECONDS)
