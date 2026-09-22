"""Independent daily demo practice laboratory.

It reads the authenticated market snapshot and places DEMO-only practice orders
automatically during the fixed 20:30–22:30 UAE learning window. It never accepts
a live/real account for learning execution.
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
START_HOUR = 20
START_MINUTE = 30
LOOP_SECONDS = max(30, min(120, int(os.getenv("LEARNING_PRACTICE_INTERVAL_SECONDS","60"))))
MIN_CONFIDENCE = max(75, min(96, int(os.getenv("LEARNING_PRACTICE_MIN_CONFIDENCE","82"))))
REQUEST_TTL = 45.0
RESULT_WATCH_POLL_SECONDS = max(2, min(15, int(os.getenv("LEARNING_RESULT_WATCH_POLL_SECONDS","5"))))
RESULT_WATCH_EXTRA_SECONDS = max(30, min(600, int(os.getenv("LEARNING_RESULT_WATCH_EXTRA_SECONDS","240"))))
LEARNING_TRADE_RETENTION_HOURS = 48
_pending = {}
_open = {}
_finalized = set()
_cfg = {}
_daily = {"day": None, "placed": 0, "win": 0, "loss": 0, "tie": 0, "blocked": 0, "strategies": set()}
_last_report_day = None
_practice_cursor = 0

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
    return datetime(day.year,day.month,day.day,START_HOUR,START_MINUTE,0,tzinfo=UAE)

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
        "approval_required":False,
        "demo_only":True,
        "schedule":"DAILY",
        "learning_window":"20:30–22:30 UAE",
        "pending":len(_pending),
        "open_trades":len(_open),
    }

async def ensure_learning_trade_state_table():
    if not DB_URL:
        return False
    try:
        import psycopg
        def init():
            with psycopg.connect(DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS candice_learning_trade_state (
                            trade_id TEXT PRIMARY KEY,
                            record JSONB NOT NULL,
                            status TEXT NOT NULL DEFAULT 'OPEN',
                            last_error TEXT,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            completed_at TIMESTAMPTZ
                        )
                    """)
                    cur.execute("""
                        DELETE FROM candice_learning_trade_state
                        WHERE updated_at < NOW() - INTERVAL '2 days'
                    """)
                db.commit()
        await asyncio.to_thread(init)
        print("LEARNING_TRADE_STATE_READY")
        return True
    except Exception as e:
        print(f"LEARNING_TRADE_STATE_INIT_FAILED type={type(e).__name__} message={str(e)[:160]}")
        return False

def _learning_trade_payload(rec):
    payload={}
    for k,v in dict(rec or {}).items():
        if k.startswith("_"):
            continue
        if isinstance(v,(str,int,float,bool)) or v is None:
            payload[k]=v
        else:
            try:
                json.dumps(v)
                payload[k]=v
            except Exception:
                payload[k]=str(v)
    return payload

async def persist_open_trade(rec):
    if not DB_URL or not rec.get("trade_id"):
        return False
    try:
        import psycopg
        trade_id=str(rec["trade_id"])
        payload=json.dumps(_learning_trade_payload(rec),separators=(",",":"),ensure_ascii=False,default=str)
        def put():
            with psycopg.connect(DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        INSERT INTO candice_learning_trade_state(trade_id,record,status)
                        VALUES(%s,%s::jsonb,'OPEN')
                        ON CONFLICT(trade_id) DO UPDATE SET
                            record=EXCLUDED.record,status='OPEN',last_error=NULL,
                            updated_at=NOW(),completed_at=NULL
                    """,(trade_id,payload))
                db.commit()
        await asyncio.to_thread(put)
        print(f"LEARNING_TRADE_PERSISTED trade_id={trade_id} pair={rec.get('pair')}")
        return True
    except Exception as e:
        print(f"LEARNING_TRADE_PERSIST_FAILED trade_id={rec.get('trade_id')} type={type(e).__name__} message={str(e)[:160]}")
        return False

async def complete_open_trade(trade_id,result):
    if not DB_URL or not trade_id:
        return False
    try:
        import psycopg
        def done():
            with psycopg.connect(DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        UPDATE candice_learning_trade_state
                        SET status='COMPLETED',last_error=NULL,updated_at=NOW(),
                            completed_at=NOW()
                        WHERE trade_id=%s
                    """,(str(trade_id),))
                db.commit()
        await asyncio.to_thread(done)
        print(f"LEARNING_TRADE_STATE_COMPLETED trade_id={trade_id} result={result}")
        return True
    except Exception as e:
        print(f"LEARNING_TRADE_COMPLETE_SAVE_FAILED trade_id={trade_id} type={type(e).__name__} message={str(e)[:160]}")
        return False

async def restore_open_trades():
    if not DB_URL:
        return 0
    try:
        import psycopg
        def read():
            with psycopg.connect(DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT trade_id,record
                        FROM candice_learning_trade_state
                        WHERE status='OPEN'
                          AND created_at > NOW() - INTERVAL '2 days'
                        ORDER BY created_at
                    """)
                    return cur.fetchall()
        rows=await asyncio.to_thread(read)
        restored=0
        for trade_id,record in rows:
            tid=str(trade_id)
            if tid in _open:
                continue
            try:
                rec=dict(record or {})
                rec["trade_id"]=tid
                rec["status"]="OPEN"
                _open[tid]=rec
                restored+=1
                print(f"LEARNING_TRADE_RESTORED trade_id={tid} pair={rec.get('pair')} entry={rec.get('entry_price')}")
            except Exception as e:
                print(f"LEARNING_TRADE_RESTORE_ITEM_FAILED trade_id={tid} type={type(e).__name__}")
        if restored:
            print(f"LEARNING_TRADE_RESTORE_COMPLETE restored={restored}")
        return restored
    except Exception as e:
        print(f"LEARNING_TRADE_RESTORE_FAILED type={type(e).__name__} message={str(e)[:160]}")
        return 0

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

def _analyze_snapshot_sync(assets,candles,prices,preferred,hints,limit,offset=0):
    candidates=[]
    now=time.time()
    # Practice is deliberately capped and rotated so the learning laboratory
    # cannot monopolize the CPU or starve the live signal scheduler.
    pool=list(assets)
    n=max(1,int(limit or 8))
    start=int(offset)%len(pool) if pool else 0
    selected=[pool[(start+i)%len(pool)] for i in range(min(n,len(pool)))] if pool else []
    for asset in selected:
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
            "body_ratio":brain.get("body_ratio"),
            "efficiency":brain.get("efficiency"),
            "momentum_norm":brain.get("momentum_norm"),
            "trend_persistence":brain.get("trend_persistence"),
            "volatility_ratio":brain.get("volatility_ratio"),
            "ema_gap_norm":brain.get("ema_gap_norm"),
            "ema_slope_norm":brain.get("ema_slope_norm"),
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
    print(
        f"LEARNING_CANDIDATE_SCAN assets={len(assets)} candles={len(candles)} "
        f"prices={len(prices)} preferred={','.join(preferred[:12])}"
    )
    batch=max(1,int(os.getenv("LEARNING_ASSET_BATCH","8")))
    global _practice_cursor
    offset=_practice_cursor
    _practice_cursor=(offset+batch)%max(1,len(assets)) if assets else 0
    candidate=await asyncio.to_thread(
        _analyze_snapshot_sync,assets,candles,prices,preferred,hints,batch,offset
    )
    if candidate:
        print(
            f"LEARNING_CANDIDATE_READY pair={candidate.get('pair')} "
            f"direction={candidate.get('direction')} confidence={candidate.get('confidence')} "
            f"strategy={candidate.get('strategy')} source={candidate.get('source')}"
        )
    else:
        print(f"LEARNING_CANDIDATE_NONE batch={batch} cursor={offset} reason=no_qualified_candidate")
    return candidate

async def _send_request(candidate):
    token=os.urandom(12).hex()
    expires=time.time()+REQUEST_TTL
    rec=dict(candidate)
    rec.update(token=token,expires_at=expires,created_at=time.time(),status="PENDING")
    _pending[token]=rec
    try:
        # Execute with the broker first. Telegram reporting is deliberately
        # outside the critical execution path so notification latency cannot
        # delay DEMO entry.
        ok, reason, placed = await _place_demo(rec, _cfg.get("admin_id",""))
        _pending.pop(token, None)
        if not ok:
            _daily["blocked"] += 1
            print(f"LEARNING_ORDER_BLOCKED pair={rec.get('pair')} strategy={rec.get('strategy')} reason={reason}")
            await _cfg["send_message"](
                f"❌ DEMO AUTO-TRADE BLOCKED\n\nReason → {reason}",
                chat_id=_cfg.get("admin_id") or None
            )
            await save_error(rec.get("strategy","UNKNOWN"), reason, rec.get("technique") or {})
            return False

        _daily["placed"] += 1
        _daily["strategies"].add(str(rec.get("strategy") or "UNKNOWN"))
        print(
            f"LEARNING_ORDER_ACCEPTED pair={placed['pair']} direction={placed['direction']} "
            f"strategy={placed['strategy']} trade_id={placed['trade_id']} "
            f"entry={placed.get('entry_price')} at={placed.get('placed_at')}"
        )
        await _cfg["send_message"](
            "🤖 DEMO AUTO-TRADE STARTED\n\n"
            f"📊 {placed['display_name']}\n"
            f"{'⬆️ UP' if placed['direction']=='UP' else '⬇️ DOWN'}\n"
            f"💰 Amount → {AMOUNT}\n"
            f"💵 Entry → {placed.get('entry_price')}\n"
            f"⏱️ Duration → {DURATION_SECONDS//60} MIN\n"
            f"🧩 Strategy → {placed['strategy']}\n"
            f"🆔 Demo Trade → {placed['trade_id']}\n"
            "🔐 DEMO ACCOUNT ONLY",
            chat_id=_cfg.get("admin_id") or None
        )
        return True
    except Exception as exc:
        _pending.pop(token,None)
        print(f"LEARNING_ORDER_REPORT_FAILED type={type(exc).__name__} message={str(exc)[:120]}")
        return True if rec.get("trade_id") else False

async def _place_demo(rec, actor_id):
    if not practice_active():
        return False,"LEARNING_WINDOW_CLOSED",None
    if str(actor_id) != str(_cfg.get("admin_id","")):
        return False,"ADMIN_ONLY",None
    if time.time()>float(rec.get("expires_at",0)):
        return False,"PRACTICE_REQUEST_EXPIRED",None
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
        print(f"LEARNING_ORDER_ATTEMPT pair={pair} direction={direction.upper()} group=demo amount={AMOUNT} duration={DURATION_SECONDS}")
        # Use the canonical DEMO order signature supported by the OlympTrade
        # client: pair, amount, direction, duration, account_id, group.
        # Avoid optional payload flags that can cause a server-side Invalid request.
        result=await asyncio.wait_for(
            client.trade.place_order(
                pair=pair,amount=AMOUNT,direction=direction,
                duration=DURATION_SECONDS,account_id=int(account_id),
                group="demo"
            ),
            timeout=5.0
        )
    except Exception as e:
        msg=str(e).replace("\n"," ")[:220]
        print(f"LEARNING_ORDER_EXCEPTION pair={pair} type={type(e).__name__} message={msg}")
        return False,f"DEMO_ORDER_EXCEPTION:{type(e).__name__}",None

    if not isinstance(result,dict):
        print(f"LEARNING_ORDER_RESPONSE_INVALID pair={pair} type={type(result).__name__}")
        return False,"DEMO_ORDER_REJECTED:INVALID_RESPONSE",None

    status_value=str(result.get("status") or result.get("state") or "").upper()
    code_value=str(result.get("code") or result.get("error_code") or "").strip()
    message_value=str(result.get("message") or result.get("error") or result.get("reason") or "").replace("\n"," ")[:180]
    success_value=result.get("success")

    print(
        f"LEARNING_ORDER_RESPONSE pair={pair} status={status_value or 'UNKNOWN'} "
        f"code={code_value or 'NONE'} message={message_value or 'NONE'} "
        f"keys={sorted(str(k) for k in result.keys())}"
    )

    rejected_states={"REJECTED","ERROR","FAILED","FAIL","DENIED"}
    if status_value in rejected_states or success_value is False or result.get("error") not in (None,"",False):
        detail=code_value or message_value or status_value or "BROKER_REJECTED"
        return False,f"DEMO_ORDER_REJECTED:{detail}",None

    trade_id=result.get("id") or result.get("trade_id") or result.get("order_id") or result.get("orderId")
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
    await persist_open_trade(rec)
    print(f"LEARNING_ORDER_CONFIRMED pair={pair} trade_id={trade_id} entry={entry_price} account_id={account_id} group=demo")
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
        await _cfg["answer_callback"](qid,"Auto demo mode is active; manual acceptance is not required.")
        return True

async def handle_trade_update(message):
    if not isinstance(message,dict):return
    event=message.get("e")
    raw_items=message.get("d") or []
    items=raw_items if isinstance(raw_items,list) else [raw_items]
    for item in items:
        if not isinstance(item,dict):continue
        tid=str(item.get("id") or item.get("trade_id") or "")
        if not tid or tid not in _open or tid in _finalized:continue
        if event==26 or str(item.get("status") or "").upper() in {"WIN","WON","PROFIT","LOSS","LOST","CLOSED"}:
            rec=dict(_open.get(tid) or {})
            if not rec:continue
            status=str(item.get("status") or "").upper()
            pnl=item.get("balance_change")
            try:
                pnl_num=float(pnl) if pnl is not None else None
            except (TypeError,ValueError):
                pnl_num=None
            result="WIN" if status in {"WIN","WON","PROFIT"} or (pnl_num is not None and pnl_num>0) else "LOSS" if status in {"LOSS","LOST"} or (pnl_num is not None and pnl_num<0) else "TIE"
            await _record_result(rec,result,"BROKER_EVENT_26",item.get("curs_close"),pnl)

async def _record_result(rec,result,source,exit_price=None,pnl=None):
    tid=str(rec.get("trade_id") or "")
    if tid:
        if tid in _finalized:
            return
        _finalized.add(tid)
        _open.pop(tid,None)
    ctx=dict(rec.get("technique") or {})
    error_code=""
    if result=="LOSS":
        strategy=str(rec.get("strategy") or "").upper()
        trend=str(ctx.get("trend_15m") or "").upper()
        try: body=float(ctx.get("body_ratio") or 0.0)
        except (TypeError,ValueError): body=0.0
        try: eff=float(ctx.get("efficiency") or 0.0)
        except (TypeError,ValueError): eff=0.0
        if strategy in {"MOMENTUM","BREAKOUT"} and trend=="SIDEWAYS":
            error_code="SIDEWAYS_CONTINUATION"
        elif body < 0.55:
            error_code="WEAK_CANDLE_BODY"
        elif eff < 0.35:
            error_code="LOW_PRICE_EFFICIENCY"
        else:
            error_code="LOSS_CONTEXT"
    if result=="WIN":
        _daily["win"] += 1
    elif result=="LOSS":
        _daily["loss"] += 1
    else:
        _daily["tie"] += 1
    _daily["day"] = str(_now_uae().date())
    db_ok=await record_practice_result(rec.get("strategy","UNKNOWN"),result,
                                 pair=rec.get("pair",""),confidence=rec.get("confidence",0),
                                 context=ctx,error_code=error_code)
    validation=await _strategy_validation_snapshot(rec.get("strategy","UNKNOWN"))
    if tid:
        await complete_open_trade(tid,result)
    if validation and validation["status"]=="VALIDATED":
        validation_line=(
            f"✅ Own Strategy Brain → VALIDATED "
            f"(samples={validation['samples']}, accuracy="
            f"{(100.0*validation['wins']/max(1,validation['wins']+validation['losses'])):.1f}%)"
        )
    elif validation:
        validation_line=(
            f"⏳ Own Strategy Brain → {validation['status']} "
            f"(samples={validation['samples']}/20)"
        )
    else:
        validation_line="⏳ Own Strategy Brain → validation pending"
    await _cfg["send_message"](
        "🧠 DEMO LEARNING RESULT\n\n"
        f"📊 {rec.get('display_name')}\n"
        f"{'🟢 WIN' if result=='WIN' else '🔴 LOSS' if result=='LOSS' else '🟡 TIE'}\n"
        f"🧩 Strategy → {rec.get('strategy')}\n"
        f"🔎 Result source → {source}\n"
        f"📦 Learning DB → {'UPDATED' if db_ok else 'UPDATE FAILED'}\n"
        f"{validation_line}\n"
        f"💾 Raw practice log → bounded to 500 records",
        chat_id=_cfg.get("admin_id") or None
    )

async def _fallback_watch(rec):
    tid=str(rec.get("trade_id") or "")
    if not tid:return
    expiry_ts=float(rec.get("entry_ts") or rec.get("placed_at") or time.time()) + DURATION_SECONDS
    deadline=expiry_ts + RESULT_WATCH_EXTRA_SECONDS
    provider=_cfg.get("snapshot_provider")
    attempts=0
    while tid in _open and tid not in _finalized and time.time() < deadline:
        attempts+=1
        try:
            now=time.time()
            if now < expiry_ts:
                await asyncio.sleep(min(RESULT_WATCH_POLL_SECONDS, max(1.0,expiry_ts-now)))
                continue
            snap=provider() if provider else {}
            candles=(snap.get("candles") or {}) if isinstance(snap,dict) else {}
            pair=str(rec.get("pair") or "")
            eligible=[]
            for c in candles.get(pair,[]) or []:
                if not isinstance(c,dict):continue
                try:
                    t=float(c.get("time",c.get("t")))
                    if t>20_000_000_000:t/=1000
                    close_ts=t+60.0
                    if close_ts <= now and close_ts >= expiry_ts-2.0:
                        exitp=float(c.get("close",c.get("c")))
                        eligible.append((close_ts,exitp))
                except Exception:
                    continue
            if eligible:
                eligible.sort(key=lambda x:x[0])
                close_ts,exitp=eligible[0]
                entry=float(rec.get("entry_price") or rec.get("reference_price") or 0.0)
                if exitp>entry:
                    result="WIN" if str(rec.get("direction")).upper()=="UP" else "LOSS"
                elif exitp<entry:
                    result="WIN" if str(rec.get("direction")).upper()=="DOWN" else "LOSS"
                else:
                    result="TIE"
                print(f"LEARNING_RESULT_FALLBACK_CONFIRMED trade_id={tid} pair={pair} attempt={attempts} close_ts={close_ts:.3f} entry={entry} exit={exitp} result={result}")
                await _record_result(rec,result,"CLOSED_CANDLE_FALLBACK",exitp,None)
                return
            print(f"LEARNING_RESULT_WAIT trade_id={tid} pair={pair} attempt={attempts} next_retry={RESULT_WATCH_POLL_SECONDS}s")
        except Exception as exc:
            print(f"LEARNING_RESULT_FALLBACK_RETRY trade_id={tid} pair={rec.get('pair')} attempt={attempts} type={type(exc).__name__} message={str(exc)[:120]}")
        await asyncio.sleep(RESULT_WATCH_POLL_SECONDS)

    if tid in _open and tid not in _finalized:
        # Never leave the practice gate locked forever. A missing broker close event
        # or missing candle is a result-availability failure, not a WIN/LOSS.
        # Release the trade slot so the next DEMO learning candidate can run.
        _open.pop(tid,None)
        try:
            await save_error(rec.get("strategy","UNKNOWN"),"RESULT_UNAVAILABLE_TIMEOUT",rec.get("technique") or {})
        finally:
            print(f"LEARNING_RESULT_SLOT_RELEASED trade_id={tid} pair={rec.get('pair')} reason=RESULT_UNAVAILABLE_TIMEOUT")
            await _cfg["send_message"](
                "⚠️ DEMO RESULT WATCH TIMEOUT\n\n"
                f"📊 {rec.get('display_name') or rec.get('pair')}\n"
                "🔓 Next demo practice slot unlocked\n"
                "📦 Result was not classified as WIN/LOSS.",
                chat_id=_cfg.get("admin_id") or None
            )


async def _strategy_validation_snapshot(strategy_id):
    """Read current strategy-learning status without changing Brain decisions."""
    if not DB_URL or not strategy_id:
        return None
    try:
        import psycopg
        def read():
            with psycopg.connect(DB_URL,connect_timeout=5) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT samples,wins,losses,status
                        FROM nexora_strategy_knowledge
                        WHERE strategy_id=%s
                    """,(str(strategy_id),))
                    return cur.fetchone()
        row=await asyncio.to_thread(read)
        if not row:
            return None
        samples,wins,losses,status=row
        return {"samples":int(samples or 0),"wins":int(wins or 0),
                "losses":int(losses or 0),"status":str(status or "CANDIDATE")}
    except Exception as e:
        print(f"LEARNING_VALIDATION_SNAPSHOT_FAILED strategy={strategy_id} type={type(e).__name__} message={str(e)[:120]}")
        return None

async def _validated_strategy_ids():
    if not DB_URL:
        return []
    try:
        import psycopg
        def read():
            with psycopg.connect(DB_URL,connect_timeout=5) as db:
                with db.cursor() as cur:
                    cur.execute("SELECT strategy_id FROM nexora_strategy_knowledge WHERE status='VALIDATED' ORDER BY updated_at DESC")
                    return [str(r[0]) for r in cur.fetchall()]
        return await asyncio.to_thread(read)
    except Exception:
        return []

async def _send_daily_report(day):
    decided = _daily["win"] + _daily["loss"]
    accuracy = (100.0 * _daily["win"] / decided) if decided else 0.0
    validated = await _validated_strategy_ids()
    await _cfg["send_message"](
        "🧠 CANDICE • LEARNING REPORT\n\n"
        "🕣 Practice → 20:30–22:30 UAE\n"
        f"📅 Day → {day}\n"
        f"🤖 DEMO AUTO-TRADE → {_daily['placed']} trades\n"
        f"🟢 WIN → {_daily['win']}\n"
        f"🔴 LOSS → {_daily['loss']}\n"
        f"🟡 TIE → {_daily['tie']}\n"
        f"🎯 Accuracy → {accuracy:.1f}%\n"
        f"⛔ Blocked → {_daily['blocked']}\n\n"
        f"✅ VALIDATED / OWN STRATEGY READY → {len(validated)}\n"
        f"🧩 Strategies → {', '.join(validated[:20]) if validated else 'None'}\n\n"
        "🔴 22:30 → Learning Auto-Trade OFF\n"
        "🔐 ADMIN ONLY",
        chat_id=_cfg.get("admin_id") or None
    )

async def run_forever():
    if os.getenv("LEARNING_PRACTICE_ENABLED","true").strip().lower()=="false":
        return
    await ensure_learning_trade_state_table()
    await restore_open_trades()
    while True:
        try:
            now=_now_uae()
            day=now.date()
            if _daily["day"] != str(day):
                _daily.update({"day":str(day),"placed":0,"win":0,"loss":0,"tie":0,"blocked":0,"strategies":set()})
            global _last_report_day
            window_end=_window_start(day)+timedelta(minutes=WINDOW_MINUTES)
            if now >= window_end and _last_report_day != day:
                await _send_daily_report(day)
                _last_report_day = day
            if practice_active(now) and not _pending and not _open:
                print(f"LEARNING_WINDOW_ACTIVE day={day} start=20:30 end=22:30 timezone=Asia/Dubai")
                candidate=await _build_candidate()
                if candidate:
                    sent = await _send_request(candidate)
                    if sent:
                        log_msg=(f"PRACTICE_WINDOW_ACTIVE day={day} start={START_HOUR:02d}:{START_MINUTE:02d} duration=2h "
                                 f"strategy={candidate['strategy']} pair={candidate['pair']} source={candidate['source']}")
                        print(log_msg)
                        print(f"LEARNING_PRACTICE_ROTATION cursor={_practice_cursor}")
                else:
                    # Keep checking within the 2h window; a valid setup can appear
                    # later as candles change.
                    pass
            # While the 2h window is active, allow another candidate only after the
            # previous order has completed. This prevents overlapping demo orders.
            for tid,rec in list(_open.items()):
                if not rec.get("_watch_started"):
                    rec["_watch_started"]=True
                    asyncio.create_task(_fallback_watch(rec))
            # Expire stale execution requests.
            for token,rec in list(_pending.items()):
                if time.time()>float(rec.get("expires_at",0)):
                    _pending.pop(token,None)
                    await save_error(rec.get("strategy","UNKNOWN"),"DEMO_REQUEST_TIMEOUT",rec.get("technique") or {})
            await asyncio.sleep(LOOP_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"LEARNING_PRACTICE_ERROR type={type(exc).__name__} message={str(exc)[:160]}")
            await asyncio.sleep(LOOP_SECONDS)
