"""Independent overnight DEMO learning laboratory.

From 18:00–06:00 UAE it combines the authenticated market snapshot, web-research
knowledge and a multi-AI strategy council. It never accepts a live/real account.
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
from ai_router import strategy_council_with_fallback
from strategy_knowledge import (
    record_practice_result, record_strategy_council, save_error,
    promote_campaign_strategy,
)

DB_URL = os.getenv("DATABASE_URL","").strip()
UAE = ZoneInfo("Asia/Dubai")
DURATION_SECONDS = max(60, min(300, int(os.getenv("LEARNING_PRACTICE_DURATION_SECONDS","60"))))
AMOUNT = max(0.01, float(os.getenv("LEARNING_DEMO_AMOUNT","1")))
WINDOW_MINUTES = 720
START_HOUR = 18
START_MINUTE = 0
LOOP_SECONDS = max(30, min(120, int(os.getenv("LEARNING_PRACTICE_INTERVAL_SECONDS","60"))))
LEARNING_SCAN_TIMEOUT_SECONDS = max(10, min(45, int(os.getenv("LEARNING_SCAN_TIMEOUT_SECONDS","25"))))
LEARNING_RESEARCH_HINT_TIMEOUT_SECONDS = max(2, min(10, int(os.getenv("LEARNING_RESEARCH_HINT_TIMEOUT_SECONDS","5"))))
LEARNING_SNAPSHOT_TIMEOUT_SECONDS = max(3, min(15, int(os.getenv("LEARNING_SNAPSHOT_TIMEOUT_SECONDS","8"))))
MIN_CONFIDENCE = max(75, min(96, int(os.getenv("LEARNING_PRACTICE_MIN_CONFIDENCE","82"))))
REQUEST_TTL = 45.0
RESULT_WATCH_POLL_SECONDS = max(2, min(15, int(os.getenv("LEARNING_RESULT_WATCH_POLL_SECONDS","5"))))
RESULT_WATCH_EXTRA_SECONDS = max(60, min(300, int(os.getenv("LEARNING_RESULT_WATCH_EXTRA_SECONDS","180"))))
LEARNING_TRADE_RETENTION_HOURS = 48
COUNCIL_REFRESH_SECONDS = max(300, min(1800, int(os.getenv("LEARNING_COUNCIL_REFRESH_SECONDS","900"))))
COUNCIL_CALL_TIMEOUT_SECONDS = max(5, min(15, int(os.getenv("LEARNING_COUNCIL_CALL_TIMEOUT_SECONDS","8"))))
_council_cache = {"session_id":"", "created_at":0.0, "data":{}}
_pending = {}
_open = {}
_finalized = set()
_cfg = {}
_daily = {"day": None, "placed": 0, "win": 0, "loss": 0, "tie": 0, "blocked": 0, "strategies": set()}
_last_report_day = None
_practice_cursor = 0
CAMPAIGN_MIN_TRADES = 100
CAMPAIGN_MIN_WIN_RATE = 0.85
CAMPAIGN_TARGET_WIN_RATE = 0.90
CAMPAIGN_STATE = {
    "session_id":"",
    "strategy":"",
    "strategy_index":0,
    "sampled":0,
    "wins":0,
    "losses":0,
    "ties":0,
    "status":"WAITING",
    "strategies":[],
    "started_at":0.0,
}
CAMPAIGN_ASSET_SEM = asyncio.Semaphore(12)

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

def _learning_session_day(now=None):
    now=now or _now_uae()
    # One learning session spans 18:00 on day D through 06:00 on day D+1.
    return now.date() if now.hour >= START_HOUR else (now-timedelta(days=1)).date()

def _window_start(day):
    return datetime(day.year,day.month,day.day,START_HOUR,START_MINUTE,0,tzinfo=UAE)

def _window_end(day):
    return _window_start(day)+timedelta(minutes=WINDOW_MINUTES)

def practice_active(now=None):
    now=now or _now_uae()
    session_day=_learning_session_day(now)
    start=_window_start(session_day)
    end=_window_end(session_day)
    return start <= now < end

def status():
    n=_now_uae()
    session_day=_learning_session_day(n)
    start=_window_start(session_day)
    end=_window_end(session_day)
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
        "learning_window":"18:00–06:00 UAE",
        "pending":len(_pending),
        "open_trades":len(_open),
    }

async def ensure_campaign_table():
    if not DB_URL:
        return False
    try:
        import psycopg
        def init():
            with psycopg.connect(DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS nexora_strategy_campaign (
                            session_id TEXT NOT NULL,
                            strategy_id TEXT NOT NULL,
                            strategy_index INTEGER NOT NULL DEFAULT 0,
                            samples INTEGER NOT NULL DEFAULT 0,
                            wins INTEGER NOT NULL DEFAULT 0,
                            losses INTEGER NOT NULL DEFAULT 0,
                            ties INTEGER NOT NULL DEFAULT 0,
                            status TEXT NOT NULL DEFAULT 'ACTIVE',
                            started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            completed_at TIMESTAMPTZ,
                            PRIMARY KEY(session_id,strategy_id)
                        )
                    """)
                db.commit()
        await asyncio.to_thread(init)
        return True
    except Exception as e:
        print(f"LEARNING_CAMPAIGN_TABLE_FAILED type={type(e).__name__} message={str(e)[:140]}")
        return False

async def _save_campaign():
    if not DB_URL or not CAMPAIGN_STATE.get("session_id") or not CAMPAIGN_STATE.get("strategy"):
        return False
    try:
        import psycopg
        s=CAMPAIGN_STATE
        def write():
            with psycopg.connect(DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        INSERT INTO nexora_strategy_campaign(
                            session_id,strategy_id,strategy_index,samples,wins,losses,ties,status,started_at,completed_at
                        ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,
                                 COALESCE(to_timestamp(%s),NOW()),
                                 CASE WHEN %s IN ('VALIDATED','REJECTED') THEN NOW() ELSE NULL END)
                        ON CONFLICT(session_id,strategy_id) DO UPDATE SET
                            strategy_index=EXCLUDED.strategy_index,
                            samples=EXCLUDED.samples,wins=EXCLUDED.wins,
                            losses=EXCLUDED.losses,ties=EXCLUDED.ties,
                            status=EXCLUDED.status,updated_at=NOW(),
                            completed_at=EXCLUDED.completed_at
                    """,(
                        str(s["session_id"]),str(s["strategy"]),int(s.get("strategy_index") or 0),
                        int(s.get("sampled") or 0),int(s.get("wins") or 0),
                        int(s.get("losses") or 0),int(s.get("ties") or 0),
                        str(s.get("status") or "ACTIVE"),float(s.get("started_at") or time.time()),
                        str(s.get("status") or "ACTIVE")
                    ))
                db.commit()
        await asyncio.to_thread(write)
        return True
    except Exception as e:
        print(f"LEARNING_CAMPAIGN_SAVE_FAILED strategy={CAMPAIGN_STATE.get('strategy')} type={type(e).__name__} message={str(e)[:120]}")
        return False

async def _load_campaign(session_id):
    if not DB_URL:
        return None
    try:
        import psycopg
        def read():
            with psycopg.connect(DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT strategy_id,strategy_index,samples,wins,losses,ties,status,
                               EXTRACT(EPOCH FROM started_at)
                        FROM nexora_strategy_campaign
                        WHERE session_id=%s
                        ORDER BY strategy_index
                    """,(str(session_id),))
                    return cur.fetchall()
        rows=await asyncio.to_thread(read)
        if not rows:
            return None
        for row in rows:
            strategy,idx,samples,wins,losses,ties,status,started=row
            if str(status)=="ACTIVE":
                CAMPAIGN_STATE.update({
                    "session_id":str(session_id),"strategy":str(strategy),
                    "strategy_index":int(idx),"sampled":int(samples or 0),
                    "wins":int(wins or 0),"losses":int(losses or 0),
                    "ties":int(ties or 0),"status":"ACTIVE",
                    "started_at":float(started or time.time()),
                })
                return CAMPAIGN_STATE
        return None
    except Exception:
        return None

async def _initialize_campaign(session_id):
    if CAMPAIGN_STATE.get("session_id")==session_id and CAMPAIGN_STATE.get("strategy"):
        return CAMPAIGN_STATE
    loaded=await _load_campaign(session_id)
    if loaded:
        return loaded

    # Ask the council once at session start. Its result determines the first
    # fixed strategy, but the actual 100-trade campaign is evaluated independently.
    strategy=""
    votes={}
    try:
        seed=await asyncio.wait_for(_build_candidate(),timeout=LEARNING_SCAN_TIMEOUT_SECONDS)
        if seed:
            strategy=str(seed.get("council_consensus") or seed.get("strategy") or "").upper()
            votes=dict(seed.get("council_votes") or {})
    except Exception:
        pass
    core=[
        "TREND_FOLLOWING","MOMENTUM","BREAKOUT","PULLBACK",
        "REVERSAL","MEAN_REVERSION","PRICE_ACTION","VOLATILITY"
    ]
    ordered=[]
    if strategy: ordered.append(strategy)
    for s,_ in sorted(votes.items(),key=lambda kv:(-int(kv[1] or 0),kv[0])):
        s=str(s).upper()
        if s in core and s not in ordered: ordered.append(s)
    for s in core:
        if s not in ordered: ordered.append(s)

    CAMPAIGN_STATE.update({
        "session_id":str(session_id),"strategy":ordered[0],
        "strategy_index":0,"sampled":0,"wins":0,"losses":0,"ties":0,
        "status":"ACTIVE","strategies":ordered,"started_at":time.time(),
    })
    await _save_campaign()
    print(
        f"LEARNING_STRATEGY_CAMPAIGN_START session={session_id} "
        f"strategy={ordered[0]} index=0 target_trades=100 min_win_rate=85% target_win_rate=90%"
    )
    return CAMPAIGN_STATE

async def _advance_campaign_if_complete():
    s=CAMPAIGN_STATE
    if s.get("status")!="ACTIVE" or int(s.get("sampled") or 0)<CAMPAIGN_MIN_TRADES:
        return False
    decided=max(1,int(s.get("wins") or 0)+int(s.get("losses") or 0))
    rate=float(s.get("wins") or 0)/decided
    strategy=str(s.get("strategy") or "").upper()
    if rate>=CAMPAIGN_MIN_WIN_RATE:
        s["status"]="VALIDATED"
        promoted=await promote_campaign_strategy(strategy,s["sampled"],s["wins"],s["losses"])
        print(
            f"LEARNING_STRATEGY_CAMPAIGN_COMPLETE strategy={strategy} samples={s['sampled']} "
            f"wins={s['wins']} losses={s['losses']} ties={s['ties']} "
            f"win_rate={rate*100:.2f}% gate=85% promoted={promoted}"
        )
    else:
        s["status"]="REJECTED"
        print(
            f"LEARNING_STRATEGY_CAMPAIGN_REJECTED strategy={strategy} samples={s['sampled']} "
            f"wins={s['wins']} losses={s['losses']} ties={s['ties']} win_rate={rate*100:.2f}% gate=85%"
        )
    await _save_campaign()
    # The next strategy starts only after the previous 100 completed results.
    strategies=list(s.get("strategies") or [])
    next_idx=int(s.get("strategy_index") or 0)+1
    if next_idx>=len(strategies):
        # Research council will be refreshed on the next session.
        print(f"LEARNING_STRATEGY_QUEUE_EXHAUSTED session={s.get('session_id')}")
        return True
    next_strategy=str(strategies[next_idx]).upper()
    s.update({
        "strategy":next_strategy,"strategy_index":next_idx,
        "sampled":0,"wins":0,"losses":0,"ties":0,
        "status":"ACTIVE","started_at":time.time(),
    })
    await _save_campaign()
    print(
        f"LEARNING_STRATEGY_NEXT strategy={next_strategy} index={next_idx} "
        f"target_trades=100 min_win_rate=85% target_win_rate=90%"
    )
    return True

async def _campaign_result(result, strategy):
    s=CAMPAIGN_STATE
    if s.get("status")!="ACTIVE":
        return
    if str(strategy or "").upper()!=str(s.get("strategy") or "").upper():
        return
    if int(s.get("sampled") or 0)>=CAMPAIGN_MIN_TRADES:
        return
    s["sampled"]=int(s.get("sampled") or 0)+1
    if result=="WIN": s["wins"]=int(s.get("wins") or 0)+1
    elif result=="LOSS": s["losses"]=int(s.get("losses") or 0)+1
    else: s["ties"]=int(s.get("ties") or 0)+1
    print(
        f"LEARNING_STRATEGY_PROGRESS strategy={s['strategy']} "
        f"sample={s['sampled']}/100 win={s['wins']} loss={s['losses']} tie={s['ties']} "
        f"rate={(100*s['wins']/max(1,s['wins']+s['losses'])):.2f}%"
    )
    await _save_campaign()
    if s["sampled"]>=CAMPAIGN_MIN_TRADES:
        await _advance_campaign_if_complete()

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

def _analyze_snapshot_sync(assets,candles,prices,preferred,hints,limit,offset=0,council_votes=None,forced_strategy=None,return_all=False):
    candidates=[]
    council_votes=dict(council_votes or {})
    now=time.time()
    # Practice is deliberately capped and rotated so the learning laboratory
    # cannot monopolize the CPU or starve the live signal scheduler.
    pool=list(assets)
    n=max(1,int(limit or 8))
    start=int(offset)%len(pool) if pool else 0
    selected=[pool[(start+i)%len(pool)] for i in range(min(n,len(pool)))] if pool else []
    for asset in selected:
        pair=str(asset.get("pair") or "")
        if not pair:
            continue
        # Overnight research evaluates the complete authenticated account universe.
        # Non-executable/locked assets are still audited but are not forced into
        # broker orders by the DEMO execution gate.
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
            brain=analyze_asset(asset,cs,price,forced_strategy=forced_strategy)
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
        council_boost=3*int(council_votes.get(strategy,0) or 0)
        candidates.append((conf+council_boost,conf,float(brain.get("market_quality") or 0),brain,source,technique,council_boost))
    if not candidates:return None
    candidates.sort(key=lambda x:(x[0],x[1],x[2]),reverse=True)
    if return_all:
        out=[]
        for _,_,_,brain,source,technique,council_boost in candidates:
            out.append({
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
                "council_boost":council_boost,
            })
        return out
    _,_,_,brain,source,technique,council_boost=candidates[0]
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
        "council_boost":council_boost,
    }

async def _build_candidate(forced_strategy=None,return_all=False):
    provider=_cfg.get("snapshot_provider")
    if not provider:
        return None
    try:
        snap=await asyncio.wait_for(
            asyncio.to_thread(provider),
            timeout=LEARNING_SNAPSHOT_TIMEOUT_SECONDS,
        )
        assets=list(snap.get("assets") or [])
        candles=copy.deepcopy(snap.get("candles") or {})
        prices=copy.deepcopy(snap.get("prices") or {})
    except asyncio.TimeoutError:
        print(f"LEARNING_SNAPSHOT_TIMEOUT seconds={LEARNING_SNAPSHOT_TIMEOUT_SECONDS}")
        return None
    except Exception as e:
        print(f"LEARNING_SNAPSHOT_FAILED type={type(e).__name__} message={str(e)[:120]}")
        return None
    try:
        hints=await asyncio.wait_for(
            _load_research_hints(),
            timeout=LEARNING_RESEARCH_HINT_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        hints=[]
        print(f"LEARNING_RESEARCH_HINTS_TIMEOUT seconds={LEARNING_RESEARCH_HINT_TIMEOUT_SECONDS} fallback=core_strategy_families")
    except Exception as e:
        hints=[]
        print(f"LEARNING_RESEARCH_HINTS_FAILED type={type(e).__name__} message={str(e)[:120]} fallback=core_strategy_families")
    session_day=_learning_session_day(_now_uae())
    session_id=f"{session_day}:OVERNIGHT"
    base_preferred=[x[0] for x in hints] or [
        "TREND_FOLLOWING","MOMENTUM","BREAKOUT","PULLBACK",
        "REVERSAL","MEAN_REVERSION","PRICE_ACTION","VOLATILITY"
    ]
    if forced_strategy:
        forced_strategy=str(forced_strategy).upper().strip()
        base_preferred=[forced_strategy]
    council_payload={
        "session_day":str(session_day),
        "mode":"DEMO_ONLY",
        "market_universe":{"assets":len(assets),"candle_series":len(candles),"price_series":len(prices)},
        "research_hints":[{"strategy":st,"source":sid} for st,sid in hints[:16]],
        "allowed_strategies":base_preferred,
    }
    global _council_cache
    council=_council_cache.get("data") if (
        _council_cache.get("session_id")==session_id
        and time.time()-float(_council_cache.get("created_at") or 0.0) < COUNCIL_REFRESH_SECONDS
    ) else None
    if council:
        print(
            f"AI_STRATEGY_COUNCIL_REUSE session={session_id} "
            f"age={time.time()-float(_council_cache.get('created_at') or 0.0):.1f}s "
            f"refresh={COUNCIL_REFRESH_SECONDS}s"
        )
    else:
        try:
            council=await asyncio.wait_for(
                strategy_council_with_fallback(council_payload),
                timeout=COUNCIL_CALL_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            council={}
            print(f"AI_STRATEGY_COUNCIL_TIMEOUT seconds={COUNCIL_CALL_TIMEOUT_SECONDS} fallback=local_brain")
        except Exception as e:
            council={}
            print(f"AI_STRATEGY_COUNCIL_FAILED type={type(e).__name__} message={str(e)[:140]} fallback=local_brain")
        _council_cache={"session_id":session_id,"created_at":time.time(),"data":dict(council or {})}
        if council.get("proposals"):
            saved=await record_strategy_council(session_id,council)
            if not saved:
                print(f"AI_STRATEGY_COUNCIL_SAVE_FAILED session={session_id}")
    votes=dict(council.get("votes") or {})
    council_order=[str(x.get("strategy") or "").upper() for x in council.get("proposals") or [] if x.get("strategy")]
    preferred=list(dict.fromkeys(council_order+base_preferred))
    print(
        f"LEARNING_CANDIDATE_SCAN assets={len(assets)} candles={len(candles)} "
        f"prices={len(prices)} preferred={','.join(preferred[:12])} "
        f"council_members={int(council.get('member_count') or 0)} "
        f"consensus={council.get('consensus_strategy') or 'NONE'} "
        f"agreement={float(council.get('agreement') or 0.0):.3f}"
    )
    batch=len(assets) if forced_strategy else max(1,int(os.getenv("LEARNING_ASSET_BATCH","8")))
    global _practice_cursor
    offset=_practice_cursor
    _practice_cursor=(offset+batch)%max(1,len(assets)) if assets else 0
    candidate=await asyncio.to_thread(
        _analyze_snapshot_sync,assets,candles,prices,preferred,hints,batch,offset,votes,forced_strategy,return_all
    )
    if candidate:
        consensus=str(council.get("consensus_strategy") or "").upper()
        items=candidate if isinstance(candidate,list) else [candidate]
        for item in items:
            strategy=str(item.get("strategy") or "").upper()
            item["council_session_id"]=session_id
            item["council_consensus"]=consensus
            item["council_agreement"]=float(council.get("agreement") or 0.0)
            item["council_members"]=int(council.get("member_count") or 0)
            item["council_votes"]=votes
            if consensus and strategy==consensus:
                item["source"]="AI_COUNCIL_CONSENSUS"
        if isinstance(candidate,list):
            print(
                f"LEARNING_CANDIDATE_READY count={len(candidate)} "
                f"strategy={forced_strategy or 'COUNCIL'} consensus={consensus or 'NONE'}"
            )
        else:
            print(
                f"LEARNING_CANDIDATE_READY pair={candidate.get('pair')} "
                f"direction={candidate.get('direction')} confidence={candidate.get('confidence')} "
                f"strategy={candidate.get('strategy')} source={candidate.get('source')} "
                f"council_consensus={consensus or 'NONE'}"
            )
    if not candidate:
        print(f"LEARNING_CANDIDATE_NONE batch={batch} cursor={offset} reason=no_qualified_candidate")
    return candidate

async def _send_request(candidate, notify=True):
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
        if notify:
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


async def _send_campaign_batch(candidates):
    strategy=str(CAMPAIGN_STATE.get("strategy") or "").upper()
    if not strategy or not isinstance(candidates,list):
        return 0
    remaining=CAMPAIGN_MIN_TRADES-int(CAMPAIGN_STATE.get("sampled") or 0)
    if remaining<=0:
        return 0
    unique=[]; seen=set()
    for item in candidates:
        if not isinstance(item,dict):
            continue
        if str(item.get("strategy") or "").upper()!=strategy:
            continue
        if not item.get("signal_eligible",True):
            continue
        pair=str(item.get("pair") or "")
        if not pair or pair in seen:
            continue
        seen.add(pair)
        unique.append(item)
        if len(unique)>=remaining:
            break
    if not unique:
        print(f"LEARNING_CAMPAIGN_NO_ELIGIBLE_ASSETS strategy={strategy} reason=no_valid_fixed_strategy_setup")
        return 0

    async def one(item):
        async with CAMPAIGN_ASSET_SEM:
            return await _send_request(item,notify=False)

    results=await asyncio.gather(*(one(item) for item in unique),return_exceptions=True)
    accepted=sum(1 for r in results if r is True)
    failed=sum(1 for r in results if r is not True)
    minute=int(time.time()//60)
    print(
        f"LEARNING_CAMPAIGN_BATCH strategy={strategy} minute={minute} "
        f"evaluated={len(candidates)} selected={len(unique)} accepted={accepted} failed={failed} "
        f"completed={CAMPAIGN_STATE.get('sampled',0)}/100"
    )
    if accepted:
        try:
            await _cfg["send_message"](
                "🧠 DEMO LEARNING BATCH\n\n"
                f"🧩 Strategy → {strategy}\n"
                f"📊 Account assets evaluated → {len(candidates)}\n"
                f"🤖 DEMO trades started → {accepted}\n"
                f"🎯 Campaign progress → {CAMPAIGN_STATE.get('sampled',0)}/100 completed\n"
                "🔐 DEMO ACCOUNT ONLY",
                chat_id=_cfg.get("admin_id") or None
            )
        except Exception:
            pass
    return accepted

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
    _daily["day"] = str(_learning_session_day(_now_uae()))
    db_ok=await record_practice_result(rec.get("strategy","UNKNOWN"),result,
                                 pair=rec.get("pair",""),confidence=rec.get("confidence",0),
                                 context=ctx,error_code=error_code,
                                 council_context={
                                     "session_id":rec.get("council_session_id"),
                                     "consensus":rec.get("council_consensus"),
                                     "agreement":rec.get("council_agreement"),
                                     "members":rec.get("council_members"),
                                     "votes":rec.get("council_votes"),
                                 })
    await _campaign_result(result,rec.get("strategy","UNKNOWN"))
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
    """Resolve DEMO results only from a broker event or a fully closed M1 candle."""
    tid=str(rec.get("trade_id") or "")
    if not tid:
        return
    expiry_ts=float(rec.get("entry_ts") or rec.get("placed_at") or time.time()) + DURATION_SECONDS
    deadline=expiry_ts + RESULT_WATCH_EXTRA_SECONDS
    provider=_cfg.get("snapshot_provider")
    attempts=0
    pair=str(rec.get("pair") or "")

    def _normalise_candles(raw):
        out=[]
        def walk(value):
            if isinstance(value,dict):
                if isinstance(value.get("candles"),list):
                    for item in value["candles"]: walk(item)
                elif isinstance(value.get("data"),list):
                    for item in value["data"]: walk(item)
                elif any(k in value for k in ("open","o","high","h","low","l","close","c")):
                    out.append(value)
            elif isinstance(value,list):
                for item in value: walk(item)
        walk(raw)
        return out

    async def _direct_candles(attempt):
        client_provider=_cfg.get("client_provider")
        client=client_provider() if client_provider else None
        if not client:
            print(f"LEARNING_RESULT_DIRECT_CANDLE_UNAVAILABLE pair={pair} attempt={attempt} reason=no_client")
            return []
        connection=getattr(client,"connection",None)
        connected=bool(connection and getattr(connection,"is_connected",False))
        if not connected:
            recovered=False
            # Recover the existing authenticated client/session only; no new
            # credentials or login flow is created by the result watcher.
            for owner,name in (
                (client,"start"),(client,"reconnect"),
                (connection,"reconnect"),(connection,"connect")
            ):
                fn=getattr(owner,name,None) if owner is not None else None
                if not callable(fn):
                    continue
                try:
                    value=fn()
                    if hasattr(value,"__await__"):
                        await asyncio.wait_for(value,timeout=5.0)
                    recovered=True
                    print(f"LEARNING_RESULT_CONNECTION_RECOVERY pair={pair} attempt={attempt} method={name}")
                    break
                except Exception as exc:
                    print(
                        f"LEARNING_RESULT_CONNECTION_RECOVERY_FAILED pair={pair} "
                        f"attempt={attempt} method={name} type={type(exc).__name__} "
                        f"message={str(exc)[:100]}"
                    )
            connection=getattr(client,"connection",None)
            connected=bool(connection and getattr(connection,"is_connected",False))
            if not connected and not recovered:
                print(f"LEARNING_RESULT_DIRECT_CANDLE_UNAVAILABLE pair={pair} attempt={attempt} reason=broker_disconnected")
                return []
        try:
            fresh=await asyncio.wait_for(
                client.market.get_candles(pair,size=60,count=5),timeout=4.0
            )
            candles=_normalise_candles(fresh)
            print(
                f"LEARNING_RESULT_DIRECT_CANDLE_FETCH pair={pair} count={len(candles)} "
                f"attempt={attempt} connected={connected}"
            )
            return candles
        except Exception as exc:
            print(
                f"LEARNING_RESULT_DIRECT_CANDLE_RETRY pair={pair} attempt={attempt} "
                f"type={type(exc).__name__} message={str(exc)[:120]}"
            )
            return []

    def _eligible(items,current_time):
        eligible=[]
        for c in items:
            try:
                t=float(c.get("time",c.get("t")))
                if t>20_000_000_000: t/=1000
                close_ts=t+60.0
                # Never classify a forming candle or an old candle. The close
                # must have completed and correspond to this trade's expiry.
                if close_ts<=current_time and close_ts>=expiry_ts-2.0:
                    eligible.append((close_ts,float(c.get("close",c.get("c")))))
            except Exception:
                continue
        return eligible

    while tid in _open and tid not in _finalized and time.time()<deadline:
        attempts+=1
        try:
            now=time.time()
            if now<expiry_ts:
                await asyncio.sleep(min(RESULT_WATCH_POLL_SECONDS,max(1.0,expiry_ts-now)))
                continue

            snap=provider() if provider else {}
            candles=(snap.get("candles") or {}) if isinstance(snap,dict) else {}
            pair_candles=_normalise_candles(candles.get(pair,[]) if isinstance(candles,dict) else [])
            eligible=_eligible(pair_candles,now)

            # Critical fix: direct broker fetch is attempted whenever the local
            # snapshot has NO ELIGIBLE CLOSED EXPIRY CANDLE, not only when it is
            # completely empty. A stale non-empty snapshot caused the previous
            # timeout path to skip the direct fetch.
            if not eligible:
                direct=await _direct_candles(attempts)
                if direct:
                    eligible=_eligible(direct,now)

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
                print(
                    f"LEARNING_RESULT_FALLBACK_CONFIRMED trade_id={tid} pair={pair} "
                    f"attempt={attempts} close_ts={close_ts:.3f} entry={entry} "
                    f"exit={exitp} result={result} verification=closed-candle"
                )
                await _record_result(rec,result,"CLOSED_CANDLE_FALLBACK",exitp,None)
                return

            print(
                f"LEARNING_RESULT_WAIT trade_id={tid} pair={pair} attempt={attempts} "
                f"reason=no_closed_expiry_candle next_retry={RESULT_WATCH_POLL_SECONDS}s"
            )
        except Exception as exc:
            print(
                f"LEARNING_RESULT_FALLBACK_RETRY trade_id={tid} pair={pair} "
                f"attempt={attempts} type={type(exc).__name__} message={str(exc)[:120]}"
            )
        await asyncio.sleep(RESULT_WATCH_POLL_SECONDS)

    if tid in _open and tid not in _finalized:
        # A timeout is a data-availability failure, never a guessed WIN/LOSS.
        # Release the active practice slot so the learning loop continues.
        _open.pop(tid,None)
        await save_error(
            rec.get("strategy","UNKNOWN"),
            "RESULT_UNAVAILABLE_TIMEOUT",
            rec.get("technique") or {}
        )
        print(
            f"LEARNING_RESULT_SLOT_RELEASED trade_id={tid} pair={pair} "
            f"reason=RESULT_UNAVAILABLE_TIMEOUT attempts={attempts}"
        )
        await _cfg["send_message"](
            "⚠️ DEMO RESULT WATCH TIMEOUT\\n\\n"
            f"📊 {rec.get('display_name') or pair}\\n"
            "🔓 Next demo practice slot unlocked\\n"
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
        "🕕 Practice → 18:00–06:00 UAE\n"
        f"📅 Day → {day}\n"
        f"🤖 DEMO AUTO-TRADE → {_daily['placed']} trades\n"
        f"🟢 WIN → {_daily['win']}\n"
        f"🔴 LOSS → {_daily['loss']}\n"
        f"🟡 TIE → {_daily['tie']}\n"
        f"🎯 Accuracy → {accuracy:.1f}%\n"
        f"⛔ Blocked → {_daily['blocked']}\n\n"
        f"✅ VALIDATED / OWN STRATEGY READY → {len(validated)}\n"
        f"🧩 Strategies → {', '.join(validated[:20]) if validated else 'None'}\n\n"
        "🔴 06:00 → Learning Auto-Trade OFF\n"
        "🔐 ADMIN ONLY",
        chat_id=_cfg.get("admin_id") or None
    )

async def run_forever():
    if os.getenv("LEARNING_PRACTICE_ENABLED","true").strip().lower()=="false":
        return
    # Startup recovery must never block the learning scheduler. The Render/Postgres
    # connection can temporarily stall during a broker reconnect; DB state is recovery
    # metadata only, so bound both operations and continue into the live learning loop.
    try:
        await asyncio.wait_for(ensure_learning_trade_state_table(), timeout=5.0)
    except asyncio.TimeoutError:
        print("LEARNING_TRADE_STATE_INIT_TIMEOUT seconds=5 fallback=memory_only")
    except Exception as e:
        print(f"LEARNING_TRADE_STATE_INIT_STARTUP_FAILED type={type(e).__name__} message={str(e)[:120]} fallback=memory_only")
    try:
        await asyncio.wait_for(restore_open_trades(), timeout=5.0)
    except asyncio.TimeoutError:
        print("LEARNING_TRADE_RESTORE_TIMEOUT seconds=5 fallback=memory_only")
    except Exception as e:
        print(f"LEARNING_TRADE_RESTORE_STARTUP_FAILED type={type(e).__name__} message={str(e)[:120]} fallback=memory_only")
    try:
        await asyncio.wait_for(ensure_campaign_table(),timeout=5.0)
    except Exception as e:
        print(f"LEARNING_CAMPAIGN_INIT_FAILED type={type(e).__name__} message={str(e)[:120]}")
    print(
        "LEARNING_PRACTICE_LOOP_STARTED schedule=DAILY window=18:00-06:00 "
        "timezone=Asia/Dubai demo_only=True ai_council=True "
        "campaign=ONE_STRATEGY_AT_A_TIME target=100 min_win_rate=85% target=90%"
    )
    last_heartbeat=0.0
    last_scan=0.0
    while True:
        try:
            now=_now_uae()
            session_day=_learning_session_day(now)
            day=session_day
            if _daily["day"] != str(day):
                _daily.update({"day":str(day),"placed":0,"win":0,"loss":0,"tie":0,"blocked":0,"strategies":set()})
            global _last_report_day
            window_end=_window_end(day)
            if now >= window_end and _last_report_day != day:
                await _send_daily_report(day)
                _last_report_day = day
            if practice_active(now):
                if time.time()-last_heartbeat >= 60:
                    last_heartbeat=time.time()
                    print(
                        f"LEARNING_HEARTBEAT day={day} active=True pending={len(_pending)} "
                        f"open={len(_open)} placed={_daily['placed']} win={_daily['win']} "
                        f"loss={_daily['loss']} tie={_daily['tie']} blocked={_daily['blocked']} "
                        f"campaign_strategy={CAMPAIGN_STATE.get('strategy') or 'INIT'} "
                        f"campaign_progress={CAMPAIGN_STATE.get('sampled',0)}/100"
                    )
                if CAMPAIGN_STATE.get("session_id")!=str(day) or not CAMPAIGN_STATE.get("strategy"):
                    await _initialize_campaign(str(day))
                if CAMPAIGN_STATE.get("status")=="ACTIVE" and time.time()-last_scan >= LOOP_SECONDS:
                    last_scan=time.time()
                    strategy=str(CAMPAIGN_STATE.get("strategy") or "").upper()
                    print(
                        f"LEARNING_WINDOW_ACTIVE day={day} start=18:00 end=06:00 "
                        f"timezone=Asia/Dubai ai_council=True fixed_strategy={strategy} "
                        f"target=100 min_win_rate=85%"
                    )
                    try:
                        candidates=await asyncio.wait_for(
                            _build_candidate(strategy,return_all=True),
                            timeout=LEARNING_SCAN_TIMEOUT_SECONDS,
                        )
                    except asyncio.TimeoutError:
                        candidates=[]
                        print(f"LEARNING_SCAN_TIMEOUT seconds={LEARNING_SCAN_TIMEOUT_SECONDS} fallback=next_scan")
                    except Exception as e:
                        candidates=[]
                        print(f"LEARNING_SCAN_FAILED type={type(e).__name__} message={str(e)[:160]} fallback=next_scan")
                    await _send_campaign_batch(candidates)
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
            await asyncio.sleep(min(LOOP_SECONDS,30))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"LEARNING_PRACTICE_ERROR type={type(exc).__name__} message={str(exc)[:160]}")
            await asyncio.sleep(LOOP_SECONDS)
