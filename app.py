import asyncio,json,logging,os,time,secrets,hashlib,re
from collections import defaultdict,deque
from datetime import datetime,timezone,timedelta
from zoneinfo import ZoneInfo
from typing import Any
from telegram import Update
from telegram.ext import ContextTypes
import httpx
from olymptrade_ws import OlympTradeClient
from olymptrade_ws.olympconfig import parameters
from brain_rules import ActiveSignal,BrainState,rank_signal_candidates
from candice_brain import analyze_asset
from ai_engine import snapshot_from_asset
from ai_router import analyze_with_fallback,review_result_with_fallback
from m1_world_learning import learning_status as m1_learning_status, record_market_snapshot_async, world_learning_loop
from strategy_knowledge import ensure_tables as ensure_strategy_knowledge_tables, refresh as refresh_strategy_knowledge, refresh_loop as strategy_knowledge_refresh_loop
from account_asset_catalog import build_account_asset_snapshot
import learning_lab as learning_lab_module
from learning_lab import (
    configure as configure_learning_lab,
    status as learning_practice_status,
    handle_callback as handle_learning_callback,
    handle_trade_update as handle_learning_trade_update,
)

logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
class _BrokerTickCapabilityFilter(logging.Filter):
    def filter(self, record):
        msg = str(record.getMessage() or "")
        if record.name == "olymptrade_ws.api.market" and "Tick subscription event=12 rejected" in msg and ("invalid_request" in msg.lower() or "invalid request" in msg.lower()):
            record.levelno = logging.INFO
            record.levelname = "INFO"
        return True
logging.getLogger("olymptrade_ws.api.market").addFilter(_BrokerTickCapabilityFilter())

class _ResolvedDemoIdentityFilter(logging.Filter):
    def filter(self, record):
        msg=str(record.getMessage() or "")
        if record.name == "olymptrade_ws.core.client" and "SESSION_DEMO_ACCOUNT_NOT_VERIFIED" in msg:
            record.levelno=logging.INFO
            record.levelname="INFO"
        return True

logging.getLogger("olymptrade_ws.core.client").addFilter(_ResolvedDemoIdentityFilter())
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

SIGNAL_SESSION_START_HOUR=0
SIGNAL_SESSION_END_HOUR=24
# Fixed five-pass Candice scan window inside every 3-minute cycle.
# Offsets are measured backward from the 1-minute entry/expiry boundary.
CYCLE_SCAN_OFFSETS=(150.0,120.0,90.0,75.0,60.0)
CYCLE_SCAN_COUNT=len(CYCLE_SCAN_OFFSETS)
# The signal scheduler runs continuously across the full UAE day. It does NOT enable broker auto-trading.
FORCE_SIGNAL_MODE=os.getenv("FORCE_SIGNAL_MODE","0").strip().lower() in {"1","true","yes","on"}
ASSET_TRADEABILITY_PROBE_TIMEOUT=0.6
ASSET_TRADEABILITY_CACHE_TTL=45.0
ASSET_TRADEABILITY_HARD_MAX_AGE=50.0
# Final tick preparation is only an optimization for boundary delivery. It must
# never be allowed to block the 3-minute scheduler when broker event-12 probes stall.
FINAL_TICK_PREP_TIMEOUT=7.0
ASSET_TRADEABILITY_CACHE={}

KNOWN_BROKER_CLOSURES_UAE={
    "ALTCOIN": datetime(2026,9,27,23,52,tzinfo=UAE_TZ).timestamp(),
}

def known_broker_closure_reason(pair, now_ts=None):
    p=str(pair or "").strip().upper()
    until=KNOWN_BROKER_CLOSURES_UAE.get(p)
    if until is None:
        return ""
    now=time.time() if now_ts is None else float(now_ts)
    if now < until:
        return f"terminal_closed_until:{datetime.fromtimestamp(until,UAE_TZ).strftime('%Y-%m-%d %H:%M %Z')}"
    return ""


def broker_unavailable_reason(item):
    """Return a broker-provided temporary/unavailable reason, or empty string."""
    if not isinstance(item,dict):
        return "invalid_asset_record"
    if item.get("disabled") is True:
        return "disabled"
    if item.get("locked") is True:
        return "locked"
    if item.get("locked_trading") is True:
        return "locked_trading"
    for key in ("active","available","tradable","is_active","is_available","is_tradable"):
        if key in item and item.get(key) is False:
            return f"{key}=false"
    status=str(item.get("status") or item.get("state") or "").strip().lower()
    if status in {"disabled","locked","inactive","unavailable","closed","off"}:
        return status
    return ""

async def check_broker_asset_tradeability(pair, cycle_id=None, force=False):
    """Return False only when the authenticated broker explicitly says the asset is closed."""
    p=str(pair or "").strip()
    now=time.time()
    if not p or not CLIENT or not getattr(CLIENT,"account_id",None):
        return True

    cached=ASSET_TRADEABILITY_CACHE.get(p)
    if (
        not force
        and isinstance(cached,dict)
        and now-float(cached.get("checked_at") or 0.0) <= ASSET_TRADEABILITY_CACHE_TTL
    ):
        state=str(cached.get("state") or "UNKNOWN").upper()
        allowed=state!="CLOSED"
        log.info(
            "BROKER_TRADEABILITY_CHECK cycle=%s pair=%s state=%s source=cache age=%.2f reason=%s",
            cycle_id,p,state,max(0.0,now-float(cached.get("checked_at") or now)),
            cached.get("reason") or "cached"
        )
        asset=next((a for a in STATE.get("assets") or [] if str(a.get("pair"))==p),None)
        if asset is not None:
            asset["broker_tradeable"]=(
                True if state=="OPEN" else False if state=="CLOSED" else None
            )
            asset["broker_tradeability_checked_at"]=float(cached.get("checked_at") or now)
            asset["broker_tradeability_source"]=str(cached.get("source") or "cache")
        if state=="CLOSED":
            STATE.get("analyses",{}).pop(p,None)
        return allowed

    method=getattr(getattr(CLIENT,"market",None),"probe_asset_tradeability",None)
    if not callable(method):
        log.warning(
            "BROKER_TRADEABILITY_CHECK cycle=%s pair=%s state=UNKNOWN source=unavailable reason=probe_method_missing",
            cycle_id,p
        )
        return True
    try:
        strike=await asyncio.wait_for(
            method(p,category="digital",timeout=ASSET_TRADEABILITY_PROBE_TIMEOUT),
            timeout=ASSET_TRADEABILITY_PROBE_TIMEOUT+1.0,
        )
        checked_at=time.time()
        if strike is None:
            state="UNKNOWN"
            reason="event80_timeout_or_no_explicit_status"
        elif isinstance(strike,dict) and strike.get("__broker_tradeable") is False:
            state="CLOSED"
            reason=str(strike.get("__probe_reason") or "broker_rejected")
        else:
            state="OPEN"
            reason=(
                str(strike.get("__probe_reason") or "fresh_strike_confirmed")
                if isinstance(strike,dict) else "fresh_response"
            )
        ASSET_TRADEABILITY_CACHE[p]={
            "state":state,
            "checked_at":checked_at,
            "tradeable":(True if state=="OPEN" else False if state=="CLOSED" else None),
            "reason":reason,
            "source":"event95+event80",
        }
        asset=next((a for a in STATE.get("assets") or [] if str(a.get("pair"))==p),None)
        if asset is not None:
            asset["broker_tradeable"]=(
                True if state=="OPEN" else False if state=="CLOSED" else None
            )
            asset["broker_tradeability_checked_at"]=checked_at
            asset["broker_tradeability_source"]="event95+event80"
        if state=="CLOSED":
            STATE.get("analyses",{}).pop(p,None)
            log.info(
                "BROKER_TRADEABILITY_CHECK cycle=%s pair=%s state=CLOSED source=event95+event80 reason=%s",
                cycle_id,p,reason
            )
            return False
        log.info(
            "BROKER_TRADEABILITY_CHECK cycle=%s pair=%s state=%s source=event95+event80 reason=%s",
            cycle_id,p,state,reason
        )
        return True
    except Exception as e:
        checked_at=time.time()
        reason=f"{type(e).__name__}:{str(e)[:100]}"
        ASSET_TRADEABILITY_CACHE[p]={
            "state":"UNKNOWN","checked_at":checked_at,"tradeable":None,
            "reason":reason,"source":"event95+event80",
        }
        asset=next((a for a in STATE.get("assets") or [] if str(a.get("pair"))==p),None)
        if asset is not None:
            asset["broker_tradeable"]=None
            asset["broker_tradeability_checked_at"]=checked_at
            asset["broker_tradeability_source"]="event95+event80"
        log.info(
            "BROKER_TRADEABILITY_CHECK cycle=%s pair=%s state=UNKNOWN source=event95+event80 reason=%s",
            cycle_id,p,reason
        )
        return True

def broker_tradeability_fresh(pair, reference_ts=None):
    p=str(pair or "").strip()
    rec=ASSET_TRADEABILITY_CACHE.get(p)
    if not isinstance(rec,dict):
        return False
    state=str(rec.get("state") or "UNKNOWN").upper()
    if state=="CLOSED":
        return False
    if state not in {"OPEN","UNKNOWN"}:
        return False
    ref=time.time() if reference_ts is None else float(reference_ts)
    try:
        age=ref-float(rec.get("checked_at") or 0.0)
    except (TypeError,ValueError):
        return False
    return 0.0<=age<=ASSET_TRADEABILITY_HARD_MAX_AGE
def signal_session_active(ts=None):
    if FORCE_SIGNAL_MODE:
        return True
    now=datetime.fromtimestamp(float(ts if ts is not None else time.time()),tz=UAE_TZ)
    return SIGNAL_SESSION_START_HOUR <= now.hour < SIGNAL_SESSION_END_HOUR

def next_signal_session_start_epoch(ts=None):
    now=datetime.fromtimestamp(float(ts if ts is not None else time.time()),tz=UAE_TZ)
    start=now.replace(hour=SIGNAL_SESSION_START_HOUR,minute=0,second=0,microsecond=0)
    if now>=start:
        start=start+timedelta(days=1)
    return start.timestamp()
# Cycle-state persistence is recovery metadata only. It must never be allowed
# to block the market/signal scheduler when the database stalls.
CYCLE_STATE_IO_TIMEOUT=2.0
CYCLE_STATE_DB_COOLDOWN=10.0
CYCLE_STATE_DB_BLOCKED_UNTIL=0.0
CYCLE_STATE_WRITE_LOCK=asyncio.Lock()
AI_REVIEW_QUEUE_POLL_SECONDS=0.5
AI_REVIEW_QUEUE_STALE_SECONDS=600.0
AI_REVIEW_QUEUE_RETRY_DELAYS=(5,15,30,60,120,300)
RESULT_WATCH_QUEUE_STALE_SECONDS=600.0
ACCOUNT_TICK_CONTROL_LOCK=asyncio.Lock()

# Durable scheduler state. Render can replace the running instance at any time;
# this state keeps the active five-pass window and candidate pool recoverable.
SCHEDULER_OWNER=secrets.token_hex(8)

async def ensure_cycle_state_table():
    if not LEARNING_DB_URL:
        log.error("CYCLE_STATE_DB_REQUIRED")
        return False
    try:
        import psycopg
        def init():
            with psycopg.connect(LEARNING_DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS candice_cycle_state (
                            cycle_id BIGINT PRIMARY KEY,
                            target_epoch DOUBLE PRECISION NOT NULL,
                            signal_epoch DOUBLE PRECISION NOT NULL,
                            signal_lead INTEGER NOT NULL,
                            completed_pass INTEGER NOT NULL DEFAULT 0,
                            candidate_pool JSONB NOT NULL DEFAULT '[]'::jsonb,
                            status TEXT NOT NULL DEFAULT 'ACTIVE',
                            last_reason TEXT,
                            owner TEXT,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            completed_at TIMESTAMPTZ
                        )
                    """)
                    cur.execute("""
                        DELETE FROM candice_cycle_state
                        WHERE updated_at < NOW() - INTERVAL '1 day'
                    """)
                db.commit()
        await asyncio.to_thread(init)
        log.info("CYCLE_STATE_QUEUE_READY")
        return True
    except Exception as e:
        log.error("CYCLE_STATE_INIT_FAILED type=%s message=%s",
                  type(e).__name__,str(e)[:180])
        return False

def _cycle_state_json(pool):
    try:
        values=list(pool.values()) if isinstance(pool,dict) else list(pool or [])
        return json.loads(json.dumps(
            values,separators=(",",":"),ensure_ascii=False,default=str
        ))
    except Exception:
        return []

async def save_cycle_state(
    cycle_id,target_epoch,signal_epoch,signal_lead,completed_pass,
    candidate_pool,status="ACTIVE",reason=""
):
    if not LEARNING_DB_URL: return False
    # Recovery metadata is best-effort and already runs in create_task(). Keep
    # one Postgres write at a time so repeated passes cannot create a connection
    # storm or leave cancelled database threads behind.
    if CYCLE_STATE_WRITE_LOCK.locked():
        log.info("CYCLE_STATE_SAVE_COALESCED cycle=%s pass=%s reason=write_in_progress",
                 cycle_id,completed_pass)
        return False
    try:
        import psycopg
        payload=json.dumps(_cycle_state_json(candidate_pool),
                           separators=(",",":"),ensure_ascii=False,default=str)
        async with CYCLE_STATE_WRITE_LOCK:
            def put():
                with psycopg.connect(
                    LEARNING_DB_URL,
                    connect_timeout=max(3,int(CYCLE_STATE_IO_TIMEOUT)+1),
                ) as db:
                    with db.cursor() as cur:
                        cur.execute("""
                            INSERT INTO candice_cycle_state(
                                cycle_id,target_epoch,signal_epoch,signal_lead,
                                completed_pass,candidate_pool,status,last_reason,owner
                            )
                            VALUES(%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s)
                            ON CONFLICT(cycle_id) DO UPDATE SET
                                target_epoch=EXCLUDED.target_epoch,
                                signal_epoch=EXCLUDED.signal_epoch,
                                signal_lead=EXCLUDED.signal_lead,
                                completed_pass=EXCLUDED.completed_pass,
                                candidate_pool=EXCLUDED.candidate_pool,
                                status=EXCLUDED.status,
                                last_reason=EXCLUDED.last_reason,
                                owner=EXCLUDED.owner,
                                updated_at=NOW(),
                                completed_at=CASE
                                    WHEN EXCLUDED.status IN ('SENT','SKIPPED') THEN NOW()
                                    ELSE candice_cycle_state.completed_at
                                END
                        """,(
                            int(cycle_id),float(target_epoch),float(signal_epoch),
                            int(signal_lead),int(completed_pass),payload,status,
                            str(reason)[:500],SCHEDULER_OWNER
                        ))
                    db.commit()
            await asyncio.to_thread(put)
        log.info("CYCLE_STATE_SAVED cycle=%s pass=%s status=%s",
                 cycle_id,completed_pass,status)
        return True
    except Exception as e:
        log.warning("CYCLE_STATE_SAVE_FAILED cycle=%s type=%s message=%s",
                    cycle_id,type(e).__name__,str(e)[:180])
        return False

async def load_recoverable_cycle_state(now=None):
    global CYCLE_STATE_DB_BLOCKED_UNTIL
    if not LEARNING_DB_URL:
        return None
    if time.time() < CYCLE_STATE_DB_BLOCKED_UNTIL:
        return None
    now=float(now or time.time())
    try:
        import psycopg
        def read():
            with psycopg.connect(LEARNING_DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT cycle_id,target_epoch,signal_epoch,signal_lead,
                               completed_pass,candidate_pool
                        FROM candice_cycle_state
                        WHERE status='ACTIVE'
                          AND signal_epoch > %s
                        ORDER BY target_epoch DESC
                        LIMIT 1
                    """,(now,))
                    return cur.fetchone()
        row=await asyncio.wait_for(
            asyncio.to_thread(read),
            timeout=CYCLE_STATE_IO_TIMEOUT,
        )
        if not row:
            return None
        cycle_id,target_epoch,signal_epoch,signal_lead,completed_pass,pool=row
        try:
            pool=list(pool or [])
        except Exception:
            pool=[]
        log.info(
            "CYCLE_STATE_RECOVERED cycle=%s completed_pass=%s candidates=%s "
            "signal_utc=%s target_utc=%s",
            cycle_id,completed_pass,len(pool),
            datetime.fromtimestamp(float(signal_epoch),tz=timezone.utc).strftime("%H:%M:%S"),
            datetime.fromtimestamp(float(target_epoch),tz=timezone.utc).strftime("%H:%M:%S")
        )
        return {
            "cycle_id":int(cycle_id),
            "target_epoch":float(target_epoch),
            "signal_epoch":float(signal_epoch),
            "signal_lead":float(signal_lead),
            "completed_pass":int(completed_pass or 0),
            "candidate_pool":pool,
        }
    except asyncio.TimeoutError:
        CYCLE_STATE_DB_BLOCKED_UNTIL=time.time()+CYCLE_STATE_DB_COOLDOWN
        log.warning(
            "CYCLE_STATE_RECOVERY_READ_FAILED type=TimeoutError message=database_read_timeout timeout=%.2fs cooldown=%.0fs",
            CYCLE_STATE_IO_TIMEOUT,CYCLE_STATE_DB_COOLDOWN
        )
        return None
    except Exception as e:
        log.warning(
            "CYCLE_STATE_RECOVERY_READ_FAILED type=%s message=%s",
            type(e).__name__,str(e)[:160]
        )
        return None

async def mark_cycle_state(cycle_id,status,reason=""):
    if not LEARNING_DB_URL: return False
    try:
        import psycopg
        async with CYCLE_STATE_WRITE_LOCK:
            def done():
                with psycopg.connect(
                    LEARNING_DB_URL,
                    connect_timeout=max(3,int(CYCLE_STATE_IO_TIMEOUT)+1),
                ) as db:
                    with db.cursor() as cur:
                        cur.execute("""
                            UPDATE candice_cycle_state
                            SET status=%s,last_reason=%s,updated_at=NOW(),
                                completed_at=NOW(),owner=%s
                            WHERE cycle_id=%s
                        """,(status,str(reason)[:500],SCHEDULER_OWNER,int(cycle_id)))
                    db.commit()
            await asyncio.to_thread(done)
        log.info("CYCLE_STATE_MARKED cycle=%s status=%s",cycle_id,status)
        return True
    except Exception as e:
        log.warning("CYCLE_STATE_MARK_FAILED cycle=%s status=%s type=%s message=%s",
                    cycle_id,status,type(e).__name__,str(e)[:160])
        return False

async def ensure_ai_review_queue_table():
    if not LEARNING_DB_URL:
        log.error("AI_REVIEW_QUEUE_DB_REQUIRED")
        return False
    try:
        import psycopg
        def init():
            with psycopg.connect(LEARNING_DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS candice_ai_review_queue (
                            review_id TEXT PRIMARY KEY,
                            record JSONB NOT NULL,
                            attempts INTEGER NOT NULL DEFAULT 0,
                            status TEXT NOT NULL DEFAULT 'PENDING',
                            next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            last_error TEXT,
                            last_provider TEXT,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            completed_at TIMESTAMPTZ
                        )
                    """)
                    cur.execute("""
                        UPDATE candice_ai_review_queue
                        SET status='PENDING',next_attempt_at=NOW(),updated_at=NOW()
                        WHERE status='PROCESSING'
                          AND updated_at < NOW() - INTERVAL '10 minutes'
                    """)
                db.commit()
        await asyncio.to_thread(init)
        log.info("AI_REVIEW_QUEUE_READY")
        return True
    except Exception as e:
        log.error("AI_REVIEW_QUEUE_INIT_FAILED type=%s message=%s",
                  type(e).__name__,str(e)[:180])
        return False

async def enqueue_ai_review(review_id,rec):
    if not LEARNING_DB_URL:
        log.error("AI_REVIEW_QUEUE_ENQUEUE_FAILED review_id=%s reason=db_not_configured",review_id)
        return False
    try:
        import psycopg
        payload=json.dumps(rec,separators=(",",":"),ensure_ascii=False)
        def put():
            with psycopg.connect(LEARNING_DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        INSERT INTO candice_ai_review_queue(review_id,record)
                        VALUES(%s,%s::jsonb)
                        ON CONFLICT(review_id) DO NOTHING
                    """,(review_id,payload))
                db.commit()
        await asyncio.to_thread(put)
        log.info("AI_REVIEW_ENQUEUED review_id=%s pair=%s result=%s",
                 review_id,rec.get("pair"),rec.get("result"))
        return True
    except Exception as e:
        log.error("AI_REVIEW_QUEUE_ENQUEUE_FAILED review_id=%s type=%s message=%s",
                  review_id,type(e).__name__,str(e)[:180])
        return False

async def claim_ai_review():
    if not LEARNING_DB_URL:
        return None
    try:
        import psycopg
        def claim():
            with psycopg.connect(LEARNING_DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT review_id,record,attempts
                        FROM candice_ai_review_queue
                        WHERE status='PENDING' AND next_attempt_at <= NOW()
                        ORDER BY created_at
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    """)
                    row=cur.fetchone()
                    if not row:
                        db.commit()
                        return None
                    review_id,record,attempts=row
                    next_attempt=int(attempts or 0)+1
                    cur.execute("""
                        UPDATE candice_ai_review_queue
                        SET status='PROCESSING',attempts=%s,updated_at=NOW()
                        WHERE review_id=%s
                    """,(next_attempt,review_id))
                db.commit()
                return str(review_id),record,next_attempt
        return await asyncio.to_thread(claim)
    except Exception as e:
        log.warning("AI_REVIEW_QUEUE_CLAIM_FAILED type=%s message=%s",
                    type(e).__name__,str(e)[:160])
        return None

async def complete_ai_review(review_id,provider):
    if not LEARNING_DB_URL: return False
    try:
        import psycopg
        def done():
            with psycopg.connect(LEARNING_DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        UPDATE candice_ai_review_queue
                        SET status='COMPLETED',last_provider=%s,
                            completed_at=NOW(),updated_at=NOW()
                        WHERE review_id=%s
                    """,(provider,review_id))
                db.commit()
        await asyncio.to_thread(done)
        return True
    except Exception as e:
        log.warning("AI_REVIEW_QUEUE_COMPLETE_FAILED review_id=%s type=%s message=%s",
                    review_id,type(e).__name__,str(e)[:160])
        return False

async def retry_ai_review(review_id,attempts,error):
    if not LEARNING_DB_URL: return False
    delay=AI_REVIEW_QUEUE_RETRY_DELAYS[
        min(max(int(attempts)-1,0),len(AI_REVIEW_QUEUE_RETRY_DELAYS)-1)
    ]
    next_at=datetime.now(timezone.utc)+timedelta(seconds=delay)
    try:
        import psycopg
        def retry():
            with psycopg.connect(LEARNING_DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        UPDATE candice_ai_review_queue
                        SET status='PENDING',next_attempt_at=%s,last_error=%s,updated_at=NOW()
                        WHERE review_id=%s
                    """,(next_at,str(error)[:500],review_id))
                db.commit()
        await asyncio.to_thread(retry)
        return True
    except Exception as e:
        log.warning("AI_REVIEW_QUEUE_RETRY_SAVE_FAILED review_id=%s type=%s message=%s",
                    review_id,type(e).__name__,str(e)[:160])
        return False

async def ai_review_worker():
    while True:
        try:
            item=await claim_ai_review()
            if not item:
                await asyncio.sleep(AI_REVIEW_QUEUE_POLL_SECONDS)
                continue
            review_id,rec,attempts=item
            log.info("AI_REVIEW_WORKER_STARTED review_id=%s attempt=%s pair=%s",
                     review_id,attempts,rec.get("pair"))
            try:
                review=await review_result_with_fallback(rec)
                review["review_id"]=review_id
                BRAIN.apply_ai_review(rec,review)
                await save_persistent_learning()
                if not await complete_ai_review(review_id,review.get("provider","external")):
                    raise RuntimeError("queue completion update failed")
                log.info("AI_REVIEW_WORKER_COMPLETED review_id=%s pair=%s result=%s provider=%s attempt=%s",
                         review_id,rec.get("pair"),rec.get("result"),
                         review.get("provider","external"),attempts)
            except Exception as e:
                saved=await retry_ai_review(review_id,attempts,e)
                delay=AI_REVIEW_QUEUE_RETRY_DELAYS[
                    min(max(int(attempts)-1,0),len(AI_REVIEW_QUEUE_RETRY_DELAYS)-1)
                ]
                log.warning("AI_REVIEW_WORKER_RETRY review_id=%s pair=%s attempt=%s saved=%s error=%s next_retry_seconds=%s",
                            review_id,rec.get("pair"),attempts,saved,str(e)[:180],delay)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("AI_REVIEW_WORKER_ERROR type=%s message=%s",
                          type(e).__name__,str(e)[:180])
            await asyncio.sleep(AI_REVIEW_QUEUE_POLL_SECONDS)

async def ensure_result_watch_queue_table():
    if not LEARNING_DB_URL:
        log.error("RESULT_WATCH_QUEUE_DB_REQUIRED")
        return False
    try:
        import psycopg
        def init():
            with psycopg.connect(LEARNING_DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS candice_result_watch_queue (
                            watch_id TEXT PRIMARY KEY,
                            record JSONB NOT NULL,
                            status TEXT NOT NULL DEFAULT 'PROCESSING',
                            last_error TEXT,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            completed_at TIMESTAMPTZ
                        )
                    """)
                    # A Render restart leaves PROCESSING rows behind. There is
                    # one service instance, so on process startup every unfinished
                    # watch is safe to resume from the persisted signal snapshot.
                    cur.execute("""
                        UPDATE candice_result_watch_queue
                        SET status='PENDING',updated_at=NOW()
                        WHERE status='PROCESSING'
                    """)
                db.commit()
        await asyncio.to_thread(init)
        log.info("RESULT_WATCH_QUEUE_READY")
        return True
    except Exception as e:
        log.error("RESULT_WATCH_QUEUE_INIT_FAILED type=%s message=%s",
                  type(e).__name__,str(e)[:180])
        return False


def _result_watch_payload(s):
    return {
        "cycle_id":int(s.cycle_id),
        "account_id":s.account_id,
        "pair":s.pair,
        "display_name":s.display_name,
        "direction":s.direction,
        "expiry_minutes":int(s.expiry_minutes),
        "entry_price":float(s.entry_price),
        "entry_ts":float(s.entry_ts),
        "entry_candle_ts":s.entry_candle_ts,
        "strategy":s.strategy,
        "reason":s.reason,
        "confidence":int(s.confidence),
        "pattern":s.pattern,
        "trend_15m":s.trend_15m,
        "structure_1m":s.structure_1m,
        "self_strategy":s.self_strategy,
        "self_strategy_version":s.self_strategy_version,
        "indicator_context":dict(s.indicator_context or {}),
    }

async def enqueue_result_watch(watch_id,s):
    if not LEARNING_DB_URL:
        log.error("RESULT_WATCH_QUEUE_ENQUEUE_FAILED watch_id=%s reason=db_not_configured",watch_id)
        return False
    try:
        import psycopg
        payload=json.dumps(_result_watch_payload(s),separators=(",",":"),ensure_ascii=False,default=str)
        def put():
            with psycopg.connect(LEARNING_DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        INSERT INTO candice_result_watch_queue(watch_id,record,status)
                        VALUES(%s,%s::jsonb,'PROCESSING')
                        ON CONFLICT(watch_id) DO NOTHING
                    """,(watch_id,payload))
                db.commit()
        await asyncio.to_thread(put)
        log.info("RESULT_WATCH_ENQUEUED watch_id=%s pair=%s expiry=%s",
                 watch_id,s.pair,s.expiry_minutes)
        return True
    except Exception as e:
        log.error("RESULT_WATCH_QUEUE_ENQUEUE_FAILED watch_id=%s type=%s message=%s",
                  watch_id,type(e).__name__,str(e)[:180])
        return False

async def persist_result_watch_background(watch_id,s):
    """Persist the watch outside the signal-critical path so DB latency cannot affect timing."""
    try:
        ok=await asyncio.wait_for(enqueue_result_watch(watch_id,s),timeout=5.0)
        if not ok:
            log.warning("RESULT_WATCH_BACKGROUND_PERSIST_FAILED watch_id=%s",watch_id)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.warning("RESULT_WATCH_BACKGROUND_PERSIST_FAILED watch_id=%s type=%s message=%s",
                    watch_id,type(e).__name__,str(e)[:160])

# Per-process guard: a broker reconnect/re-authentication must never start a second watcher for the same signal.
RESULT_WATCH_TASKS: dict[str, asyncio.Task] = {}


def start_result_watch(key):
    existing=RESULT_WATCH_TASKS.get(key)
    if existing is not None and not existing.done():
        log.info("RESULT_WATCH_DUPLICATE_SUPPRESSED watch_key=%s",key)
        return False
    task=asyncio.create_task(result_watch(key))
    RESULT_WATCH_TASKS[key]=task

    def _clear(done_task):
        if RESULT_WATCH_TASKS.get(key) is done_task:
            RESULT_WATCH_TASKS.pop(key,None)
    task.add_done_callback(_clear)
    return True


async def complete_result_watch(watch_id):
    if not LEARNING_DB_URL:return False
    try:
        import psycopg
        def done():
            with psycopg.connect(LEARNING_DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        UPDATE candice_result_watch_queue
                        SET status='COMPLETED',completed_at=NOW(),updated_at=NOW()
                        WHERE watch_id=%s
                    """,(watch_id,))
                db.commit()
        await asyncio.to_thread(done)
        log.info("RESULT_WATCH_COMPLETED watch_id=%s",watch_id)
        return True
    except Exception as e:
        log.warning("RESULT_WATCH_QUEUE_COMPLETE_FAILED watch_id=%s type=%s message=%s",
                    watch_id,type(e).__name__,str(e)[:160])
        return False

async def mark_result_watch_error(watch_id,error):
    if not LEARNING_DB_URL:return False
    try:
        import psycopg
        def save():
            with psycopg.connect(LEARNING_DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        UPDATE candice_result_watch_queue
                        SET status='PROCESSING',last_error=%s,updated_at=NOW()
                        WHERE watch_id=%s
                    """,(str(error)[:500],watch_id))
                db.commit()
        await asyncio.to_thread(save)
        return True
    except Exception as e:
        log.warning("RESULT_WATCH_QUEUE_ERROR_SAVE_FAILED watch_id=%s type=%s message=%s",
                    watch_id,type(e).__name__,str(e)[:160])
        return False

async def restore_pending_result_watches():
    if not LEARNING_DB_URL or not CLIENT or not CLIENT.connection.is_connected:
        return 0
    try:
        import psycopg
        def read():
            with psycopg.connect(LEARNING_DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT watch_id,record
                        FROM candice_result_watch_queue
                        WHERE status IN ('PENDING','PROCESSING')
                          AND created_at > NOW() - INTERVAL '2 days'
                        ORDER BY created_at
                    """)
                    return cur.fetchall()
        rows=await asyncio.to_thread(read)
        restored=0
        for watch_id,record in rows:
            try:
                cycle_id=int(record["cycle_id"])
                pair=str(record["pair"])
                entry_ts=float(record["entry_ts"])
                key=f"{cycle_id}:{pair}:{entry_ts}"
                if key not in BRAIN.active_signals:
                    BRAIN.active_signals[key]=ActiveSignal(
                        cycle_id=cycle_id,
                        account_id=int(record["account_id"]) if record.get("account_id") is not None else None,
                        pair=pair,
                        display_name=str(record.get("display_name") or pair),
                        direction=str(record.get("direction") or "").upper(),
                        expiry_minutes=int(record.get("expiry_minutes") or 1),
                        entry_price=float(record.get("entry_price") or 0),
                        entry_ts=entry_ts,
                        entry_candle_ts=record.get("entry_candle_ts"),
                        strategy=str(record.get("strategy") or ""),
                        reason=str(record.get("reason") or ""),
                        confidence=int(record.get("confidence") or 0),
                        pattern=str(record.get("pattern") or ""),
                        trend_15m=str(record.get("trend_15m") or ""),
                        structure_1m=str(record.get("structure_1m") or ""),
                        self_strategy=str(record.get("self_strategy") or ""),
                        self_strategy_version=str(record.get("self_strategy_version") or ""),
                        indicator_context=dict(record.get("indicator_context") or {}),
                    )
                if start_result_watch(key):
                    restored+=1
                log.info("RESULT_WATCH_RESTORED watch_id=%s pair=%s entry_ts=%s",watch_id,pair,entry_ts)
            except Exception as e:
                log.warning("RESULT_WATCH_RESTORE_FAILED watch_id=%s type=%s message=%s",
                            watch_id,type(e).__name__,str(e)[:160])
        return restored
    except Exception as e:
        log.warning("RESULT_WATCH_RESTORE_SCAN_FAILED type=%s message=%s",
                    type(e).__name__,str(e)[:160])
        return 0

# Secure member access: one admin-generated code grants all MB* IDs for 24h.
ACCESS_TTL=timedelta(hours=24)
ACCESS_CODE_TTL=timedelta(hours=24)
ACCESS_CODE_DIGITS=10
ADMIN_TELEGRAM_ID=os.getenv("ADMIN_TELEGRAM_ID","").strip()
ADMIN_LIFETIME_CODE=os.getenv("ADMIN_LIFETIME_CODE","").strip()
ADMIN_EMAIL=os.getenv("ADMIN_EMAIL","").strip()
MAKE_ACCESS_CODE_WEBHOOK=os.getenv("MAKE_ACCESS_CODE_WEBHOOK","").strip()
RESEND_API_KEY=os.getenv("RESEND_API_KEY","").strip()
ACCESS_CODE_FROM_EMAIL=os.getenv("ACCESS_CODE_FROM_EMAIL","").strip()

def configured_member_ids():
    ids=set()
    for key,value in os.environ.items():
        if re.fullmatch(r"MB[1-9][0-9]*",key) and value.strip():
            try: ids.add(int(value.strip()))
            except ValueError: log.warning("MEMBER_ID_INVALID key=%s",key)
    return ids

def _access_code_hash(code): return hashlib.sha256(code.encode()).hexdigest()

async def ensure_access_table():
    if not LEARNING_DB_URL: log.error("MEMBER_ACCESS_DB_REQUIRED"); return False
    try:
        import psycopg
        def init():
            with psycopg.connect(LEARNING_DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""CREATE TABLE IF NOT EXISTS nexora_member_access (
                        id SMALLINT PRIMARY KEY, code_hash TEXT, code_expires_at TIMESTAMPTZ,
                        access_expires_at TIMESTAMPTZ, granted_at TIMESTAMPTZ,
                        granted_by BIGINT, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
                    cur.execute("""CREATE TABLE IF NOT EXISTS nexora_access_audit (
                        id BIGSERIAL PRIMARY KEY, event TEXT NOT NULL, actor BIGINT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
                db.commit()
        await asyncio.to_thread(init); return True
    except Exception as e:
        log.error("MEMBER_ACCESS_DB_INIT_FAILED type=%s message=%s",type(e).__name__,str(e)[:160]); return False

async def audit_access(event,actor=None):
    if not LEARNING_DB_URL: return
    try:
        import psycopg
        def write():
            with psycopg.connect(LEARNING_DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur: cur.execute("INSERT INTO nexora_access_audit(event,actor) VALUES(%s,%s)",(event,actor))
                db.commit()
        await asyncio.to_thread(write)
    except Exception as e: log.warning("ACCESS_AUDIT_FAILED type=%s",type(e).__name__)

async def send_access_code_email(code):
    if not ADMIN_EMAIL:
        log.error("ACCESS_EMAIL_NOT_CONFIGURED"); return False
    try:
        subject="NEXORA-AI Admin Access Code"
        text=f"NEXORA-AI 24-hour member access code:\n\n{code}\n\nUse /mbaccess CODE from your Admin Telegram only."
        if MAKE_ACCESS_CODE_WEBHOOK:
            payload={
                "type":"nexora_access_code",
                "code":str(code),
                "to":[ADMIN_EMAIL],
                "subject":subject,
                "text":text,
            }
            async with httpx.AsyncClient(timeout=10) as h:
                r=await h.post(
                    MAKE_ACCESS_CODE_WEBHOOK,
                    headers={"Content-Type":"application/json"},
                    json=payload,
                )
                r.raise_for_status()
            log.info("ACCESS_EMAIL_SENT via=make_webhook")
            return True
        if not (RESEND_API_KEY and ACCESS_CODE_FROM_EMAIL):
            log.error("ACCESS_EMAIL_NOT_CONFIGURED"); return False
        payload={"from":ACCESS_CODE_FROM_EMAIL,"to":[ADMIN_EMAIL],"subject":subject,"text":text}
        async with httpx.AsyncClient(timeout=10) as h:
            r=await h.post(
                "https://api.resend.com/emails",
                headers={"Authorization":f"Bearer {RESEND_API_KEY}","Content-Type":"application/json"},
                json=payload,
            )
            r.raise_for_status()
        log.info("ACCESS_EMAIL_SENT via=resend")
        return True
    except Exception as e:
        log.error("ACCESS_EMAIL_SEND_FAILED type=%s message=%s",type(e).__name__,str(e)[:160]); return False

async def generate_member_access_code(actor):
    if str(actor)!=ADMIN_TELEGRAM_ID or not await ensure_access_table(): return False
    code="".join(str(secrets.randbelow(10)) for _ in range(ACCESS_CODE_DIGITS))
    now=datetime.now(timezone.utc)
    try:
        import psycopg
        def save():
            with psycopg.connect(LEARNING_DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""INSERT INTO nexora_member_access
                    (id,code_hash,code_expires_at,access_expires_at,granted_at,granted_by)
                    VALUES(1,%s,%s,NULL,NULL,%s)
                    ON CONFLICT(id) DO UPDATE SET code_hash=EXCLUDED.code_hash,
                    code_expires_at=EXCLUDED.code_expires_at,access_expires_at=NULL,
                    granted_at=NULL,granted_by=EXCLUDED.granted_by,updated_at=NOW()""",
                    (_access_code_hash(code),now+ACCESS_CODE_TTL,actor))
                db.commit()
        await asyncio.to_thread(save)
        if await send_access_code_email(code):
            await audit_access("CODE_GENERATED",int(actor))
            log.info("MEMBER_ACCESS_CODE_GENERATED status=sent expires=%s",(now+ACCESS_CODE_TTL).isoformat()); return True
        return False
    except Exception as e:
        log.error("MEMBER_ACCESS_CODE_SAVE_FAILED type=%s message=%s",type(e).__name__,str(e)[:160]); return False

async def grant_member_access(actor,code):
    if str(actor)!=ADMIN_TELEGRAM_ID:
        await audit_access("UNAUTHORIZED_MBACCESS_ATTEMPT",int(actor) if str(actor).isdigit() else None); return "DENIED"
    if not code or len(code)!=ACCESS_CODE_DIGITS or not code.isdigit(): return "INVALID"
    if not configured_member_ids(): return "NO_MEMBERS"
    if not await ensure_access_table(): return "DB_ERROR"
    now=datetime.now(timezone.utc)
    try:
        import psycopg
        def verify():
            with psycopg.connect(LEARNING_DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("SELECT code_hash,code_expires_at FROM nexora_member_access WHERE id=1"); row=cur.fetchone()
                    if not row or not row[0] or not row[1] or row[1]<=now: return "EXPIRED"
                    if not secrets.compare_digest(str(row[0]),_access_code_hash(code)): return "INVALID"
                    cur.execute("UPDATE nexora_member_access SET access_expires_at=%s,granted_at=%s,granted_by=%s,updated_at=NOW() WHERE id=1",(now+ACCESS_TTL,now,actor))
                    cur.execute("INSERT INTO nexora_access_audit(event,actor) VALUES(%s,%s)",("ACCESS_GRANTED",actor))
                db.commit(); return "GRANTED"
        result=await asyncio.to_thread(verify)
        if result=="GRANTED": log.info("MEMBER_ACCESS_GRANTED member_count=%d expires=%s",len(configured_member_ids()),(now+ACCESS_TTL).isoformat())
        return result
    except Exception as e:
        log.error("MEMBER_ACCESS_VERIFY_FAILED type=%s message=%s",type(e).__name__,str(e)[:160]); return "DB_ERROR"

async def member_access_active(telegram_id):
    if telegram_id not in configured_member_ids() or not LEARNING_DB_URL: return False
    try:
        import psycopg
        now=datetime.now(timezone.utc)
        def read():
            with psycopg.connect(LEARNING_DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("SELECT access_expires_at FROM nexora_member_access WHERE id=1"); row=cur.fetchone()
                    return bool(row and row[0] and row[0]>now)
        return await asyncio.to_thread(read)
    except Exception: return False


def uae_time(ts):
    return datetime.fromtimestamp(float(ts),tz=UAE_TZ).strftime("%H:%M:%S")

STATE={"status":"starting","assets":[],"prices":{},"price_source":{},"candles":{},"analyses":{},"network":{},"read_only":True,"cycle":0,"last_cycle":None,"account_id":None,"account_group":"demo","feed_source":"authenticated_websocket","live_orders":{},"live_order_events":[],"live_order_monitor":{"connected":False,"updated_at":None,"last_query_at":None,"last_error":None,"source":"authenticated_broker_event21_22_26+event31_poll"}}
CANDLE_FETCH_SEM=asyncio.Semaphore(24)
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
# Bounded local tick history powers a real 5-second micro-candle view. It never
# fabricates missing ticks; unavailable coverage is reported explicitly.
TICK_HISTORY=defaultdict(lambda: deque(maxlen=720))
# Event-1 is the only live market stream available for observed tick activity.
# This cache never invents traded volume; it only lets the brain use the number
# of actually received broker ticks when candle volume is absent.
TICK_ACTIVITY_AUDIT_LAST={}
MULTI_TF_FRAMES=tuple(range(1,16))
MULTI_TF_MIN_COMPLETE_BARS=3
MULTI_TF_5S_SECONDS=5
MULTI_TF_5S_MIN_COMPLETE_BARS=4
TICK_RESUB_SEM=asyncio.Semaphore(6)
TICK_RESUB_TIMEOUT=1.0
LIVE_TICK_MAX_AGE=5.0
# Rolling coverage window for the account-wide rotating live feed. The broker
# only exposes a small number of simultaneous tick subscriptions, so an asset
# can be live-covered without having a fresh tick at every one-second audit.
LIVE_TICK_COVERAGE_MAX_AGE=60.0
QUOTE_SNAPSHOT_MAX_AGE=6.0
QUOTE_SNAPSHOT_REFRESH=2.0
QUOTE_SNAPSHOT_SEM=asyncio.Semaphore(48)
QUOTE_SNAPSHOT_LAST={}
AI_REVIEW_CACHE={}
AI_REVIEW_TTL=90.0
AI_REVIEW_FAIL_TTL=90.0
# A zero-volume Volume Profile is an equal-activity price-distribution proxy,
# not verified real volume. Proxy live signals remain clearly marked and are
# admitted only after strict Brain setup gates; external AI is a verifier, not
# a hard scheduler dependency when every provider is unavailable.
ALLOW_PROXY_LIVE_FALLBACK=os.getenv("ALLOW_PROXY_LIVE_FALLBACK","1").strip().lower() in {"1","true","yes","on"}
AI_DEEP_REVIEW_TOP_N=max(1,min(5,int(os.getenv("AI_DEEP_REVIEW_TOP_N","5") or 5)))
# Preserve a fully reviewed candidate for the short exact-boundary window.
# This prevents a transient provider/cache refresh from erasing a valid setup
# after it has already passed the Brain + live-price gates.
CANDIDATE_CACHE={}
CANDIDATE_CACHE_TTL=75.0
# Once a specific candidate/candle is explicitly rejected by a hard
# qualification gate, do not resurrect that same decision through the
# exact-boundary cache fallback. A later scan can still produce a new key
# when a new closed candle forms.
CANDIDATE_CACHE_HARD_REJECTED={}
AI_PROVIDER_COOLDOWN={}
AI_REVIEW_TIMEOUT=3.2
# Rotating account-wide live quote scan. It does not touch Brain timing; it only
# keeps current account prices warm for analysis/candidate selection.
ACCOUNT_LIVE_SCAN_BATCH=32
ACCOUNT_LIVE_SCAN_INTERVAL=0.10
ACCOUNT_LIVE_SCAN_CURSOR=0
# Account-wide event-1 tick subscription manager. Subscriptions are read-only;
# the worker only requests market quotes and never places/modifies trades.
ACCOUNT_TICK_SUB_SEM=asyncio.Semaphore(1)
ACCOUNT_TICK_SUB_BATCH=2
ACCOUNT_TICK_SUB_RETRY=2.0
ACCOUNT_TICK_SUB_DELAY=0.35
# The authenticated broker feed has been observed to accept four simultaneous
# event-1 pair subscriptions. Use all four slots so the final fallback pool can
# keep multiple independent candidates live at the exact signal boundary.
# This does not change Brain direction/quality gates; it only preserves live
# quote coverage for fallback candidates.
ACCOUNT_TICK_MAX_SLOTS=4
ACCOUNT_TICK_PIN_SLOTS=4
ACCOUNT_TICK_ROTATE_INTERVAL=15.0
# Temporarily back off pairs that the authenticated event-12 channel explicitly
# rejects, instead of wasting every rotation/final-boundary slot on them.
ACCOUNT_TICK_REJECT_COOLDOWN=600.0
ACCOUNT_TICK_REJECT_BACKOFF=(600.0,1800.0,7200.0,43200.0,86400.0)
ACCOUNT_TICK_FRESH_WAIT=0.5
ACCOUNT_TICK_FINAL_PROBE_LIMIT=8
ACCOUNT_TICK_SUB_TIMEOUT=1.8
ACCOUNT_TICK_SUBSCRIBED=set()
ACCOUNT_TICK_LAST_ATTEMPT={}
ACCOUNT_TICK_REJECT_COUNT=defaultdict(int)
ACCOUNT_TICK_REJECT_UNTIL={}
ACCOUNT_TICK_PINNED={}
ACCOUNT_TICK_ROTATE_CURSOR=0
ACCOUNT_TICK_LAST_ROTATION=0.0
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
                    try:                        out[f"{path}.{k}"]=int(val)
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

# Canonical account-facing asset labels. Internal OlympTrade instrument
# IDs stay in "pair" for routing/market API calls, while every user-facing
# message uses these account labels. These labels were taken from the
# account-visible asset names already established in the project screenshots;
# crypto/real variants are normalized to their human asset names as well.
ACCOUNT_ASSET_DISPLAY_MAP={
    "EURCAD_OTC":"EURCAD OTC","AUDNZD_OTC":"AUDNZD OTC","GBPJPY_OTC":"GBPJPY OTC",
    "CADCHF_OTC":"CADCHF OTC","CHFJPY_OTC":"CHFJPY OTC","GBPAUD_OTC":"GBPAUD OTC",
    "EURJPY_OTC":"EURJPY OTC","EURCHF_OTC":"EURCHF OTC","EURNZD_OTC":"EURNZD OTC",
    "GBPCHF_OTC":"GBPCHF OTC","NZDCAD_OTC":"NZDCAD OTC","NZDJPY_OTC":"NZDJPY OTC",
    "GBPNZD_OTC":"GBPNZD OTC","NZDCHF_OTC":"NZDCHF OTC","XAGUSD_OTC":"Silver OTC",
    "EURAUD_OTC":"EURAUD OTC","EURUSD_OTC":"EURUSD OTC","AUDUSD_OTC":"AUDUSD OTC",
    "USDCHF_OTC":"USDCHF OTC","XAUUSD_OTC":"Gold OTC","USDCAD_OTC":"USDCAD OTC",
    "NZDUSD_OTC":"NZDUSD OTC","AUDCAD_OTC":"AUDCAD OTC","GBPUSD_OTC":"GBPUSD OTC",
    "GBPCAD_OTC":"GBPCAD OTC","USDJPY_OTC":"USDJPY OTC","AUDCHF_OTC":"AUDCHF OTC",
    "CADJPY_OTC":"CADJPY OTC","EURGBP_OTC":"EURGBP OTC","AUDJPY_OTC":"AUDJPY OTC",
    "ASIA_X":"Asia Composite Index","EUROPE_X":"Europe Composite Index",
    "GOAL_X":"Football Champions 2026 Index","MCI_X":"Compound Index",
    "HMA_X":"Halal Market Axis","ULTRA_X":"Quickler","STABLE_X":"Stable Tick Index",
    "ARAB_X":"Arabian General Index","OASIS_X":"Oasis Index","QAHWA_X":"Qahwa Index","CRYPTO_X":"Crypto",
    "ALTCOIN":"Altcoin","Bitcoin":"Bitcoin","BTCUSD_OTC":"Bitcoin OTC",
    "ETHUSD_OTC":"Ethereum OTC","ETHUSD":"Ethereum","LTCUSD_OTC":"Litecoin OTC",
    "LTCUSD":"Litecoin","BNBUSD_OTC":"BNB OTC","PEPEUSD_OTC":"Pepe OTC",
    "SHIBUSD_OTC":"Shiba Inu OTC","DOGUSD_OTC":"Dogecoin OTC","XRPUSD_OTC":"XRP OTC",
}

def _looks_like_internal_pair(value,pair):
    s=str(value or "").strip()
    p=str(pair or "").strip()
    if not s:return True
    if s==p:return True
    # Common raw instrument forms should never leak into Telegram as a
    # substitute for the account-facing label.
    compact=s.upper().replace("/","").replace(" ","")
    raw=p.upper().replace("/","").replace(" ","")
    if compact==raw:return True
    if re.fullmatch(r"[A-Z0-9]+(?:_[A-Z0-9]+)?",s) and ("_" in s or s.upper()==s):
        return True
    return False

def display_name(x):
    # First honor a genuine human-facing field returned by the authenticated
    # account feed, including nested asset metadata used by some payloads.
    if not isinstance(x,dict): return ""
    p=pair_name(x)
    for k in ("title","display_name","displayName","label","displayLabel","asset_name","assetName","name"):
        v=x.get(k)
        if isinstance(v,str) and v.strip() and not _looks_like_internal_pair(v,p):
            return v.strip()
    nested_candidates=[]
    for k in ("asset","instrument","product","metadata","meta","details"):
        v=x.get(k)
        if isinstance(v,dict):
            for nk in ("title","display_name","displayName","label","displayLabel","asset_name","assetName","name"):
                nv=v.get(nk)
                if isinstance(nv,str) and nv.strip():
                    nested_candidates.append(nv.strip())
    for v in nested_candidates:
        if not _looks_like_internal_pair(v,p):
            return v
    # Finally use the project-wide canonical account-name converter.
    mapped=ACCOUNT_ASSET_DISPLAY_MAP.get(p)
    if mapped:
        return mapped
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

# Event-182 is the broker's discovery feed, but it can expose instruments that are
# not present in the user's currently verified account-visible asset list. The
# user's supplied account asset catalog is therefore the safety boundary for
# signal generation. An Event-182 pair must exist in that verified catalog before
# it can enter STATE["assets"], Brain analysis, or Telegram signalling.
#
# This prevents a broker-feed-only instrument such as DDI_X from generating a
# signal when the user's account UI does not show that asset.
def build_assets(client=None,raw=None):
    prof={}
    for x in raw or []:
        if not isinstance(x,dict): continue
        p=pair_name(x); v=x.get("profitability")
        if p and isinstance(v,(int,float)): prof[p.upper()]=int(v)

    catalog=build_account_asset_snapshot()
    verified_by_pair={
        str(item.get("pair") or "").upper():item
        for item in catalog
        if isinstance(item,dict) and item.get("pair")
    }

    out=[]; seen=set(); rejected=[]
    raw_pairs={
        pair_name(x) for x in (raw or [])
        if isinstance(x,dict) and pair_name(x)
    }
    unverified=sorted(
        p for p in raw_pairs
        if p.upper() not in verified_by_pair
    )
    log.info(
        "ACCOUNT_AUTHENTICATED_ASSET_UNIVERSE raw=%d unique=%d source=event_182",
        len(raw or []),len(raw_pairs)
    )
    log.info(
        "ACCOUNT_VERIFIED_ASSET_FILTER catalog=%d raw=%d matched=%d "
        "unverified=%d sample=%s source=user_verified_account_catalog",
        len(verified_by_pair),len(raw_pairs),
        len(raw_pairs)-len(unverified),len(unverified),unverified[:25]
    )

    for x in raw or []:
        if not isinstance(x,dict): continue
        p=pair_name(x)
        key=p.upper() if p else ""
        if not p or key in seen: continue
        if key not in verified_by_pair:
            rejected.append({
                "pair":p,
                "reason":"not_in_verified_account_catalog"
            })
            continue

        seen.add(key)
        verified=verified_by_pair[key]
        unavailable_reason=broker_unavailable_reason(x)
        closure_reason=known_broker_closure_reason(p)
        if unavailable_reason:
            rejected.append({
                "pair":p,
                "reason":f"broker_unavailable:{unavailable_reason}"
            })
            continue
        api_blocked=False

        # The verified account catalog controls the user-facing asset identity;
        # Event-182 supplies the current broker-side profitability/availability
        # metadata for the same exact pair.
        title=str(verified.get("display_name") or display_name(x) or p)
        try:
            profitability=int(prof.get(key,x.get("profitability",0)))
        except Exception:
            profitability=0

        quickler=(
            key=="ULTRA_X"
            or "quickler" in " ".join(
                _norm_text(x.get(k)) for k in
                ("pair","symbol","name","title","display_name","displayName",
                 "product","category","instrument_type",
                 "expiration_type","expiration_mode")
            )
        )

        out.append({
            "pair":p,
            "display_name":title,
            "title":title,
            "signal_asset_label":title,
            "profitability":profitability,
            "locked":False,
            "locked_trading":False,
            "disabled":False,
            "api_blocked":False,
            "broker_schedule_blocked":bool(closure_reason),
            "broker_closed_reason":closure_reason,
            "mode":"OTC" if "_OTC" in p.upper() else "REAL",
            "trading_mode":"FLEX_TIME",
            "market_group":verified.get("market_group"),
            "signal_eligible":not quickler and not bool(closure_reason)
        })

    log.info(
        "ACCOUNT_ASSET_FILTER source=event_182_intersection_verified_catalog "
        "raw=%d accepted_verified=%d rejected=%d",
        len(raw or []),len(out),len(rejected)
    )
    if rejected:
        log.info("ACCOUNT_ASSET_REJECTED sample=%s",rejected[:25])
    log.info(
        "ACCOUNT_OPEN_ASSET_NAMES source=verified_account_catalog∩event_182 "
        "count=%d names=%s",
        len(out),
        [{"pair":a["pair"],"account_name":a["display_name"]} for a in out]
    )
    return out

async def sync_account_assets(client, reason="periodic"):
    if not client or not client.account_id: return False
    try:
        raw=await asyncio.wait_for(client.market.get_available_assets(client.account_id),timeout=10.0)
    except Exception as e:
        log.warning("ACCOUNT_ASSET_SYNC_FAILED account_id=%s reason=%s type=%s message=%s",
                    client.account_id,reason,type(e).__name__,str(e)[:160]); return False
    if not isinstance(raw,list) or not raw:
        log.warning("ACCOUNT_ASSET_SYNC_EMPTY account_id=%s reason=%s keep_count=%d",
                    client.account_id,reason,len(STATE["assets"])); return False
    assets=build_assets(client,raw)
    if not assets:
        log.warning("ACCOUNT_ASSET_SYNC_ZERO account_id=%s reason=%s keep_count=%d",
                    client.account_id,reason,len(STATE["assets"])); return False
    old={a["pair"] for a in STATE["assets"]}; new={a["pair"] for a in assets}
    added=sorted(new-old); removed=sorted(old-new)
    STATE["assets"]=assets
    STATE["account_id"]=client.account_id
    STATE["account_group"]="demo"
    STATE["feed_source"]="authenticated_websocket:event_182_account_universe"
    for pair in removed:
        STATE["prices"].pop(pair,None); STATE["price_source"].pop(pair,None)
        STATE["candles"].pop(pair,None); STATE["analyses"].pop(pair,None)
        QUOTE_SNAPSHOT_LAST.pop(pair,None); TICK_HISTORY.pop(pair,None)
        CANDLE_FETCH_LAST.pop(pair,None); CANDLE_UNAVAILABLE_UNTIL.pop(pair,None)
        CANDLE_GOOD_ONCE.discard(pair)
    account_blocked=sum(1 for a in assets if a.get("api_blocked"))
    log.info(
        "ACCOUNT_ASSET_SYNC source=authenticated_account:event_182_raw account_id=%s reason=%s "
        "raw_account=%d universe=%d api_blocked_metadata=%d added=%d removed=%d total=%d",
        client.account_id,reason,len(raw),len(assets),account_blocked,len(added),len(removed),len(assets)
    )
    log.info("ACCOUNT_OPEN_ASSET_NAMES source=authenticated_account:event_182_raw count=%d names=%s",
             len(assets),[{"pair":a["pair"],"account_name":a["display_name"]} for a in assets])
    if added: log.info("ACCOUNT_ASSET_ADDED sample=%s",added[:25])
    if removed: log.info("ACCOUNT_ASSET_REMOVED sample=%s",removed[:25])
    return True

async def on_asset_update(message):
    """Apply partial event-183 updates without treating them as a full asset scan."""
    raw=message.get("d") if isinstance(message,dict) else None
    if not isinstance(raw,list) or not raw:
        return
    current={a["pair"]:a for a in STATE["assets"]}
    updated=0
    ignored=0
    for x in raw:
        if not isinstance(x,dict):
            continue
        p=pair_name(x)
        if not p:
            continue
        if p not in current:
            # event 183 is an update stream, not authority to widen the account universe.
            ignored+=1
            continue
        merged=current[p].copy()
        title=display_name(x) or merged.get("display_name") or ACCOUNT_ASSET_DISPLAY_MAP.get(p)
        if title:
            merged["display_name"]=title
            merged["title"]=title
            merged["signal_asset_label"]=title
        for key in (
            "profitability","locked","locked_trading","disabled",
            "active","available","tradable","is_active","is_available",
            "is_tradable","status","state"
        ):
            if key in x:
                merged[key]=x.get(key)
        blocked_reason=broker_unavailable_reason(merged)
        merged["api_blocked"]=bool(blocked_reason)
        merged["signal_eligible"]=not bool(blocked_reason)
        current[p]=merged
        if blocked_reason:
            STATE.get("analyses",{}).pop(p,None)
            ASSET_TRADEABILITY_CACHE.pop(p,None)
        updated+=1
    if updated:
        # Asset event updates are intentionally ignored as a universe change.
        # Event 183 may update metadata only; Event 182 remains the account universe.
        log.info(
            "ACCOUNT_ASSET_FEED_UPDATE_IGNORED source=authenticated_event_183 "
            "reason=authenticated_event_183_metadata_only visible=%d",
            len(STATE["assets"])
        )
    log.info(
        "ACCOUNT_ASSET_FEED_UPDATE source=authenticated_websocket:event_183 updated=%d ignored_unknown=%d visible=%d",
        updated,ignored,len(STATE["assets"])
    )


async def scan_account_live_feed():
    """Audit account-wide authenticated live quote coverage.

    Event-1 ticks are preferred when available. The broker current-candle quote
    stream is the authenticated fallback and is not confused with external data.
    """
    assets=list(STATE["assets"])
    if not CLIENT or not assets:
        return
    now=time.time()
    total=len(assets)
    event1_fresh=sum(
        1 for a in assets
        if str(STATE["price_source"].get(a["pair"],""))=="tick"
        and has_fresh_live_price(a["pair"],now,LIVE_TICK_MAX_AGE)
    )
    broker_fresh=sum(
        1 for a in assets
        if str(STATE["price_source"].get(a["pair"],""))=="authenticated_broker_live_candle"
        and has_fresh_live_price(a["pair"],now,QUOTE_SNAPSHOT_MAX_AGE)
    )
    any_fresh=sum(
        1 for a in assets
        if has_fresh_live_price(a["pair"],now,max(LIVE_TICK_MAX_AGE,QUOTE_SNAPSHOT_MAX_AGE))
    )
    recent_missing_pairs=[
        a["pair"] for a in assets
        if not has_fresh_live_price(a["pair"],now,LIVE_TICK_COVERAGE_MAX_AGE)
    ]
    recent=total-len(recent_missing_pairs)
    log.info(
        "ACCOUNT_LIVE_FEED_SCAN source=authenticated_session assets=%d "
        "event1_fresh=%d broker_live_fresh=%d any_fresh=%d recent_any=%d recent_missing=%d",
        total,event1_fresh,broker_fresh,any_fresh,recent,len(recent_missing_pairs)
    )


async def refresh_broker_live_quote(pair):
    """Read the current quote from the authenticated broker session.

    Event-1 ticks are preferred when available. This fallback reads the current
    in-progress 1-minute candle from the same authenticated session. The live
    quote is never fed into indicator candle history; it is only used as the
    current price at Brain/final-signal boundaries.
    """
    client=CLIENT
    if not client or not pair or not getattr(client.connection,"is_connected",False):
        return False
    try:
        q=await asyncio.wait_for(
            client.market.get_live_quote(pair),
            timeout=3.0
        )
        if not isinstance(q,dict) or q.get("price") is None:
            return False
        now=time.time()
        price=float(q["price"])
        broker_candle=q.get("candle") or {}
        broker_ts=broker_candle.get("time",broker_candle.get("t"))
        try:
            broker_ts=float(broker_ts)
            if broker_ts>10000000000:
                broker_ts/=1000.0
        except Exception:
            broker_ts=now
        STATE["prices"][pair]=(price,broker_ts,now)
        STATE["price_source"][pair]="authenticated_broker_live_candle"
        return True
    except Exception as e:
        log.debug(
            "BROKER_LIVE_QUOTE_REFRESH_FAILED pair=%s type=%s message=%s",
            pair,type(e).__name__,str(e)[:120]
        )
        return False

async def account_live_quote_worker():
    """Continuously rotate the authenticated account asset universe through the
    broker current-quote endpoint, so Brain sees the same account market feed even
    when Event-1 tick subscriptions are unavailable.
    """
    cursor=0
    while True:
        try:
            client=CLIENT
            assets=list(STATE.get("assets") or [])
            if not client or not assets or not getattr(client.connection,"is_connected",False):
                await asyncio.sleep(2.0)
                continue
            pairs=[str(a.get("pair") or "") for a in assets if a.get("pair")]
            if not pairs:
                await asyncio.sleep(2.0)
                continue
            batch_size=max(4,min(16,int(ACCOUNT_LIVE_SCAN_BATCH//2)))
            batch=[]
            for _ in range(min(batch_size,len(pairs))):
                p=pairs[cursor % len(pairs)]
                cursor+=1
                if p and p not in batch:
                    batch.append(p)
            results=await asyncio.gather(
                *(refresh_broker_live_quote(p) for p in batch),
                return_exceptions=True
            )
            updated=sum(1 for x in results if x is True)
            fresh=sum(
                1 for p in pairs
                if has_fresh_live_price(p,time.time(),max(LIVE_TICK_MAX_AGE,QUOTE_SNAPSHOT_MAX_AGE))
            )
            log.info(
                "ACCOUNT_LIVE_BROKER_FEED batch=%d updated=%d account_assets=%d "
                "fresh_any_source=%d source=authenticated_session_current_candle",
                len(batch),updated,len(pairs),fresh
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning(
                "ACCOUNT_LIVE_BROKER_FEED_WORKER_ERROR type=%s message=%s",
                type(e).__name__,str(e)[:160]
            )
        await asyncio.sleep(2.0)

async def account_live_feed_worker():
    # This worker is observability only. Actual event-1 tick reception is
    # continuous in on_tick(); the audit interval must not create avoidable CPU
    # and log pressure on the free Render instance.
    while True:
        try:
            await scan_account_live_feed()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("ACCOUNT_LIVE_FEED_WORKER_ERROR type=%s message=%s",type(e).__name__,str(e)[:160])
        await asyncio.sleep(2.0)


async def _unsubscribe_account_tick(pair):
    client=CLIENT
    if not client or not pair or not getattr(client.connection,"is_connected",False):
        return False
    try:
        await asyncio.wait_for(client.market.unsubscribe_ticks(pair),timeout=4.0)
        ACCOUNT_TICK_SUBSCRIBED.discard(pair)
        log.info("ACCOUNT_TICK_UNSUBSCRIBE pair=%s status=accepted",pair)
        return True
    except Exception as e:
        log.warning("ACCOUNT_TICK_UNSUBSCRIBE pair=%s status=failed type=%s message=%s",
                    pair,type(e).__name__,str(e)[:120])
        return False

async def _subscribe_account_tick(pair):
    client=CLIENT
    if not client or not pair or not getattr(client.connection,"is_connected",False):
        return False
    # Hard broker-capacity invariant. The authenticated connection currently
    # accepts two simultaneous Event-12 subscriptions. Never send Event-12
    # while both slots are occupied; this prevents capacity-induced
    # invalid_request responses and protects the signal-time feed.
    if pair not in ACCOUNT_TICK_SUBSCRIBED and len(ACCOUNT_TICK_SUBSCRIBED)>=ACCOUNT_TICK_MAX_SLOTS:
        log.info(
            "ACCOUNT_TICK_SUBSCRIBE_SKIP pair=%s reason=slot_capacity active=%d max=%d",
            pair,len(ACCOUNT_TICK_SUBSCRIBED),ACCOUNT_TICK_MAX_SLOTS
        )
        return False
    now=time.time()
    blocked_until=float(ACCOUNT_TICK_REJECT_UNTIL.get(pair) or 0.0)
    if blocked_until>now:
        log.info(
            "ACCOUNT_TICK_SUBSCRIBE_SKIP pair=%s reason=rejection_cooldown seconds=%.1f",
            pair,blocked_until-now
        )
        return False
    async with ACCOUNT_TICK_SUB_SEM:
        try:
            # Let the caller control rotation pacing; this function itself must
            # stay latency-bounded because it is also used before signal delivery.
            await asyncio.wait_for(client.market.subscribe_ticks(pair),timeout=ACCOUNT_TICK_SUB_TIMEOUT)
            ACCOUNT_TICK_SUBSCRIBED.add(pair)
            ACCOUNT_TICK_LAST_ATTEMPT[pair]=time.time()
            ACCOUNT_TICK_REJECT_COUNT[pair]=0
            ACCOUNT_TICK_REJECT_UNTIL.pop(pair,None)
            log.info("ACCOUNT_TICK_SUBSCRIBE pair=%s status=accepted",pair)
            return True
        except asyncio.CancelledError:
            raise
        except Exception as e:
            attempt=ACCOUNT_TICK_REJECT_COUNT[pair]+1
            ACCOUNT_TICK_REJECT_COUNT[pair]=attempt
            ACCOUNT_TICK_LAST_ATTEMPT[pair]=time.time()
            msg=str(e)[:180]
            lower=msg.lower()
            broker_unusable=(
                "invalid request" in lower
                or "invalid_request" in lower
                or "rejected after 2 attempts" in lower
            )
            if broker_unusable:
                backoff=ACCOUNT_TICK_REJECT_BACKOFF[min(attempt-1,len(ACCOUNT_TICK_REJECT_BACKOFF)-1)]
                ACCOUNT_TICK_REJECT_UNTIL[pair]=time.time()+backoff
                log.info("ACCOUNT_TICK_CAPABILITY_MISS pair=%s attempt=%d retry_after=%.0fs action=skip_and_fallback type=%s", pair,attempt,backoff,type(e).__name__)
            else:
                log.warning("ACCOUNT_TICK_SUBSCRIBE pair=%s status=rejected attempt=%d broker_unusable=false type=%s message=%s", pair,attempt,type(e).__name__,msg)
            return False

def _tick_pinned_pairs():
    now=time.time()
    expired=[p for p,until in ACCOUNT_TICK_PINNED.items() if float(until)<=now]
    for p in expired:
        ACCOUNT_TICK_PINNED.pop(p,None)
    return [p for p,until in ACCOUNT_TICK_PINNED.items() if float(until)>now]

async def pin_account_tick_pairs(pairs,ttl=12.0,require_fresh=False,fresh_wait=None):
    """Pin final candidate tick slots; optionally require an actual fresh event-1 tick."""
    global ACCOUNT_TICK_LAST_ROTATION
    async with ACCOUNT_TICK_CONTROL_LOCK:
        unique=[]
        seen=set()
        for p in pairs or []:
            p=str(p or "")
            if p and p not in seen:
                seen.add(p);unique.append(p)
        probe_limit=min(
            len(unique),
            int(
                ACCOUNT_TICK_FINAL_PROBE_LIMIT
                if require_fresh
                else max(ACCOUNT_TICK_PIN_SLOTS*3,ACCOUNT_TICK_PIN_SLOTS)
            )
        )
        targets=unique[:probe_limit]
        until=time.time()+float(ttl)

        # Never turn a transient event-12 rejection into zero live-tick coverage.
        # Probe beyond the first four candidates and keep no more than four
        # subscriptions active. This improves the chance that the strongest
        # fallback assets have authenticated event-1 coverage.
        previous_active=set(ACCOUNT_TICK_SUBSCRIBED)
        for p in list(previous_active):
            if p not in targets:
                await _unsubscribe_account_tick(p)

        accepted_targets=[]
        for p in targets:
            ok=(p in ACCOUNT_TICK_SUBSCRIBED) or await _subscribe_account_tick(p)
            if not ok:
                continue
            if require_fresh:
                wait_limit=float(
                    fresh_wait if fresh_wait is not None else ACCOUNT_TICK_FRESH_WAIT
                )
                deadline=time.time()+max(0.25,min(3.0,wait_limit))
                while time.time()<deadline and not has_fresh_live_price(
                    p,time.time(),LIVE_TICK_MAX_AGE
                ):
                    await asyncio.sleep(0.10)
                if not has_fresh_live_price(p,time.time(),LIVE_TICK_MAX_AGE):
                    log.info(
                        "ACCOUNT_TICK_SUBSCRIBE_NO_FRESH pair=%s wait=%.1f next_asset=TRUE",
                        p,wait_limit
                    )
                    await _unsubscribe_account_tick(p)
                    continue
            accepted_targets.append(p)
            if len(accepted_targets)>=ACCOUNT_TICK_PIN_SLOTS:
                break

        # Pin only assets that are actually subscribed.
        for p in list(ACCOUNT_TICK_PINNED):
            if p not in accepted_targets:
                ACCOUNT_TICK_PINNED.pop(p,None)
        for p in accepted_targets:
            ACCOUNT_TICK_PINNED[p]=until

        restored=[]
        for p in sorted(previous_active):
            if len(ACCOUNT_TICK_SUBSCRIBED)>=ACCOUNT_TICK_MAX_SLOTS:
                break
            if p in targets or p in ACCOUNT_TICK_SUBSCRIBED:
                continue
            if await _subscribe_account_tick(p):
                restored.append(p)

        active_targets=sum(1 for p in accepted_targets if p in ACCOUNT_TICK_SUBSCRIBED)
        ACCOUNT_TICK_LAST_ROTATION=time.time()
        log.info(
            "ACCOUNT_TICK_PIN targets=%s active=%s active_targets=%d restored=%s ttl=%.1f",
            accepted_targets,sorted(ACCOUNT_TICK_SUBSCRIBED),active_targets,restored,float(ttl)
        )
        return active_targets

async def ensure_account_tick_subscriptions():
    """Candidate-driven authenticated event-1 coverage manager.

    Final candidate preparation owns event-12 live slots. Do not rotate
    subscriptions across the full account universe; that creates avoidable
    INVALID_REQUEST churn and can consume the timing budget.
    """
    return None

async def account_tick_subscription_worker():
    # Event-12 ownership is candidate-driven now; no background subscription churn.
    while True:
        try:
            await asyncio.sleep(5.0)
        except asyncio.CancelledError:
            raise
TELEGRAM_HTTP_CLIENT=None
TELEGRAM_HTTP_CLIENT_LOCK=asyncio.Lock()
TELEGRAM_SIGNAL_TIMEOUT=2.0
TELEGRAM_DEFAULT_TIMEOUT=3.5

async def _get_telegram_http_client():
    global TELEGRAM_HTTP_CLIENT
    if TELEGRAM_HTTP_CLIENT is not None and not TELEGRAM_HTTP_CLIENT.is_closed:
        return TELEGRAM_HTTP_CLIENT
    async with TELEGRAM_HTTP_CLIENT_LOCK:
        if TELEGRAM_HTTP_CLIENT is None or TELEGRAM_HTTP_CLIENT.is_closed:
            TELEGRAM_HTTP_CLIENT=httpx.AsyncClient(
                limits=httpx.Limits(max_connections=20,max_keepalive_connections=10,keepalive_expiry=60.0),
                timeout=httpx.Timeout(connect=1.5,read=3.0,write=2.0,pool=0.5),
                headers={"Connection":"keep-alive"},
            )
    return TELEGRAM_HTTP_CLIENT

async def telegram(text, chat_id=None, reply_markup=None, timeout_seconds=TELEGRAM_DEFAULT_TIMEOUT):
    token=os.getenv("TELEGRAM_BOT_TOKEN","").strip()
    chat=str(chat_id or STATE.get("telegram_chat_id") or os.getenv("TELEGRAM_CHAT_ID","")).strip()
    if not token or not chat:
        log.warning("TELEGRAM_NOT_CONFIGURED")
        return False
    started=time.perf_counter()
    try:
        client=await _get_telegram_http_client()
        timeout=max(0.75,min(8.0,float(timeout_seconds)))
        payload={"chat_id":chat,"text":text,"parse_mode":"HTML"}
        if reply_markup is not None: payload["reply_markup"]=reply_markup
        r=await asyncio.wait_for(
            client.post(f"https://api.telegram.org/bot{token}/sendMessage",json=payload),
            timeout=timeout
        )
        latency_ms=(time.perf_counter()-started)*1000.0
        if r.status_code>=400:
            try: detail=r.json()
            except Exception: detail={"description":r.text[:200]}
            log.warning("TELEGRAM_SEND_FAILED status=%s description=%s latency_ms=%.1f",
                        r.status_code,detail.get("description"),latency_ms)
            return False
        log.info("TELEGRAM_SEND_OK chat=%s latency_ms=%.1f",chat,latency_ms)
        return True
    except asyncio.TimeoutError:
        latency_ms=(time.perf_counter()-started)*1000.0
        log.warning("TELEGRAM_SEND_FAILED type=TimeoutError timeout=%.2fs latency_ms=%.1f",
                    timeout,latency_ms)
        return False
    except Exception as e:
        latency_ms=(time.perf_counter()-started)*1000.0
        log.warning("TELEGRAM_SEND_FAILED type=%s message=%s latency_ms=%.1f",
                    type(e).__name__,str(e)[:200],latency_ms)
        return False

async def telegram_answer_callback(query_id, text_msg=""):
    token=os.getenv("TELEGRAM_BOT_TOKEN","").strip()
    if not token or not query_id: return False
    started=time.perf_counter()
    try:
        client=await _get_telegram_http_client()
        r=await asyncio.wait_for(
            client.post(
                f"https://api.telegram.org/bot{token}/answerCallbackQuery",
                json={"callback_query_id":str(query_id),"text":str(text_msg)[:180]},
            ),
            timeout=2.0
        )
        log.info("TELEGRAM_CALLBACK_ACK sent=%s latency_ms=%.1f",
                 r.status_code<400,(time.perf_counter()-started)*1000.0)
        return r.status_code<400
    except Exception as e:
        log.warning("TELEGRAM_CALLBACK_ACK_FAILED type=%s message=%s latency_ms=%.1f",
                    type(e).__name__,str(e)[:140],(time.perf_counter()-started)*1000.0)
        return False

async def send_learning_summary(summary):
    if not summary:
        return
    research=summary.get("research_progress") or {}
    lines=[
        "📚 CANDICE BRAIN • 10-SIGNAL LEARNING SUMMARY",
        "",
        f"🧠 Learning Batch → #{summary.get('batch_no')}",
        f"📊 Signals → {summary.get('signals',0)}",
        f"🟢 WIN → {summary.get('wins',0)}",
        f"🔴 LOSS → {summary.get('losses',0)}",
        f"🟡 TIE → {summary.get('ties',0)}",
        f"📈 Win Rate → {summary.get('win_rate',0):.1f}%",
        "",
        f"🌐 WEB RESEARCH → Day {research.get('day',1)}/{research.get('target_days',15)}",
        f"📖 Topic → {research.get('topic','microstructure_and_noise')}",
        f"🎯 15-Day Target → {'REACHED' if research.get('target_complete') else 'IN PROGRESS'}",
        "",
        "🧠 BRAIN STRATEGY RESULTS"
    ]
    for name,b in sorted((summary.get("strategies") or {}).items(),key=lambda kv:(-kv[1].get("n",0),kv[0])):
        lines.append(f"• {name}: {b.get('win',0)}W / {b.get('loss',0)}L / {b.get('tie',0)}T")
    lines += ["","🧬 SELF STRATEGY RESULTS"]
    for name,b in sorted((summary.get("self_strategies") or {}).items(),key=lambda kv:(-kv[1].get("n",0),kv[0])):
        lines.append(f"• {name}: {b.get('win',0)}W / {b.get('loss',0)}L / {b.get('tie',0)}T")
    lines += ["","⏱️ EXPIRY RESULTS"]
    for name,b in sorted((summary.get("expiries") or {}).items(),key=lambda kv:int(kv[0])):
        lines.append(f"• {name} MIN: {b.get('win',0)}W / {b.get('loss',0)}L / {b.get('tie',0)}T")
    lines += ["","📋 LAST 10 SIGNALS"]
    for i,d in enumerate(summary.get("details") or [],1):
        lines.append(f"{i}. {d.get('pair')} {d.get('direction')} | {d.get('strategy')} | {d.get('expiry')}m | {d.get('result')} | {d.get('confidence')}%")
    lines += ["","📐 INDICATOR RESULTS"]
    for name,b in sorted((summary.get("indicator_contexts") or {}).items(),key=lambda kv:(-kv[1].get("n",0),kv[0])):
        lines.append(f"• {name}: {b.get('win',0)}W / {b.get('loss',0)}L / {b.get('tie',0)}T")
    lines += ["","📖 WHAT BRAIN LEARNED"]
    lines.extend(f"• {x}" for x in (summary.get("lessons") or []))
    lines += ["","🔐 Learning scope → authenticated token account only",
              "⏸️ Account cooldown → 10 MIN",
              "⚠️ Cooldown pauses new signals for this authenticated account only."]
    await telegram("\n".join(lines),chat_id=ADMIN_TELEGRAM_ID)

async def telegram_background(text_msg,label):
    try:
        started=time.perf_counter()
        sent=await telegram(text_msg,timeout_seconds=TELEGRAM_DEFAULT_TIMEOUT)
        log.info("TELEGRAM_DELIVERY_BACKGROUND label=%s sent=%s latency_ms=%.1f",
                 label,sent,(time.perf_counter()-started)*1000.0)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.warning("TELEGRAM_DELIVERY_BACKGROUND_FAILED label=%s type=%s message=%s",
                    label,type(e).__name__,str(e)[:160])

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
                price_value=float(q)
                STATE["prices"][p]=(price_value,broker_ts,received_at)
                STATE["price_source"][p]="tick"
                # Keep only a bounded receipt-time tick history. This is local
                # market-data history used for 5s confirmation and is read-only.
                TICK_HISTORY[p].append((received_at,price_value))
                updated+=1
            except Exception:
                pass
    if updated:
        last_log=STATE.get("_tick_state_log_at",0.0)
        if received_at-last_log>=10.0:
            STATE["_tick_state_log_at"]=received_at
            log.info("TICK_STATE_READY updated=%d tracked=%d",updated,len(STATE["prices"]))

def _apply_tick_activity_volume(pair,candles):
    """Enrich zero-volume candles with observed broker Event-1 tick counts.
    
    This is explicitly tick-activity data, not traded/notional volume. It is
    only used when the broker candle itself has no positive volume field.
    """
    source=list(candles or [])
    if not source:
        return source

    history=list(TICK_HISTORY.get(str(pair),()))
    if not history:
        return source

    minute_counts=defaultdict(int)
    for received_at,_price in history:
        try:
            bucket=int(float(received_at)//60)*60
            minute_counts[bucket]+=1
        except (TypeError,ValueError):
            continue

    enriched=[]
    enriched_count=0
    for raw in source:
        if not isinstance(raw,dict):
            continue
        item=dict(raw)
        existing=0.0
        for key in ("real_volume","realVolume","trade_volume","tradeVolume",
                    "traded_volume","tradedVolume","base_volume","baseVolume",
                    "quote_volume","quoteVolume","volume",
                    "tick_volume","tickVolume","ticks","tick_count","tickCount","vol","v"):
            try:
                value=float(item.get(key,0) or 0)
            except (TypeError,ValueError):
                value=0.0
            if value>0.0:
                existing=value
                break

        ts=_candle_epoch(item)
        if existing<=0.0 and ts is not None:
            count=int(min(100000,max(0,minute_counts.get(int(ts//60)*60,0))))
            if count>0:
                item["tick_volume"]=count
                item["volume_source"]="TICK_ACTIVITY"
                enriched_count+=1
        enriched.append(item)

    now=time.time()
    last=float(TICK_ACTIVITY_AUDIT_LAST.get(str(pair),0.0) or 0.0)
    if now-last>=60.0:
        TICK_ACTIVITY_AUDIT_LAST[str(pair)]=now
        total=len(enriched)
        log.info(
            "TICK_ACTIVITY_VOLUME_AUDIT pair=%s candles=%d enriched=%d coverage=%.3f "
            "tick_history=%d source=event1_received_ticks",
            pair,total,enriched_count,
            (enriched_count/max(1,total)),
            len(history),
        )
    return enriched


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
    if not client or not pairs:
        return 0
    unique=[]
    seen=set()
    for p in pairs:
        p=str(p or "")
        if p and p not in seen:
            seen.add(p);unique.append(p)
    # Pin the strongest candidates so the broker's limited event-1 slots are
    # dedicated to the exact assets needed for the final signal boundary.
    return await pin_account_tick_pairs(unique,ttl=12.0)

async def ensure_candidate_quotes(pairs):
    """Refresh candidate quotes from authenticated Event-1 ticks, then broker current-candle quotes."""
    client=CLIENT
    if not client or not pairs:
        return 0
    await ensure_candidate_ticks(pairs)
    unique=list(dict.fromkeys(str(p) for p in pairs if p))
    deadline=time.time()+1.5
    fresh=0
    while time.time()<deadline:
        now=time.time()
        fresh=sum(1 for p in unique if has_fresh_live_price(p,now,LIVE_TICK_MAX_AGE))
        if fresh>=min(len(unique),ACCOUNT_TICK_MAX_SLOTS):
            break
        await asyncio.sleep(0.10)
    # Event-1 is optional. For any candidate still missing a fresh quote, use the
    # authenticated broker current-candle endpoint instead of external/synthetic data.
    missing=[p for p in unique if not has_fresh_live_price(p,time.time(),max(LIVE_TICK_MAX_AGE,QUOTE_SNAPSHOT_MAX_AGE))]
    if missing:
        results=await asyncio.gather(
            *(refresh_broker_live_quote(p) for p in missing[:ACCOUNT_TICK_FINAL_PROBE_LIMIT]),
            return_exceptions=True
        )
        fallback_updated=sum(1 for x in results if x is True)
        fresh=sum(
            1 for p in unique
            if has_fresh_live_price(p,time.time(),max(LIVE_TICK_MAX_AGE,QUOTE_SNAPSHOT_MAX_AGE))
        )
        log.info(
            "LIVE_PRICE_BROKER_FALLBACK requested=%d updated=%d fresh=%d",
            len(missing[:ACCOUNT_TICK_FINAL_PROBE_LIMIT]),fallback_updated,fresh
        )
    else:
        log.info("LIVE_PRICE_EVENT1_REFRESH requested=%d fresh=%d",len(unique),fresh)
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
        # Every authenticated account asset is analyzed for coverage. The
        # signal-eligibility flag is applied later by final_candidate(); this
        # keeps the account-wide 104/104 catalog analysis complete without allowing a
        # non-signal asset into the send gate.
        async with CANDLE_FETCH_SEM:
            last_reason="unknown"
            for attempt in range(1,CANDLE_FETCH_RETRIES+1):
                try:
                    await asyncio.sleep(0.05 if attempt==1 else CANDLE_FETCH_RETRY_DELAY)
                    cs=await asyncio.wait_for(
                        # Fetch extra closed history so the forming/current
                        # M1 bar cannot reduce the live Brain input to 59 bars.
                        # AVWAP+Volume Profile requires 60 completed M1 candles.
                        client.market.get_candles(p,size=60,count=120),
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
                        if newest is not None and age is not None and 0 <= age <= 75.0 and len(closed)>=60:
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

    # Pass-level refreshes are time-bounded by the scheduler. A timeout must not
    # cancel the final analysis phase after individual assets have already returned
    # usable closed candles. Finalize analysis from every successfully stored dataset
    # even when the surrounding wait_for() is cancelling this coroutine; this prevents
    # the observed state where CANDLE_REFRESH_RECOVERED exists but analyzed=0.
    try:
        await asyncio.gather(*(one(a) for a in due),return_exceptions=True)
    finally:
        reference=time.time()
        analyzed_count=0
        live_price_count=0
        for a in assets:
            p=a["pair"]
            price=STATE["prices"].get(p,(None,None))[0]
            if price is not None:
                live_price_count+=1
            closed=_closed_candles(STATE["candles"].get(p,[]),reference)
            if len(closed)<60:
                if a.get("signal_eligible",True):
                    STATE["analyses"].pop(p,None)
                continue
            # Feed only completed 1-minute candles into the isolated forward-learning lab.
            # The lab predicts candle t+1 using information available at candle t close;
            # it never changes this live technical analysis path.
            try:
                # M1 learning writes are deliberately moved off the trading event
                # loop. They use a dedicated single-worker executor inside the lab,
                # so slow database writes cannot delay pass timing or Telegram delivery.
                asyncio.create_task(
                    record_market_snapshot_async(p,closed,reference)
                )
            except Exception as e:
                log.debug("M1_WORLD_SNAPSHOT_FAILED pair=%s type=%s message=%s",p,type(e).__name__,str(e)[:120])
            # Count every successfully analyzed account asset, even when its
            # technical setup does not qualify as a signal. The latter remains
            # represented separately by STATE["analyses"] for candidate ranking.
            analyzed_count+=1
            analysis_candles=_apply_tick_activity_volume(p,closed)
            an=analyze_asset(a,analysis_candles,price)
            if an:
                an["live_price_source"]=STATE["price_source"].get(p,"none")
                an["account_feed_source"]="authenticated_account:event_182+broker_current_candle"
            if an and a.get("signal_eligible",True):
                an["profitability"]=a["profitability"]
                STATE["analyses"][p]=an
            elif a.get("signal_eligible",True):
                STATE["analyses"].pop(p,None)

        stale_count=sum(1 for a in assets if _candle_data_stale(a["pair"],reference))
        log.info(
            "LIVE_ANALYSIS_REFRESH assets=%d analyzed=%d live_quote=%d signal_eligible=%d fetched=%d stale=%d qualified=%d",
            len(assets),analyzed_count,live_price_count,
            sum(1 for a in assets if a.get("signal_eligible",True)),
            len(due),stale_count,len(STATE["analyses"])
        )


def has_fresh_live_price(pair,reference_ts=None,max_age=LIVE_TICK_MAX_AGE):
    rec=STATE["prices"].get(pair)
    if not rec or len(rec)<1 or rec[0] is None:return False
    received=tick_received_at(pair)
    if received is None:return False
    ref=time.time() if reference_ts is None else float(reference_ts)
    try:age=ref-received
    except Exception:return False
    return 0 <= age <= float(max_age)

def _aggregate_closed_minutes(candles,minutes,reference_ts):
    """Aggregate complete closed 1m candles into an exact N-minute timeframe."""
    if minutes==1:
        return list(candles or [])
    seconds=int(minutes)*60
    groups={}
    for c in candles or []:
        ts=_candle_epoch(c)
        if ts is None:
            continue
        bucket=(int(ts)//seconds)*seconds
        g=groups.setdefault(bucket,[])
        g.append(c)
    out=[]
    for bucket in sorted(groups):
        g=sorted(groups[bucket],key=lambda x: _candle_epoch(x) or 0)
        expected={_candle_epoch(x) for x in g}
        complete=len(g)==minutes and all(
            (bucket + i*60) in expected for i in range(minutes)
        )
        if not complete:
            continue
        end=bucket+seconds
        if end>float(reference_ts):
            continue
        out.append({
            "time":bucket,
            "open":float(g[0].get("open",g[0].get("o"))),
            "high":max(float(x.get("high",x.get("h"))) for x in g),
            "low":min(float(x.get("low",x.get("l"))) for x in g),
            "close":float(g[-1].get("close",g[-1].get("c"))),
            "volume":sum(float(x.get("volume",x.get("v",0)) or 0) for x in g),
        })
    return out

def _aggregate_closed_5s_ticks(pair,reference_ts):
    """Build completed 5s micro-candles from real received ticks only."""
    items=list(TICK_HISTORY.get(pair,()))
    if not items:
        return []
    seconds=MULTI_TF_5S_SECONDS
    groups={}
    cutoff=float(reference_ts)-90.0
    for received,price in items:
        if received>cutoff and received<reference_ts:
            bucket=int(received//seconds)*seconds
            groups.setdefault(bucket,[]).append(float(price))
    out=[]
    for bucket in sorted(groups):
        prices=groups[bucket]
        if not prices:
            continue
        if bucket+seconds>float(reference_ts):
            continue
        out.append({
            "time":bucket,
            "open":prices[0],
            "high":max(prices),
            "low":min(prices),
            "close":prices[-1],
        })
    return out

def _frame_bias(bars):
    if len(bars)<MULTI_TF_MIN_COMPLETE_BARS:
        return {"status":"INSUFFICIENT","direction":"NEUTRAL","bars":len(bars),"strength":0.0}
    recent=bars[-3:]
    moves=[float(x["close"])-float(x["open"]) for x in recent]
    last=recent[-1]
    net=float(last["close"])-float(recent[0]["open"])
    avg_range=sum(max(float(x["high"])-float(x["low"]),1e-12) for x in recent)/len(recent)
    strength=min(1.0,abs(net)/max(avg_range*1.5,1e-12))
    up=sum(1 for x in moves if x>0)
    down=sum(1 for x in moves if x<0)
    if net>0 and up>=2:
        direction="UP"
    elif net<0 and down>=2:
        direction="DOWN"
    else:
        direction="NEUTRAL"
    return {"status":"READY","direction":direction,"bars":len(bars),"strength":round(strength,3)}

def _latest_closed_candle_direction(bars):
    """Return direction from the latest completed candle only; no indicators or volume."""
    bars=list(bars or [])
    if not bars:
        return {"status":"INSUFFICIENT","direction":"NEUTRAL","bars":0}
    last=bars[-1]
    try:
        open_price=float(last.get("open",last.get("o")))
        close_price=float(last.get("close",last.get("c")))
    except (TypeError,ValueError):
        return {"status":"INSUFFICIENT","direction":"NEUTRAL","bars":len(bars)}
    if close_price>open_price:
        direction="UP"
    elif close_price<open_price:
        direction="DOWN"
    else:
        direction="NEUTRAL"
    return {
        "status":"READY",
        "direction":direction,
        "bars":len(bars),
        "open":open_price,
        "close":close_price,
        "time":_candle_epoch(last),
    }

def _latest_closed_30s_candle(pair,reference_ts=None):
    """Build the latest completed 30s candle from real received ticks only."""
    reference=time.time() if reference_ts is None else float(reference_ts)
    items=list(TICK_HISTORY.get(pair,()))
    if not items:
        return {"status":"INSUFFICIENT","direction":"NEUTRAL","bars":0}
    groups={}
    for received,price in items:
        if received>=reference:
            continue
        bucket=int(received//30)*30
        if bucket+30>reference:
            continue
        groups.setdefault(bucket,[]).append(float(price))
    candles=[]
    for bucket in sorted(groups):
        prices=groups[bucket]
        if prices:
            candles.append({
                "time":bucket,
                "open":prices[0],
                "high":max(prices),
                "low":min(prices),
                "close":prices[-1],
            })
    return _latest_closed_candle_direction(candles)

def _final_delivery_precheck(candidate,reference_ts=None):
    """Score final delivery readiness using only the locked AVWAP + Volume Profile brain.

    This is a zero-network scheduling aid. It must never use the legacy 1m/2m/5s/
    multi-timeframe confirmation stack for the production AVWAP+VP strategy.
    """
    try:
        pair=str((candidate or {}).get("pair") or "")
        expected=str((candidate or {}).get("direction") or "").upper()
        strategy=str((candidate or {}).get("strategy") or "").upper()
        if not pair or expected not in {"UP","DOWN"} or strategy!= "AVWAP_VOLUME_PROFILE":
            return -1000.0

        ind=dict((candidate or {}).get("indicators") or (candidate or {}).get("indicator_context") or {})
        px=float((candidate or {}).get("price") or 0.0)
        avwap=float(ind.get("anchored_vwap") or candidate.get("avwap") or 0.0)
        poc=float(ind.get("volume_profile_poc") or candidate.get("poc") or 0.0)
        vah=float(ind.get("volume_profile_vah") or candidate.get("vah") or 0.0)
        val=float(ind.get("volume_profile_val") or candidate.get("val") or 0.0)
        if px<=0 or avwap<=0 or poc<=0:
            return -900.0

        aligned=(px>avwap and px>poc) if expected=="UP" else (px<avwap and px<poc)
        if not aligned:
            return -800.0

        score=0.0
        score+=100.0
        if bool(ind.get("value_area_acceptance")):
            score+=35.0
        if bool(ind.get("level_reclaim")):
            score+=15.0
        if bool(ind.get("slope_persistent")):
            score+=25.0
        if bool(ind.get("poc_migration_aligned")):
            score+=20.0
        if not bool(ind.get("poc_migration_against")):
            score+=8.0
        coverage=float(ind.get("volume_coverage") or 0.0)
        score += 12.0*min(1.0,max(0.0,coverage))
        if str(ind.get("volume_quality") or "").upper()=="LOW":
            score-=25.0
        score+=min(20.0,float(candidate.get("confidence") or 0.0)*0.20)
        return round(score,3)
    except Exception:
        return -900.0


def _candle_confirmation_2m(bars,expected):
    """Candle-first 2m confirmation; indicators are intentionally not used."""
    expected=str(expected or "").upper()
    if expected not in {"UP","DOWN"}:
        return {"status":"INSUFFICIENT","direction":"NEUTRAL","reason":"invalid_direction"}
    if len(bars)<MULTI_TF_MIN_COMPLETE_BARS:
        return {"status":"INSUFFICIENT","direction":"NEUTRAL","reason":"2m_bars_insufficient"}
    frame=_frame_bias(bars)
    recent=bars[-3:]
    moves=[float(x["close"])-float(x["open"]) for x in recent]
    same=sum(1 for move in moves if (move>0 if expected=="UP" else move<0))
    last=recent[-1]
    body=float(last["close"])-float(last["open"])
    rng=max(float(last["high"])-float(last["low"]),1e-12)
    body_ratio=min(1.0,abs(body)/rng)
    volume_values=[float(x.get("volume",0) or 0) for x in bars[-12:]]
    current_volume=volume_values[-1]
    prior_volumes=volume_values[:-1]
    avg_volume=(sum(prior_volumes)/len(prior_volumes)) if prior_volumes else 0.0
    volume_available=current_volume>0 and avg_volume>0
    volume_ratio=(current_volume/avg_volume) if avg_volume>0 else 0.0
    volume_ok=volume_available and volume_ratio>=1.0
    candle_direction="UP" if body>0 else "DOWN" if body<0 else "NEUTRAL"
    trend_ok=frame.get("direction")==expected
    candle_ok=candle_direction==expected and body_ratio>=0.45
    return {
        "status":"READY",
        "direction":expected if trend_ok and candle_ok and volume_ok else "NEUTRAL",
        "trend_direction":frame.get("direction","NEUTRAL"),
        "same_direction_candles":same,
        "required_same_direction":2,
        "candle_direction":candle_direction,
        "body_ratio":round(body_ratio,3),
        "trend_ok":trend_ok,
        "candle_ok":candle_ok,
        "volume_ok":volume_ok,
        "volume":current_volume,
        "avg_volume":avg_volume,
        "volume_ratio":round(volume_ratio,3),
        "volume_available":volume_available,
    }

def build_multi_timeframe_context(pair,candles,reference_ts=None,expected=None):
    """Analyze closed 30s/1m/2m candle trend + volume before a 1m signal."""
    reference=time.time() if reference_ts is None else float(reference_ts)
    frames={}
    tick5=_aggregate_closed_5s_ticks(pair,reference)
    frames["5s"]=_frame_bias(tick5)
    frames["5s"]["bars"]=len(tick5)
    source_5s="ticks"

    tick_items=list(TICK_HISTORY.get(pair,()))
    tick30=[]
    cutoff=reference-180.0
    groups={}
    for received,price in tick_items:
        if cutoff<received<reference:
            bucket=int(received//30)*30
            groups.setdefault(bucket,[]).append(float(price))
    for bucket in sorted(groups):
        prices=groups[bucket]
        if bucket+30>reference or not prices: continue
        tick30.append({"time":bucket,"open":prices[0],"high":max(prices),"low":min(prices),"close":prices[-1],"volume":len(prices)})
    frames["30s"]=_frame_bias(tick30)
    frames["30s"]["bars"]=len(tick30)
    frames["30s"]["transaction_count"]=len(tick_items)

    closed=list(candles or [])
    for minutes in range(1,16):
        bars=_aggregate_closed_minutes(closed,minutes,reference)
        frames[f"{minutes}m"]=_frame_bias(bars)

    if expected in {"UP","DOWN"}:
        bars2=_aggregate_closed_minutes(closed,2,reference)
        frames["2m"]["candle_confirmation"]=_candle_confirmation_2m(bars2,expected)

    return {
        "frames":frames,
        "source_5s":source_5s,
        "generated_at":reference,
    }

def multi_timeframe_confirmation(context,expected):
    """Final closed-candle confirmation gate for a 1-minute signal.

    The authenticated event-1 tick stream is not guaranteed to provide enough
    30-second buckets for every asset. A missing 30s frame therefore uses a
    clearly-labelled strict substitute built from closed 1m/2m + higher frames;
    it never fabricates a 30s candle.
    """
    expected=str(expected or "").upper()
    frames=dict((context or {}).get("frames") or {})
    if expected not in {"UP","DOWN"}:
        return False,{"reason":"invalid_direction"}

    five=frames.get("5s",{})
    thirty=frames.get("30s",{})
    one=frames.get("1m",{})
    two=frames.get("2m",{}).get("candle_confirmation") or {}

    thirty_status=str(thirty.get("status") or "")
    thirty_available=thirty_status=="READY" and int(thirty.get("bars") or 0)>=2
    thirty_mode="CONFIRMED" if thirty_available else "1M_2M_FALLBACK"

    if thirty_available:
        if thirty.get("direction")!=expected:
            return False,{
                "reason":"30s_candle_trend_conflict",
                "direction":thirty.get("direction","NEUTRAL")
            }
    else:
        # Event-1 tick history is not guaranteed to contain two completed 30s
        # buckets for every authenticated account asset. 30s is therefore an
        # optional micro-confirmation, while the signal's exact 30-second lead
        # timing remains mandatory in the scheduler. Never fabricate a 30s bar.
        log.info(
            "30S_CONFIRMATION_UNAVAILABLE reason=insufficient_closed_30s_bars bars=%s mode=%s action=fallback_1m_2m",
            thirty.get("bars",0),thirty_mode
        )

    if one.get("status")!="READY":
        return False,{"reason":"1m_confirmation_insufficient","bars":one.get("bars",0)}
    if one.get("direction")!=expected:
        return False,{"reason":"1m_candle_trend_conflict","direction":one.get("direction","NEUTRAL")}

    if two.get("status")!="READY":
        return False,{"reason":"2m_confirmation_insufficient"}
    if two.get("candle_direction")!=expected:
        return False,{"reason":"2m_candle_trend_conflict",
                       "direction":two.get("candle_direction","NEUTRAL"),
                       "same_direction_candles":two.get("same_direction_candles",0)}
    if not two.get("trend_ok") or not two.get("candle_ok"):
        return False,{"reason":"2m_candle_strength_insufficient",
                       "trend_ok":two.get("trend_ok",False),
                       "candle_ok":two.get("candle_ok",False),
                       "body_ratio":two.get("body_ratio",0)}

    volume_available=bool(two.get("volume_available"))
    volume_ok=bool(two.get("volume_ok"))
    if volume_available and not volume_ok:
        return False,{"reason":"2m_volume_confirmation_failed",
                       "volume_ratio":two.get("volume_ratio",0),
                       "volume_available":True}
    volume_mode="CONFIRMED" if volume_available else "UNAVAILABLE_STRICT_SUBSTITUTE"

    short=[frames.get(f"{m}m",{}) for m in (1,2,3,4)]
    short=[x for x in short if x.get("status")=="READY"]
    if len(short)<3:
        return False,{"reason":"short_frames_insufficient","ready":len(short)}
    short_align=sum(1 for x in short if x.get("direction")==expected)
    short_opp=sum(1 for x in short if x.get("direction") not in {expected,"NEUTRAL"})

    # Normal path: at least 3 of 4 short frames agree. When the optional 30s
    # micro-frame is unavailable, require stronger 1m/2m agreement instead.
    required_align=4 if not thirty_available else 3
    minimum_body=0.65 if not thirty_available else 0.45
    if short_align<required_align or short_opp>1:
        return False,{
            "reason":"short_frame_conflict",
            "align":short_align,"opp":short_opp,
            "required_align":required_align,
            "30s_mode":thirty_mode
        }
    if float(two.get("body_ratio") or 0.0)<minimum_body:
        return False,{
            "reason":"fallback_1m_2m_body_insufficient" if not thirty_available else "2m_candle_strength_insufficient",
            "body_ratio":two.get("body_ratio",0),
            "required_body_ratio":minimum_body,
            "30s_mode":thirty_mode
        }

    all_ready=[]
    for m in range(5,16):
        x=frames.get(f"{m}m",{})
        if x.get("status")=="READY":
            all_ready.append(x)
    directional=[x for x in all_ready if x.get("direction") in {"UP","DOWN"}]
    align=sum(1 for x in directional if x.get("direction")==expected)
    opp=sum(1 for x in directional if x.get("direction")!=expected)
    agreement=align/max(1,len(directional))
    required_higher=0.70 if (not thirty_available or not volume_available) else 0.60
    if directional and (agreement<required_higher or opp>2):
        return False,{
            "reason":"higher_frame_conflict",
            "align":align,"opp":opp,"directional":len(directional),
            "agreement":round(agreement,3),
            "required_agreement":required_higher,
            "30s_mode":thirty_mode
        }

    five_status=five.get("status")
    five_direction=five.get("direction","NEUTRAL")
    five_warning="5s_insufficient" if five_status!="READY" else (
        "5s_opposite" if five_direction not in {expected,"NEUTRAL"} else "none"
    )

    return True,{
        "reason":"multi_timeframe_confirmed",
        "30s":thirty.get("direction","NEUTRAL"),
        "30s_bars":thirty.get("bars",0),
        "30s_mode":thirty_mode,
        "1m":one.get("direction","NEUTRAL"),
        "2m":two.get("candle_direction","NEUTRAL"),
        "2m_same_direction_candles":two.get("same_direction_candles",0),
        "2m_candle_direction":two.get("candle_direction","NEUTRAL"),
        "2m_body_ratio":two.get("body_ratio",0),
        "2m_volume_ratio":two.get("volume_ratio",0),
        "2m_volume_available":volume_available,
        "volume_mode":volume_mode,
        "5s":five_direction,
        "5s_warning":five_warning,
        "short_align":short_align,
        "short_opp":short_opp,
        "short_required_align":required_align,
        "higher_align":align,
        "higher_opp":opp,
        "higher_directional":len(directional),
        "higher_required_agreement":required_higher,
        "agreement":round(agreement,3),
    }

def live_price_age(pair,reference_ts=None):
    received=tick_received_at(pair)
    if received is None:return None
    ref=time.time() if reference_ts is None else float(reference_ts)
    try:return max(0.0,ref-received)
    except Exception:return None

async def final_candidate(use_cached_only=False,require_live_price=False,deep_analysis=False,seed_candidates=None,return_ranked=False):
    BRAIN.prune_expired_cooldowns()

    # Hard account boundary: Brain may select/analyze an asset only after the
    # fixed user-supplied 104-asset catalog is loaded for the authenticated demo session.
    if not CLIENT or STATE.get("account_group") != "demo" or not STATE.get("account_id"):
        log.info("BRAIN_ACCOUNT_GATE blocked=account_not_ready")
        return None
    if not str(STATE.get("feed_source") or "").startswith("authenticated_websocket:event_182_account_universe"):
        log.info("BRAIN_ACCOUNT_GATE blocked=non_account_feed source=%s",
                 STATE.get("feed_source"))
        return None
    eligible=[
        a for a in BRAIN.filter_candidates(STATE["assets"])
        if a.get("signal_eligible",True)
    ]
    analyzed=[STATE["analyses"][a["pair"]].copy() for a in eligible if a["pair"] in STATE["analyses"]]

    # Volume-first candidate selection:
    # 1) scan the full authenticated account asset universe;
    # 2) keep assets with broker-reported real volume, broker tick volume, or
    #    observed Event-1 tick-activity proxy;
    # 3) keep only the top 10 by volume-data quality/coverage;
    # 4) run the Candice Brain + AI verification only inside that volume pool.
    #
    # This is a candidate-priority layer only. It never overrides Brain direction,
    # closed-candle structure, live quote checks, duplicate guards, or the final
    # delivery gates. When the broker provides no usable volume field for any asset,
    # the existing candidate path is preserved so volume-data scarcity cannot stop
    # the 3-minute scheduler.
    volume_priority_top_n=max(1,min(10,int(os.getenv("VOLUME_PRIORITY_TOP_N","10") or 10)))

    def _volume_priority_key(candidate):
        ind=dict(candidate.get("indicators") or candidate.get("indicator_context") or {})
        data_class=str(ind.get("volume_data_class") or "").upper().strip()
        source_rank={
            "REAL_VOLUME":4,
            "TICK_VOLUME":3,
            "TICK_ACTIVITY_PROXY":2,
            "NO_BROKER_VOLUME":0,
        }.get(data_class,0)
        try:
            volume_coverage=float(ind.get("volume_coverage") or 0.0)
        except (TypeError,ValueError):
            volume_coverage=0.0
        try:
            source_coverage=max(
                float(ind.get("real_volume_coverage") or 0.0),
                float(ind.get("tick_volume_coverage") or 0.0),
                float(ind.get("tick_activity_coverage") or 0.0),
            )
        except (TypeError,ValueError):
            source_coverage=0.0
        try:
            volume_bars=int(ind.get("volume_bars") or 0)
        except (TypeError,ValueError):
            volume_bars=0
        try:
            fresh_activity_bars=int(ind.get("tick_activity_bars") or 0)
        except (TypeError,ValueError):
            fresh_activity_bars=0
        return (
            source_rank,
            round(source_coverage,6),
            round(volume_coverage,6),
            volume_bars,
            fresh_activity_bars,
            int(candidate.get("confidence") or 0),
        )

    volume_enabled=[x for x in analyzed if _volume_priority_key(x)[0]>0 and _volume_priority_key(x)[1]>0.0]
    volume_enabled.sort(key=_volume_priority_key,reverse=True)
    volume_selected=volume_enabled[:volume_priority_top_n]
    if volume_selected:
        selected_pairs={str(x.get("pair")) for x in volume_selected}
        volume_rank_by_pair={
            str(x.get("pair")):idx for idx,x in enumerate(volume_selected,1)
        }
        selected_modes={}
        for idx,x in enumerate(volume_selected,1):
            pair=str(x.get("pair"))
            ind=dict(x.get("indicators") or x.get("indicator_context") or {})
            x["volume_priority_rank"]=idx
            x["volume_priority_selected"]=True
            x["volume_priority_source"]=str(ind.get("volume_data_class") or ind.get("volume_mode") or "UNKNOWN")
            selected_modes[x["volume_priority_source"]]=selected_modes.get(x["volume_priority_source"],0)+1
        analyzed_for_brain=volume_selected
        log.info(
            "VOLUME_PRIORITY_SCAN account_assets=%d analyzed=%d volume_enabled=%d "
            "selected=%d top_n=%d modes=%s pairs=%s",
            len(STATE.get("assets") or []),len(analyzed),len(volume_enabled),
            len(volume_selected),volume_priority_top_n,selected_modes,
            ",".join(str(x.get("pair")) for x in volume_selected),
        )
    else:
        analyzed_for_brain=analyzed
        log.warning(
            "VOLUME_PRIORITY_FALLBACK account_assets=%d analyzed=%d reason=no_usable_volume_data "
            "action=preserve_existing_brain_pool",
            len(STATE.get("assets") or []),len(analyzed)
        )

    adapted=[BRAIN.adaptive_candidate(x) for x in analyzed_for_brain]
    # Final-pass resilience: earlier passes may have found a strong candidate that
    # disappears from STATE["analyses"] on the last refresh because its technical
    # score moved below the threshold for that instant. Never send that older decision
    # directly. Instead, optionally re-run the current closed-candle Brain analysis for
    # those earlier seed pairs and merge only freshly requalified candidates into the
    # normal ranking/review path. This preserves the Brain strategy and prevents a
    # transient final-pass refresh from creating an artificial signal gap.
    recovery_adapted=[]
    recovery_seen=set()
    if seed_candidates:
        seed_list=[x for x in (seed_candidates or []) if isinstance(x,dict) and x.get("pair")]
        log.info(
            "FINAL_RECOVERY_START seeds=%d deep=%s require_live=%s",
            len(seed_list),deep_analysis,require_live_price
        )
        for seed in seed_list[:100]:
            pair=str(seed.get("pair"))
            if pair in recovery_seen:
                continue
            recovery_seen.add(pair)
            if any(str(x.get("pair"))==pair for x in adapted):
                continue
            asset=next((a for a in eligible if str(a.get("pair"))==pair),None)
            if not asset:
                continue
            try:
                current_candles=_closed_candles(
                    STATE["candles"].get(pair,[]),time.time()
                )
                if len(current_candles)<60:
                    log.info(
                        "FINAL_RECOVERY_REJECTED pair=%s reason=closed_candles=%s",
                        pair,len(current_candles)
                    )
                    continue
                live_price=STATE["prices"].get(
                    pair,(None,None)
                )[0]
                analysis_candles=_apply_tick_activity_volume(pair,current_candles)
                refreshed=analyze_asset(asset,analysis_candles,live_price)
                if not refreshed:
                    log.info(
                        "FINAL_RECOVERY_REJECTED pair=%s reason=brain_no_setup",
                        pair
                    )
                    continue
                refreshed["profitability"]=asset.get("profitability",0)
                adapted_recovered=BRAIN.adaptive_candidate(refreshed)
                recovery_adapted.append(adapted_recovered)
                log.info(
                    "FINAL_RECOVERY_BRAIN_RECHECK pair=%s confidence=%s strategy=%s direction=%s",
                    pair,adapted_recovered.get("confidence"),
                    adapted_recovered.get("strategy"),
                    adapted_recovered.get("direction")
                )
            except Exception as e:
                log.warning(
                    "FINAL_RECOVERY_REJECTED pair=%s type=%s message=%s",
                    pair,type(e).__name__,str(e)[:140]
                )
        if recovery_adapted:
            log.info(
                "FINAL_RECOVERY_MERGED seeds=%d requalified=%d",
                len(seed_list),len(recovery_adapted)
            )

    # Expand every independently qualified Brain technique instead of keeping
    # only the single best technique returned for an asset. A late confirmation
    # failure on BREAKOUT/TREND_FOLLOWING/etc. must fall through to another
    # already-qualified technique for the same asset before the cycle is skipped.
    candidate_inputs=[]
    variant_count=0
    for base in adapted+recovery_adapted:
        variants=base.get("strategy_candidates") or []
        if not variants:
            candidate_inputs.append(base)
            continue
        for variant in variants:
            if not isinstance(variant,dict):
                continue
            item=base.copy()
            item["strategy"]=str(variant.get("strategy") or base.get("strategy") or "").upper()
            item["direction"]=str(variant.get("direction") or base.get("direction") or "").upper()
            raw_variant_score=int(variant.get("score") or base.get("confidence") or 0)
            # Do not let strategy expansion discard the Brain's learned outcome
            # calibration. Each variant is adapted independently from its own raw
            # technical score before final ranking.
            item["confidence"]=raw_variant_score
            item["market_quality"]=float(raw_variant_score)
            item["strategy_variant"]=True
            item["strategy_variant_count"]=len(variants)
            item["strategy_variant_score"]=raw_variant_score
            item["strategy_margin"]=0.0
            item["self_strategy_version"]=str(
                variant.get("self_strategy_version") or base.get("self_strategy_version") or ""
            )
            item=BRAIN.adaptive_candidate(item)
            candidate_inputs.append(item)
            variant_count+=1
    log.info(
        "BRAIN_TECHNIQUE_EXPANSION base_assets=%d variants=%d candidate_inputs=%d",
        len(adapted)+len(recovery_adapted),variant_count,len(candidate_inputs)
    )
    raw=rank_signal_candidates(candidate_inputs)
    if not raw:
        if candidate_inputs:
            top_debug=max(candidate_inputs,key=lambda x:(int(x.get("confidence") or 0),float(x.get("market_quality") or 0)))
            log.info("CANDIDATE_GATE_REJECTED analyzed=%d top_pair=%s top_confidence=%s top_quality=%s strategy=%s",
                     len(candidate_inputs),top_debug.get("pair"),top_debug.get("confidence"),
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
    # Pass-5 live-price handling is deliberately separated from technical
    # ranking. A hard pre-filter on the broker's two rotating tick slots can
    # starve a strong candidate simply because another asset happened to occupy
    # a fresh slot at that instant. Rank the closed-candle candidates first,
    # review them, then refresh live quotes one candidate at a time in ranked
    # order until one has a genuinely fresh authenticated tick.
    # Keep the final pass broad enough to preserve the 150/day operational
    # target without relaxing the Brain quality threshold. The final delivery
    # gate will still reject weak candidates and fall through to the next asset.
    # Preliminary passes seed the candidate pool. Deep verification happens before the
    # final 30-second signal boundary so delivery itself is reserved for the exact
    # timed Telegram send.
    # Deep/final passes must not arbitrarily discard qualified account assets.
    # refresh_candles() already evaluates the full authenticated account asset set.
    # When deep_analysis is enabled, review every currently Brain-qualified setup
    # so one failed candidate can fall through to the next valid asset.
    # Preliminary passes remain intentionally narrow to protect the 3-minute timing window.
    review_scope_n=min(
        len(raw),
        max(
            AI_DEEP_REVIEW_TOP_N,
            volume_priority_top_n if volume_selected else 0
        )
    )
    top=raw if not deep_analysis else raw[:review_scope_n]
    if deep_analysis:
        log.info(
            "AI_DEEP_REVIEW_SCOPE candidates=%d configured_top_n=%d volume_top_n=%d "
            "review_scope=%d volume_priority=%s",
            len(raw),AI_DEEP_REVIEW_TOP_N,volume_priority_top_n,
            review_scope_n,bool(volume_selected)
        )
    if require_live_price:
        log.info(            "LIVE_PRICE_SELECTION_MODE source=authenticated_event1 candidates=%d deep=%s",
            len(top),deep_analysis)
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

        # Multi-timeframe confirmation is a FINAL delivery gate, not a
        # candidate-discovery gate. Brain + AI must be allowed to rank multiple
        # assets first. At the exact signal boundary we check 30s + 2m on the
        # top candidate; if it fails, send_cycle_signal() immediately checks the
        # next ranked asset. This prevents one asset's late confirmation failure
        # from suppressing the whole cycle.
        cache_key=(x["pair"],str(x.get("entry_candle_ts")),x.get("direction"))
        x["multi_timeframe"]={}
        x["multi_timeframe_confirmed"]=True
        x["multi_timeframe_diagnostic"]={"reason":"LOCKED_AVWAP_VOLUME_PROFILE_NO_LEGACY_GATE"}
        # The local Candice Brain remains the primary technical engine, but a
        # qualified candidate must now receive a real external AI verification
        # pass when time permits. The verifier gets the same closed-candle
        # technical context that produced the candidate and can veto a direct
        # directional contradiction. Provider failure is fail-open to the local
        # Brain, so outages never stop signal generation or the 30s timing path.
        technical_context={
            "local_candidate_direction":str(x.get("direction") or "").upper(),
            "local_candidate_strategy":str(x.get("strategy") or ""),
            "local_candidate_confidence":int(x.get("confidence") or 0),
            "trend_15m":str(x.get("trend_15m") or ""),
            "structure_1m":str(x.get("structure_1m") or ""),
            "pattern":str(x.get("pattern") or ""),
            "direction_agreement":float(x.get("direction_agreement") or 0.0),
            "strategy_margin":float(x.get("strategy_margin") or 0.0),
            "multi_timeframe":dict(x.get("multi_timeframe") or {}),
            "indicators":dict(x.get("indicators") or {}),
            "evidence":dict(x.get("evidence") or {}),
        }
        snap=snapshot_from_asset(
            asset,closed,price,now,
            technical_context=technical_context
        )
        cached=AI_REVIEW_CACHE.get(cache_key)
        ttl=AI_REVIEW_TTL if cached and cached[1] else AI_REVIEW_FAIL_TTL
        if not deep_analysis:
            d=None
        elif cached and time.time()-cached[0] < ttl:
            d=cached[1]
        elif use_cached_only:
            d=None
        else:
            d=None
            try:
                d=await asyncio.wait_for(analyze_with_fallback(snap),timeout=AI_REVIEW_TIMEOUT)
                AI_REVIEW_CACHE[cache_key]=(time.time(),d)
            except Exception as e:
                log.warning("AI_PRE_SIGNAL_VERIFY_FAILED pair=%s local_direction=%s type=%s message=%s",
                            x["pair"],x.get("direction"),type(e).__name__,str(e)[:120])
                AI_REVIEW_CACHE[cache_key]=(time.time(),None)

        local_confidence=int(x.get("confidence") or 0)
        local=x.copy()
        local.update({
            "confidence":local_confidence,
            "reason":x.get("reason") or "Candice local Brain verified closed-candle evidence",
            "ai_provider":"CANDICE_LOCAL_BRAIN_PRIMARY"
        })

        # Final lightweight setup-strength guard. This is intentionally scoped
        # to BREAKOUT candidates only and runs before the external verifier,
        # so it cannot consume additional cycle time. It vetoes only weak
        # breakouts; other strategies and the 30-second deadline are untouched.
        if (
            str(x.get("strategy") or "").upper()=="BREAKOUT"
            and (
                not bool((x.get("evidence") or {}).get("breakout_confirmed"))
                or float(x.get("breakout_distance_up") if str(x.get("direction") or "").upper()=="UP"
                         else x.get("breakout_distance_down") or 0.0) < 0.15
                or float(x.get("body_ratio") or 0.0) < 0.55
                or float(x.get("momentum_norm") or 0.0) < 0.30
                or float(x.get("efficiency") or 0.0) < 0.35
            )
        ):
            log.info(
                "BREAKOUT_STRENGTH_GATE_REJECTED pair=%s direction=%s distance=%.3f body=%.2f momentum=%.2f efficiency=%.2f",
                x.get("pair"),x.get("direction"),
                float(x.get("breakout_distance_up") if str(x.get("direction") or "").upper()=="UP"
                      else x.get("breakout_distance_down") or 0.0),
                float(x.get("body_ratio") or 0.0),float(x.get("momentum_norm") or 0.0),
                float(x.get("efficiency") or 0.0)
            )
            CANDIDATE_CACHE_HARD_REJECTED[cache_key]=time.time()
            return None

        strategy_name=str(x.get("strategy") or "").upper()
        trend_name=str(x.get("trend_15m") or "").upper()
        evidence=x.get("evidence") or {}

        # Strict proxy-volume fallback for brokers that expose zero candle volume.
        # Preliminary scans may carry the candidate, but a proxy-volume setup is
        # live-eligible only after deep external-AI direction verification. The AI
        # never changes the Candice Brain direction; disagreement vetoes the setup.
        if strategy_name=="AVWAP_VOLUME_PROFILE":
            ind_ctx=dict(x.get("indicators") or x.get("indicator_context") or {})
            proxy_volume=(ind_ctx.get("real_volume_verified") is not True)
            local["volume_proxy_mode"]=bool(proxy_volume)
            local["volume_proxy_ai_verified"]=False
            local["volume_proxy_local_fallback"]=False
            if proxy_volume and deep_analysis:
                if isinstance(d,dict) and str(d.get("direction") or "").upper() in {"UP","DOWN"}:
                    ai_direction=str(d.get("direction") or "").upper()
                    try:
                        ai_confidence=max(0,min(100,int(float(d.get("confidence") or 0))))
                    except (TypeError,ValueError):
                        ai_confidence=0
                    if ai_direction!=str(x.get("direction") or "").upper() or ai_confidence<80:
                        log.info(
                            "AI_PROXY_VOLUME_VETO pair=%s local_direction=%s "
                            "reason=strict_ai_disagreement_or_low_confidence provider=%s ai_direction=%s ai_confidence=%s",
                            x.get("pair"),x.get("direction"),d.get("provider"),
                            ai_direction,ai_confidence
                        )
                        CANDIDATE_CACHE_HARD_REJECTED[cache_key]=time.time()
                        return None
                    local["volume_proxy_ai_verified"]=True
                    local["volume_proxy_ai_confidence"]=ai_confidence
                    log.info(
                        "AI_PROXY_VOLUME_VERIFIED pair=%s direction=%s ai_confidence=%s provider=%s",
                        x.get("pair"),x.get("direction"),ai_confidence,d.get("provider")
                    )
                else:
                    # The external verifier is a confirmation layer, not a scheduler
                    # dependency. Provider outage/quota exhaustion must not erase every
                    # otherwise-qualified local setup from the 3-minute cycle. Fall back
                    # only when the locked Brain setup remains exceptionally strong and
                    # all exact AVWAP/VP gates have already passed.
                    ind_fallback=dict(x.get("indicators") or x.get("indicator_context") or {})
                    fallback_value_position=str(ind_fallback.get("value_position") or "").upper()
                    fallback_direction=str(x.get("direction") or "").upper()
                    fallback_setup_ok=(
                        local_confidence>=90
                        and bool(ind_fallback.get("exact_live_setup"))
                        and ind_fallback.get("slope_persistent") is True
                        and ind_fallback.get("level_reclaim") is False
                        and ((fallback_direction=="UP" and fallback_value_position=="ABOVE_VALUE")
                             or (fallback_direction=="DOWN" and fallback_value_position=="BELOW_VALUE"))
                    )
                    if fallback_setup_ok:
                        local["volume_proxy_local_fallback"]=True
                        local["volume_proxy_local_fallback_confidence"]=local_confidence
                        log.info(
                            "AI_PROXY_VOLUME_LOCAL_FALLBACK pair=%s direction=%s local_confidence=%s "
                            "reason=external_ai_unavailable_strict_local_setup",
                            x.get("pair"),x.get("direction"),local_confidence
                        )
                    else:
                        log.info(
                            "AI_PROXY_VOLUME_VETO pair=%s local_direction=%s "
                            "reason=external_ai_unavailable_proxy_volume_not_live_eligible confidence=%s",
                            x.get("pair"),x.get("direction"),local_confidence
                        )
                        CANDIDATE_CACHE_HARD_REJECTED[cache_key]=time.time()
                        return None

        # Weak momentum inside a SIDEWAYS 15m regime produced two of the
        # consecutive losses. Keep this as a local zero-latency qualification
        # gate; it does not alter cycle scheduling or add a network request.
        if (
            strategy_name=="MOMENTUM"
            and trend_name=="SIDEWAYS"
            and (
                float(x.get("momentum_norm") or 0.0) < 0.50
                or float(x.get("body_ratio") or 0.0) < 0.60
                or float(x.get("efficiency") or 0.0) < 0.45
                or int(evidence.get("recent_aligned_candles") or 0) < 2
                or str(x.get("pattern") or "").upper() not in {
                    "BULLISH_CANDLE","BEARISH_CANDLE",
                    "BULLISH_REJECTION","BEARISH_REJECTION"
                }
            )
        ):
            log.info(
                "SIDEWAYS_MOMENTUM_GATE_REJECTED pair=%s direction=%s momentum=%.2f body=%.2f efficiency=%.2f aligned=%s pattern=%s",
                x.get("pair"),x.get("direction"),float(x.get("momentum_norm") or 0.0),
                float(x.get("body_ratio") or 0.0),float(x.get("efficiency") or 0.0),
                evidence.get("recent_aligned_candles"),x.get("pattern")
            )
            CANDIDATE_CACHE_HARD_REJECTED[cache_key]=time.time()
            return None

        if local_confidence>=90:
            if isinstance(d,dict) and str(d.get("direction") or "").upper() in {"UP","DOWN"}:
                ai_direction=str(d.get("direction") or "").upper()
                try:
                    ai_confidence=max(0,min(100,int(float(d.get("confidence") or 0))))
                except (TypeError,ValueError):
                    ai_confidence=0

                # A confident external contradiction is a hard veto for this
                # exact candidate. We do not swap the direction; the next ranked
                # candidate may still be selected. This preserves the Brain's
                # strategy and timing while closing the old "AI bypass" hole.
                if ai_confidence>=75 and ai_direction!=str(x.get("direction") or "").upper():
                    log.warning(
                        "AI_PRE_SIGNAL_CONFLICT pair=%s local_direction=%s ai_direction=%s "
                        "local_confidence=%s ai_confidence=%s provider=%s strategy=%s",
                        x["pair"],x.get("direction"),ai_direction,local_confidence,
                        ai_confidence,d.get("provider"),x.get("strategy")
                    )
                    return None

                log.info(
                    "AI_PRE_SIGNAL_VERIFY pair=%s local_direction=%s ai_direction=%s "
                    "local_confidence=%s ai_confidence=%s provider=%s verdict=AGREE_OR_UNCERTAIN",
                    x["pair"],x.get("direction"),ai_direction,local_confidence,
                    ai_confidence,d.get("provider")
                )
                # Never let the external model inflate or rewrite the Brain's
                # technical confidence; it is only a verifier here.
                local["ai_provider"]=d.get("provider")
            else:
                log.info(
                    "AI_PRE_SIGNAL_VERIFY pair=%s local_direction=%s local_confidence=%s verdict=LOCAL_FALLBACK",
                    x["pair"],x.get("direction"),local_confidence
                )
            CANDIDATE_CACHE[cache_key]=(time.time(),local.copy())
            return local

        # Sub-90 local candidates still use the original external-AI qualification
        # path. This keeps the existing signal threshold and fallback behavior.
        if isinstance(d,dict) and int(d.get("confidence",0))>=90:
            y=x.copy()
            y.update({
                "confidence":int(d["confidence"]),
                "reason":d.get("reason") or x["reason"],
                "ai_provider":d.get("provider")
            })
            CANDIDATE_CACHE[cache_key]=(time.time(),y.copy())
            return y

        # Provider unavailable / low-confidence AI: preserve the local result only
        # when it already meets the Brain threshold. No signal timing is extended.
        if not use_cached_only and local_confidence>=90:
            CANDIDATE_CACHE[cache_key]=(time.time(),local.copy())
            return local
        return None

    results=await asyncio.gather(*(review_one(x) for x in top),return_exceptions=True)
    for x,r in zip(top,results):
        if isinstance(r,Exception):
            log.warning("AI_REVIEW_TASK_FAILED pair=%s type=%s message=%s",x["pair"],type(r).__name__,str(r)[:120])
        elif r:
            reviewed.append(r)
    ranked=rank_signal_candidates(reviewed)
    if return_ranked:
        # Pass 5 may provide several Brain+AI-qualified candidates. The delivery
        # stage will check the final candle/quote on each candidate in rank order.
        # This is the key fallback that prevents one asset from suppressing a cycle.
        return ranked[:100]
    if ranked and require_live_price:
        # The broker only keeps a small number of event-1 tick slots active.
        # Refresh candidates sequentially after Brain/AI qualification so a
        # fresh quote is obtained for the actual ranked choice instead of
        # arbitrarily filtering on whichever slot happened to be hot.
        attempted=0
        for candidate in ranked[:min(8,len(ranked))]:
            attempted+=1
            p=candidate["pair"]
            now_live=time.time()
            if not has_fresh_live_price(p,now_live,LIVE_TICK_MAX_AGE):
                try:
                    await ensure_candidate_quotes([p])
                except Exception as e:
                    log.warning(
                        "FINAL_LIVE_QUOTE_REFRESH_FAILED pair=%s type=%s message=%s",
                        p,type(e).__name__,str(e)[:120]
                    )
            if has_fresh_live_price(p,time.time(),LIVE_TICK_MAX_AGE):
                log.info(
                    "FINAL_LIVE_CANDIDATE_READY pair=%s confidence=%s attempted=%d",
                    p,candidate.get("confidence"),attempted
                )
                return candidate
        log.info(
            "FINAL_LIVE_CANDIDATE_NONE ranked=%d attempted=%d",
            len(ranked),attempted
        )
    elif ranked:
        return ranked[0]

    # Exact-boundary resilience: if this cycle already completed a valid Brain
    # + AI/local review for the same candle, reuse that completed decision for a
    # few seconds instead of re-running the provider at the deadline. The live
    # quote is still required, so this cannot send on stale market data.
    cached_candidates=[]
    now_cache=time.time()
    for x in top:
        key=(x["pair"],str(x.get("entry_candle_ts")),x.get("direction"))
        rejected_at=CANDIDATE_CACHE_HARD_REJECTED.get(key)
        if rejected_at is not None:
            if now_cache-rejected_at <= CANDIDATE_CACHE_TTL:
                continue
            CANDIDATE_CACHE_HARD_REJECTED.pop(key,None)
        cached=CANDIDATE_CACHE.get(key)
        if not cached:
            continue
        cached_at,candidate=cached
        if now_cache-cached_at > CANDIDATE_CACHE_TTL:
            CANDIDATE_CACHE.pop(key,None)
            continue
        if int(candidate.get("confidence") or 0) < 90:
            continue
        if require_live_price and not has_fresh_live_price(candidate["pair"],now_cache,LIVE_TICK_MAX_AGE):
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
    watch_id=f"{s.cycle_id}:{s.pair}:{s.entry_ts}"

    # The alert is sent 30s before the entry boundary. The old code incorrectly
    # used that pre-entry reference as the actual entry price, which could
    # reverse the WIN/LOSS classification and then poison persistent learning.
    await asyncio.sleep(max(0,s.entry_ts-time.time()))

    entry_price=None
    entry_source=""
    entry_deadline=time.time()+8.0
    while time.time()<entry_deadline and entry_price is None:
        rec=STATE["prices"].get(s.pair)
        if rec and rec[0] is not None and has_fresh_live_price(s.pair,time.time(),LIVE_TICK_MAX_AGE):
            try:
                entry_price=float(rec[0])
                entry_source=STATE["price_source"].get(s.pair,"tick")
                break
            except (TypeError,ValueError):
                pass
        try:
            await ensure_candidate_quotes([s.pair])
        except Exception as e:
            log.warning("ACTUAL_ENTRY_REFRESH_FAILED pair=%s type=%s message=%s",
                        s.pair,type(e).__name__,str(e)[:120])
        await asyncio.sleep(0.10)

    if entry_price is None:
        # Never feed a 30s-old reference price back into Brain learning.
        log.warning("ACTUAL_ENTRY_UNAVAILABLE pair=%s target=%s reason=no_fresh_authenticated_price durable_watch=%s",
                    s.pair,s.entry_ts,watch_id)
        await mark_result_watch_error(watch_id,"actual_entry_unavailable")
        return

    s.entry_price=entry_price
    if LEARNING_DB_URL:
        try:
            import psycopg
            updated_payload=_result_watch_payload(s)
            updated_payload["actual_entry_source"]=entry_source
            def _persist_entry():
                payload=json.dumps(updated_payload,separators=(",",":"),ensure_ascii=False,default=str)
                with psycopg.connect(LEARNING_DB_URL,connect_timeout=8) as db:
                    with db.cursor() as cur:
                        cur.execute("""
                            INSERT INTO candice_result_watch_queue(watch_id,record,status,last_error,updated_at)
                            VALUES(%s,%s::jsonb,'PROCESSING',NULL,NOW())
                            ON CONFLICT(watch_id) DO UPDATE
                            SET record=EXCLUDED.record,
                                updated_at=NOW(),
                                last_error=NULL,
                                status='PROCESSING'
                        """,(watch_id,payload))
                    db.commit()
            await asyncio.to_thread(_persist_entry)
            log.info("RESULT_WATCH_ENTRY_PERSISTED watch_id=%s pair=%s entry=%.12g",watch_id,s.pair,s.entry_price)
        except Exception as e:
            log.warning("RESULT_WATCH_ENTRY_PERSIST_FAILED watch_id=%s type=%s message=%s",watch_id,type(e).__name__,str(e)[:160])
    log.info("ACTUAL_ENTRY_CAPTURED pair=%s entry=%.12g source=%s entry_ts=%s",
             s.pair,s.entry_price,entry_source,
             datetime.fromtimestamp(s.entry_ts,tz=timezone.utc).strftime("%H:%M:%S"))

    await asyncio.sleep(max(0,s.expiry_minutes*60-(time.time()-s.entry_ts)))

    # Result verification is based only on a completed candle.
    expiry_price=None
    expiry_source=""
    client=CLIENT
    for attempt in range(1,5):
        try:
            if client and getattr(client.connection,"is_connected",False):
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
        # Do not recurse: repeated closed-candle misses must not build an
        # unbounded Python call chain. Retry iteratively for a short bounded window.
        retry_deadline=time.time()+20.0
        while expiry_price is None and time.time()<retry_deadline and key in BRAIN.active_signals:
            await asyncio.sleep(1.0)
            client=CLIENT
            for attempt in range(1,5):
                try:
                    if client and getattr(client.connection,"is_connected",False):
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
                    log.warning("RESULT_CANDLE_RETRY_FAILED pair=%s attempt=%d type=%s message=%s",
                                s.pair,attempt,type(e).__name__,str(e)[:120])
                if expiry_price is not None or attempt>=4:
                    break
        if expiry_price is None:
            log.warning(
                "RESULT_PENDING_NO_CLOSED_CANDLE_FINAL pair=%s entry=%s durable_watch=%s "
                "reason=bounded_retry_exhausted",
                s.pair,s.entry_price,watch_id
            )
            await mark_result_watch_error(watch_id,"no_closed_candle_bounded_retry")
            BRAIN.active_signals.pop(key,None)
            return

    rec=BRAIN.finish_signal(key,expiry_price)

    # Result classification is complete. Never block result delivery on an external
    # AI provider; enqueue the full evidence for durable History-AI processing.
    review_id=f"{rec['cycle_id']}:{rec['pair']}:{rec['entry_ts']}"
    await enqueue_ai_review(review_id,rec)
    await save_persistent_learning()
    # 10-signal learning summaries are intentionally silent. Internal learning
    # continues unchanged; only the post-admin 100-Telegram-signal evaluation
    # produces a user-facing report.
    BRAIN.consume_batch_summary()
    telegram_eval_report=rec.get("telegram_eval_report")
    if telegram_eval_report:
        log.info(
            "TELEGRAM_100_SIGNAL_EVALUATION_COMPLETE signals=%s wins=%s losses=%s ties=%s "
            "continue_wins=%s continue_losses=%s win_rate=%s%%",
            telegram_eval_report.get("signals"),telegram_eval_report.get("wins"),
            telegram_eval_report.get("losses"),telegram_eval_report.get("ties"),
            telegram_eval_report.get("continue_wins"),telegram_eval_report.get("continue_losses"),
            telegram_eval_report.get("win_rate")
        )
        await telegram(
            "🏁 TELEGRAM SIGNAL EVALUATION — 100 COMPLETE\\n\\n"
            f"🔢 Signals → {telegram_eval_report.get('signals',0)}/100\\n"
            f"✅ WIN → {telegram_eval_report.get('wins',0)}\\n"
            f"❌ LOSS → {telegram_eval_report.get('losses',0)}\\n"
            f"➡️ Continue WIN → {telegram_eval_report.get('continue_wins',0)}\\n"
            f"➡️ Continue LOSS → {telegram_eval_report.get('continue_losses',0)}\\n"
            f"🟡 TIE → {telegram_eval_report.get('ties',0)}\\n"
            f"🎯 Win Rate → {telegram_eval_report.get('win_rate',0):.2f}%\\n\\n"
            "📌 Count source → successfully delivered Telegram signals only\\n"
            "🚫 DEMO trades / old signals / 10-signal updates are excluded.",
            chat_id=STATE.get("telegram_chat_id") or None
        )
    else:
        snap=BRAIN.telegram_eval_snapshot()
        if snap.get("active") and rec.get("telegram_eval_counted"):
            log.info(
                "TELEGRAM_EVALUATION_PROGRESS signals=%s/100 wins=%s losses=%s ties=%s "
                "continue_wins=%s continue_losses=%s win_rate=%s%%",
                snap.get("signals"),snap.get("wins"),snap.get("losses"),snap.get("ties"),
                snap.get("continue_wins"),snap.get("continue_losses"),snap.get("win_rate")
            )
    label=rec["display_name"]
    direction_icon="🟢" if rec["direction"]=="UP" else "🔴"
    result_icon={"WIN":"✅","LOSS":"❌","TIE":"🟡"}[rec["result"]]
    trend=str(rec.get("trend_15m") or "").upper()
    trend_label="BULLISH" if "BULL" in trend else "BEARISH" if "BEAR" in trend else "—"
    structure=str(rec.get("structure_1m") or "").upper()
    structure_label=(
        "ABOVE AVWAP + POC" if "ABOVE_AVWAP_POC" in structure
        else "BELOW AVWAP + POC" if "BELOW_AVWAP_POC" in structure
        else "—"
    )
    await telegram(
        "📊 <b>CANDICE RESULT</b>\n"
        "\n"
        f"📈 <b>{label}</b>\n"
        f"{direction_icon} <b>{rec['direction']}</b>  •  <b>{rec['expiry_minutes']} MIN</b>\n"
        f"💰 Entry → <code>{rec['entry_price']}</code>\n"
        f"🏁 Exit → <code>{rec['exit_price']}</code>\n"
        f"{result_icon} <b>{rec['result']}</b>\n"
        "\n"
        f"📈 15M Bias → <b>{trend_label}</b>\n"
        f"🕯️ 1M Close → <b>{structure_label}</b>\n"
        "📐 Engine → <b>AVWAP + VOLUME PROFILE</b>\n"
        f"🎯 Confidence → <b>{rec['confidence']}%</b>\n"
        f"🔎 Verification → <b>candle-closed</b>\n\n"
        "🟣 <b>DEMO • MANUAL ENTRY</b>\n"
        "🤖 <b>CANDICE BRAIN • LIVE</b>"
    )
    await complete_result_watch(watch_id)
    log.info(
        "RESULT pair=%s result=%s strategy=%s self_strategy=%s self_version=%s confidence=%s trend=%s structure=%s pattern=%s expiry=%s entry=%s exit=%s source=%s cooldown=%s",
        rec["pair"],rec["result"],rec["strategy"],rec.get("self_strategy",""),rec.get("self_strategy_version",""),rec["confidence"],rec["trend_15m"],
        rec["structure_1m"],rec["pattern"],rec["expiry_minutes"],rec["entry_price"],rec["exit_price"],
        expiry_source,rec["result"]=="LOSS"
    )

async def cycle_loop():
    # Fast 3-minute signal scheduler, active 24/7.
    # For each cycle:
    #   - cycle_start = T - 180s
    #   - Telegram signal = cycle_start + 150s (2m30s after Candice cycle start)
    #   - entry/expiry boundary T = cycle_start + 180s
    #   - five Brain passes are completed inside the same 150s pre-signal window
    #   - pass 5 is the deep/final qualification pass 15s before delivery
    #   - 5s + every complete 1m..15m frame are checked in each pass
    #   - result watching and History-AI remain background tasks and never block
    #     the scheduler, so one completed signal cannot stop the next cycle.
    # The Brain/AI strategy itself is unchanged; only the scheduling cadence
    # and the requested expiry are changed for DEMO analysis.
    # Day signal session: every 3 minutes. Signal is fixed at +2m30s from
    # cycle start; the 1-minute DEMO entry/expiry boundary is +3m00s.
    SIGNAL_INTERVAL=180.0
    SIGNAL_LEADS=(30.0,30.0)
    SCAN_OFFSETS=CYCLE_SCAN_OFFSETS

    async def send_cycle_signal(candidate,target,signal_lead,cycle_id):
        if not signal_session_active(target):
            log.info(
                "SIGNAL_DELIVERY_BLOCKED_OUTSIDE_SESSION cycle=%s target_utc=%s",
                cycle_id,time.strftime("%H:%M:%S",time.gmtime(target))
            )
            return False
        if not candidate or not BRAIN.can_send_cycle_signal(
            STATE.get("account_id"),candidate.get("pair")
        ):
            if candidate:
                log.info(
                    "SIGNAL_DELIVERY_BLOCKED cooldown_or_account cycle=%s pair=%s",
                    cycle_id,candidate.get("pair")
                )
            return False

        p=candidate["pair"]
        expected=str(candidate.get("direction") or "").upper()
        closure_reason=known_broker_closure_reason(p)
        if closure_reason:
            log.info(
                "SIGNAL_DELIVERY_BLOCKED_BROKER_CLOSED cycle=%s pair=%s reason=%s",
                cycle_id,p,closure_reason
            )
            return False
        if not broker_tradeability_fresh(p,time.time()):
            log.info(
                "SIGNAL_DELIVERY_BLOCKED_BROKER_NOT_TRADABLE cycle=%s pair=%s "
                "reason=missing_stale_or_negative_probe next_asset=TRUE",
                cycle_id,p
            )
            return False
        now=time.time()

        # Delivery boundary must be zero-network and bounded. Pass 5 is
        # responsible for warming the strongest candidate quote slots before the
        # boundary. Do NOT subscribe/unsubscribe or wait for broker responses here:
        # one event-12/13 timeout can consume the entire signal second and suppress
        # every remaining fallback candidate. A candidate without a genuinely fresh
        # authenticated event-1 tick is skipped immediately.
        quote_now=time.time()
        quote_age=live_price_age(p,quote_now)
        if not has_fresh_live_price(p,quote_now,LIVE_TICK_MAX_AGE):
            # Event-1 delivery ticks are preferred, but the authenticated broker
            # current-candle endpoint is the authoritative fallback. Refresh the
            # exact candidate at the signal boundary instead of rejecting a valid
            # setup merely because the account-wide 2s rotation has not touched it
            # within the last five seconds.
            refreshed=False
            try:
                refreshed=await asyncio.wait_for(
                    refresh_broker_live_quote(p),timeout=2.5
                )
            except Exception as e:
                log.info(
                    "FINAL_QUOTE_BROKER_REFRESH_FAILED cycle=%s pair=%s "
                    "type=%s message=%s",
                    cycle_id,p,type(e).__name__,str(e)[:120]
                )
            quote_now=time.time()
            quote_age=live_price_age(p,quote_now)
            if refreshed and has_fresh_live_price(
                p,quote_now,max(LIVE_TICK_MAX_AGE,2.5)
            ):
                log.info(
                    "FINAL_QUOTE_BROKER_REFRESHED cycle=%s pair=%s quote_age=%.3f "
                    "source=authenticated_broker_live_candle",
                    cycle_id,p,float(quote_age or 0.0)
                )
            else:
                log.info(
                    "FINAL_QUOTE_STALE_SKIP cycle=%s pair=%s quote_age=%s max_age=%.1f next_asset=TRUE",
                    cycle_id,p,
                    ("NONE" if quote_age is None else f"{quote_age:.3f}"),
                    float(LIVE_TICK_MAX_AGE)
                )
                return False

        entry=STATE["prices"].get(p,(None,None))[0]
        if entry is None:
            log.info(
                "NO_VALID_SIGNAL_AT_SEND cycle=%s pair=%s reason=price_missing next_asset=TRUE",
                cycle_id,p
            )
            return False

        # Final closed-candle confirmation is prepared in SCAN_5, which now runs
        # 15 seconds before the exact signal boundary. The signal second itself is
        # reserved for the fresh authenticated quote + delivery, eliminating the
        # previous ~200-300ms boundary race that caused valid setups to miss.
        prepared_at=candidate.get("final_delivery_prepared_at")
        prepared_ok=bool(candidate.get("final_delivery_confirmed"))
        # Pass 5 is intentionally prepared at the configured signal lead
        # (normally 30s before entry). The old 20s hard limit therefore rejected
        # every normally-prepared candidate at the exact boundary. Fresh live
        # price is still required below, so extending this cache-age bound does
        # not permit stale market quotes to be sent.
        prepared_max_age=max(20.0,float(signal_lead)+5.0)
        prepared_age=(None if prepared_at is None else time.time()-float(prepared_at))
        if prepared_at is None or prepared_age>prepared_max_age:
            log.info(
                "FINAL_DELIVERY_PREP_EXPIRED cycle=%s pair=%s prepared_at=%s age=%s max_age=%.1f next_asset=TRUE",
                cycle_id,p,prepared_at,
                ("NONE" if prepared_age is None else f"{prepared_age:.3f}"),
                prepared_max_age
            )
            return False
        # AVWAP+Volume Profile is the locked live brain. Do not apply legacy
        # multi-timeframe/volume/body delivery gates to this strategy.
        # Fresh authenticated price + closed-candle decision + confidence remain
        # mandatory.
        if not prepared_ok and str(candidate.get("strategy") or "").upper()!="AVWAP_VOLUME_PROFILE":
            log.info(
                "FINAL_DELIVERY_CONFIRMATION_REJECTED cycle=%s pair=%s strategy=%s reason=%s diagnostic=%s next_asset=TRUE",
                cycle_id,p,str(candidate.get("strategy") or ""),
                candidate.get("final_delivery_confirmation_reason","unknown"),
                candidate.get("final_delivery_diagnostic") or {}
            )
            return False

        confidence=int(candidate.get("confidence") or 0)
        strategy_name=str(candidate.get("strategy") or "").upper()
        trend_name=str(candidate.get("trend_15m") or "").upper()

        # Exact-entry market-state gate: the closed M1 setup must still be
        # aligned with the live authenticated quote at the instant of delivery.
        # This uses only the AVWAP + Volume Profile levels already produced by
        # the locked technical Brain; it does not introduce another indicator.
        if strategy_name=="AVWAP_VOLUME_PROFILE":
            ind=dict(candidate.get("indicators") or candidate.get("indicator_context") or {})
            avwap=float(ind.get("anchored_vwap") or candidate.get("avwap") or 0.0)
            poc=float(ind.get("volume_profile_poc") or candidate.get("poc") or 0.0)
            vah=float(ind.get("volume_profile_vah") or candidate.get("vah") or 0.0)
            val=float(ind.get("volume_profile_val") or candidate.get("val") or 0.0)
            aligned_live=(entry>avwap and entry>poc) if expected=="UP" else (entry<avwap and entry<poc)
            acceptance_live=(entry>=vah) if expected=="UP" else (entry<=val)
            if not aligned_live:
                log.info(
                    "FINAL_LIVE_AVWAP_VP_REJECTED cycle=%s pair=%s direction=%s "
                    "reason=live_quote_crossed_avwap_or_poc entry=%s avwap=%s poc=%s next_asset=TRUE",
                    cycle_id,p,expected,entry,avwap,poc
                )
                return False
            if bool(ind.get("value_area_acceptance")) and not acceptance_live:
                log.info(
                    "FINAL_LIVE_AVWAP_VP_REJECTED cycle=%s pair=%s direction=%s "
                    "reason=live_quote_reentered_value_area entry=%s vah=%s val=%s next_asset=TRUE",
                    cycle_id,p,expected,entry,vah,val
                )
                return False

            # The 1-minute continuation check is part of the exact setup. At the
            # delivery boundary, the authenticated live quote must not have fallen
            # below the closed candle that qualified the setup.
            # The Brain already validates M1 continuation on CLOSED candles.
            # At the delivery boundary, AVWAP/POC alignment plus value-area
            # acceptance are the live-state guards. Requiring the live quote to
            # remain beyond the old confirmation-candle close is redundant and
            # incorrectly rejects normal retest/hold behavior before a 1-minute
            # entry. Keep the Brain's closed-candle continuation evidence.
            continuation_ok=bool(ind.get("m1_continuation_ok"))
            if not continuation_ok:
                log.info(
                    "FINAL_LIVE_AVWAP_VP_REJECTED cycle=%s pair=%s direction=%s "
                    "reason=closed_m1_continuation_invalid continuation=%s next_asset=TRUE",
                    cycle_id,p,expected,ind.get("m1_continuation_ok")
                )
                return False

            real_volume_ok=(ind.get("real_volume_verified") is True)
            fallback_value_position=str(ind.get("value_position") or "").upper()
            fallback_direction=str(expected or "").upper()
            # Recovery/reranking can rebuild a candidate and drop bookkeeping
            # flags. Recompute the SAME strict deterministic local fallback
            # from the current Brain indicators instead of trusting metadata.
            strict_local_proxy_fallback=(
                int(confidence or 0)>=90
                and bool(ind.get("exact_live_setup"))
                and ind.get("slope_persistent") is True
                and ind.get("level_reclaim") is False
                and ((fallback_direction=="UP" and fallback_value_position=="ABOVE_VALUE")
                     or (fallback_direction=="DOWN" and fallback_value_position=="BELOW_VALUE"))
            )
            proxy_volume_mode=str(ind.get("volume_mode") or "").upper()
            proxy_volume_ok=(
                ALLOW_PROXY_LIVE_FALLBACK
                and proxy_volume_mode in {
                    "M1_EQUAL_ACTIVITY_PROXY",
                    "M1_TICK_ACTIVITY_PROXY",
                    "TICK_VOLUME",
                }
                and (
                    (
                        candidate.get("volume_proxy_ai_verified") is True
                        and int(candidate.get("volume_proxy_ai_confidence") or 0)>=80
                    )
                    or (
                        candidate.get("volume_proxy_local_fallback") is True
                        and int(candidate.get("volume_proxy_local_fallback_confidence") or 0)>=90
                    )
                    or strict_local_proxy_fallback
                )
            )
            if not real_volume_ok and not proxy_volume_ok:
                log.info(
                    "FINAL_LIVE_AVWAP_VP_REJECTED cycle=%s pair=%s direction=%s "
                    "reason=volume_verification_failed volume_quality=%s coverage=%s proxy_ai_verified=%s next_asset=TRUE",
                    cycle_id,p,expected,ind.get("volume_quality"),
                    ind.get("volume_coverage"),
                    candidate.get("volume_proxy_ai_verified",False)
                )
                return False

        log.info(
            "FINAL_AVWAP_VOLUME_PROFILE_CONFIRMED cycle=%s pair=%s direction=%s diagnostic=%s",
            cycle_id,p,expected,candidate.get("final_delivery_diagnostic") or {}
        )
        if confidence < 90:
            log.info(
                "NO_VALID_SIGNAL_AT_SEND cycle=%s pair=%s reason=confidence_%s",
                cycle_id,p,confidence
            )
            return False

        ts=target-signal_lead
        if time.time() > ts+0.75:
            log.info(
                "NO_VALID_SIGNAL_AT_SEND cycle=%s pair=%s reason=deadline_passed",
                cycle_id,p
            )
            return False

        # Only a candidate that survived pass 5 is eligible for final delivery.
        if (
            str(candidate.get("strategy") or "").upper()!="AVWAP_VOLUME_PROFILE"
            and (
                int(candidate.get("qualified_pass") or 0) < 4
                or not bool(candidate.get("deep_verified"))
            )
        ):
            log.info(
                "NO_VALID_SIGNAL_AT_SEND cycle=%s pair=%s reason=not_deep_final_preparation_qualified",
                cycle_id,p
            )
            return False

        try:
            # Live signal expiry is permanently fixed at 1 minute.
            # The 3-minute value is the cycle interval only; it must never
            # change the DEMO signal expiry in this delivery lane.
            signal_expiry=1
            s=BRAIN.mark_signal_sent(
                account_id=STATE.get("account_id"),
                pair=p,display_name=candidate["display_name"],
                direction=candidate["direction"],expiry_minutes=signal_expiry,
                entry_price=entry,entry_ts=target,
                entry_candle_ts=candidate["entry_candle_ts"],
                strategy=candidate["strategy"],reason=candidate["reason"],confidence=confidence,
                pattern=str(candidate.get("pattern") or ""),
                trend_15m=str(candidate.get("trend_15m") or ""),
                structure_1m=str(candidate.get("structure_1m") or ""),
                self_strategy=str(candidate.get("self_strategy") or ""),
                self_strategy_version=str(candidate.get("self_strategy_version") or ""),
                indicator_context=dict(candidate.get("indicators") or {})
            )
        except Exception as e:
            log.warning("SIGNAL_RESERVATION_FAILED cycle=%s pair=%s type=%s message=%s",
                        cycle_id,p,type(e).__name__,str(e)[:160])
            return False

        key=f"{s.cycle_id}:{s.pair}:{s.entry_ts}"
        indicator_check=format_live_indicator_check(
            s.indicator_context,
            STATE["price_source"].get(s.pair,"authenticated_broker_live_candle")
        )
        log.info(
            "LIVE_INDICATOR_SNAPSHOT cycle=%s pair=%s source=%s indicators=%s",
            cycle_id,s.pair,STATE["price_source"].get(s.pair,"unknown"),
            s.indicator_context or {}
        )
        ind=s.indicator_context or {}
        ef=float(ind.get("ema_fast") or 0.0); es=float(ind.get("ema_slow") or 0.0)
        rv=float(ind.get("rsi") or 0.0); sk=float(ind.get("stochastic_k") or 0.0); sd=float(ind.get("stochastic_d") or 0.0)
        msg=format_candice_signal_message(s, indicator_check, ts, target)

        delivery_started=time.perf_counter()
        delivered=await telegram(msg,timeout_seconds=TELEGRAM_SIGNAL_TIMEOUT)
        delivery_latency_ms=(time.perf_counter()-delivery_started)*1000.0

        if not delivered:
            try:
                brain_key=f"{s.cycle_id}:{s.pair}:{s.entry_ts}"
                BRAIN.active_signals.pop(brain_key,None)
                BRAIN.sent_keys.discard(BRAIN.duplicate_key(s.pair,s.entry_candle_ts))
                BRAIN.cycle_signal_sent=False
            except Exception as rollback_error:
                log.error("SIGNAL_ROLLBACK_FAILED cycle=%s pair=%s type=%s message=%s",
                          cycle_id,p,type(rollback_error).__name__,str(rollback_error)[:160])
            log.warning(
                "TELEGRAM_SIGNAL_DELIVERY_FAILED cycle=%s pair=%s latency_ms=%.1f "
                "reason=delivery_not_confirmed next_asset=TRUE",
                cycle_id,p,delivery_latency_ms
            )
            return False

        signal_lag_seconds=time.time()-ts
        log.info(
            "TELEGRAM_SIGNAL_DELIVERY cycle=%s pair=%s sent=True latency_ms=%.1f "
            "signal_lag_seconds=%.3f entry_in_seconds=%.3f",
            cycle_id,p,delivery_latency_ms,signal_lag_seconds,
            max(0.0,target-time.time())
        )
        log.info(
            "FINAL_SIGNAL cycle=%s pair=%s direction=%s strategy=%s confidence=%s price_source=%s "
            "trend=%s structure=%s pattern=%s self_strategy=%s self_version=%s expiry=%s "
            "qualified_pass=%s signal_utc=%s target_utc=%s lead_seconds=%.3f",
            cycle_id,s.pair,s.direction,s.strategy or "UNKNOWN",s.confidence,
            STATE["price_source"].get(s.pair,"unknown"),
            s.trend_15m or "UNKNOWN",s.structure_1m or "UNKNOWN",s.pattern or "UNKNOWN",
            getattr(s,"self_strategy","UNKNOWN"),getattr(s,"self_strategy_version","UNKNOWN"),
            s.expiry_minutes,candidate.get("qualified_pass"),
            datetime.fromtimestamp(ts,tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3],
            datetime.fromtimestamp(target,tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3],
            target-time.time()
        )
        asyncio.create_task(persist_result_watch_background(key,s))
        start_result_watch(key)
        return True

    cycle_sequence=0
    target=None

    def account_ready_now():
        return bool(
            CLIENT
            and getattr(CLIENT.connection,"is_connected",False)
            and STATE.get("account_group")=="demo"
            and STATE.get("account_id")
            and str(STATE.get("feed_source") or "").startswith("authenticated_websocket:event_182_account_universe")
            and len(STATE.get("assets") or [])>0
        )

    while True:
        now=time.time()

        # Signal delivery is enabled across the full UAE day. This changes only
        # the signal schedule; broker auto-trading remains isolated/off.
        if not signal_session_active(now):
            if target is not None:
                log.info(
                    "SIGNAL_SESSION_OFF start=00:00 end=24:00 timezone=Asia/Dubai "
                    "reason=overnight_learning_mode"
                )
            target=None
            next_start=next_signal_session_start_epoch(now)
            log.info(
                "SIGNAL_SESSION_WAIT next_start_utc=%s force_signal_mode=%s",
                time.strftime("%H:%M:%S",time.gmtime(next_start)),FORCE_SIGNAL_MODE
            )
            await asyncio.sleep(min(max(1.0,next_start-time.time()),60.0))
            continue

        recovered=None
        resume_completed_pass=0

        if target is None:
            recovered=await load_recoverable_cycle_state(now)
            if recovered:
                target=float(recovered["target_epoch"])
                cycle_id=int(recovered["cycle_id"])
                cycle_sequence=(cycle_id % 20) or 20
                signal_lead=SIGNAL_LEADS[0 if cycle_sequence<=10 else 1]
                signal_at=target-signal_lead
                resume_completed_pass=int(recovered.get("completed_pass") or 0)
                candidate_pool={}
                for item in recovered.get("candidate_pool") or []:
                    if isinstance(item,dict) and item.get("pair"):
                        # Strategy is part of candidate identity. A single asset/candle
                        # may have multiple independently-qualified techniques; recovery
                        # must not collapse them back into one entry after a restart.
                        key=(
                            item.get("pair"),
                            str(item.get("entry_candle_ts")),
                            str(item.get("direction") or "").upper(),
                            str(item.get("strategy") or "").upper(),
                            str(item.get("self_strategy_version") or "")
                        )
                        candidate_pool[key]=item
                log.info(
                    "CYCLE_RESUME_AFTER_RESTART cycle=%s completed_pass=%s candidates=%s",
                    cycle_id,resume_completed_pass,len(candidate_pool)
                )
            else:
                target=(int(now)//int(SIGNAL_INTERVAL)+1)*int(SIGNAL_INTERVAL)
                cycle_id=int(target//SIGNAL_INTERVAL)
                cycle_sequence=(cycle_id % 20) or 20
                signal_lead=SIGNAL_LEADS[0 if cycle_sequence<=10 else 1]
                signal_at=target-signal_lead
                candidate_pool={}
        else:
            target += SIGNAL_INTERVAL
            cycle_id=int(target//SIGNAL_INTERVAL)
            cycle_sequence=(cycle_id % 20) or 20
            signal_lead=SIGNAL_LEADS[0 if cycle_sequence<=10 else 1]
            signal_at=target-signal_lead
            candidate_pool={}

        signal_lead=SIGNAL_LEADS[0 if cycle_sequence<=10 else 1]
        signal_at=target-signal_lead

        if not signal_session_active(target):
            log.info(
                "SIGNAL_CYCLE_BLOCKED_OUTSIDE_06_18 cycle=%s target_utc=%s",
                cycle_id,time.strftime("%H:%M:%S",time.gmtime(target))
            )
            target=None
            continue

        # Restart guard: an ordinary restart must not throw away a still-viable
        # recovered cycle. When a persisted cycle is active, its target/signaling
        # boundary stays authoritative and completed passes are resumed in place.
        # Only discard the recovered cycle when too little time remains to safely
        # finish the pending passes and perform the final delivery checks.
        cycle_start=target-SIGNAL_INTERVAL
        if time.time()>cycle_start+5.0:
            recovery_signal_in=max(0.0,signal_at-time.time())
            can_resume_recovered=bool(
                recovered
                and (
                    (
                        resume_completed_pass < len(SCAN_OFFSETS)
                        and recovery_signal_in >= 20.0
                    )
                    or (
                        # Pass 5 is already complete, so no additional analysis
                        # window is required. Preserve the exact original signal
                        # timestamp even when a Render restart leaves <20s.
                        resume_completed_pass >= len(SCAN_OFFSETS)
                        and recovery_signal_in >= 1.0
                        and bool(candidate_pool)
                    )
                )
            )
            if can_resume_recovered:
                log.info(
                    "CYCLE_RECOVERY_KEEP_PARTIAL cycle=%s completed_pass=%s "
                    "signal_utc=%s signal_in=%.2f reason=resumable_window",
                    cycle_id,resume_completed_pass,
                    time.strftime("%H:%M:%S",time.gmtime(signal_at)),
                    recovery_signal_in
                )
            else:
                old_target=target
                old_cycle_id=cycle_id
                target+=SIGNAL_INTERVAL
                cycle_id=int(target//SIGNAL_INTERVAL)
                cycle_sequence=(cycle_id % 20) or 20
                signal_lead=30.0
                signal_at=target-signal_lead
                cycle_start=target-SIGNAL_INTERVAL
                if recovered:
                    log.info(
                        "CYCLE_RECOVERY_DISCARDED cycle=%s old_cycle=%s reason=window_not_resumable "
                        "signal_in=%.2f old_target_utc=%s",
                        cycle_id,old_cycle_id,recovery_signal_in,
                        time.strftime("%H:%M:%S",time.gmtime(old_target))
                    )
                    recovered=None
                    resume_completed_pass=0
                    candidate_pool={}
                log.info(
                    "CYCLE_RESTART_ALIGNMENT cycle=%s old_target_utc=%s new_start_utc=%s signal_utc=%s target_utc=%s",
                    cycle_id,
                    time.strftime("%H:%M:%S",time.gmtime(old_target)),
                    time.strftime("%H:%M:%S",time.gmtime(cycle_start)),
                    time.strftime("%H:%M:%S",time.gmtime(signal_at)),
                    time.strftime("%H:%M:%S",time.gmtime(target))
                )
        until_start=cycle_start-time.time()
        if until_start>0:
            log.info(
                "CYCLE_START_WAIT cycle=%s start_utc=%s seconds=%.2f signal_utc=%s target_utc=%s",
                cycle_id,time.strftime("%H:%M:%S",time.gmtime(cycle_start)),until_start,
                time.strftime("%H:%M:%S",time.gmtime(signal_at)),
                time.strftime("%H:%M:%S",time.gmtime(target))
            )
            await asyncio.sleep(until_start)

        # Root-cause guard: cycle analysis must never start with an empty or
        # unauthenticated account snapshot. A restart immediately before the
        # signal boundary cannot complete five passes safely, so skip that
        # boundary and preserve the next full 3-minute cycle window.
        signal_in=max(0.0,signal_at-time.time())
        log.info(
            "CYCLE_ACCOUNT_READY_GUARD cycle=%s account_ready=%s assets=%d "
            "signal_in=%.2f signal_utc=%s",
            cycle_id,account_ready_now(),len(STATE.get("assets") or []),signal_in,
            time.strftime("%H:%M:%S",time.gmtime(signal_at))
        )
        if signal_in < 20.0:
            final_recovery_ready=bool(
                recovered
                and resume_completed_pass >= len(SCAN_OFFSETS)
                and signal_in >= 1.0
                and candidate_pool
                and account_ready_now()
            )
            if not final_recovery_ready:
                log.warning(
                    "CYCLE_SKIPPED_LATE_START cycle=%s signal_utc=%s signal_in=%.2f "
                    "reason=insufficient_window_for_account_and_five_passes",
                    cycle_id,time.strftime("%H:%M:%S",time.gmtime(signal_at)),signal_in
                )
                continue
            log.info(
                "CYCLE_RECOVERY_FINAL_WINDOW cycle=%s completed_pass=%s "
                "signal_in=%.2f candidates=%s reason=pass5_already_complete",
                cycle_id,resume_completed_pass,signal_in,len(candidate_pool)
            )

        if not account_ready_now():
            ready_deadline=signal_at-20.0
            while time.time()<ready_deadline and not account_ready_now():
                await asyncio.sleep(0.5)
            if not account_ready_now():
                log.warning(
                    "CYCLE_SKIPPED_ACCOUNT_NOT_READY cycle=%s signal_utc=%s "
                    "reason=account_not_ready_by_safe_window",
                    cycle_id,time.strftime("%H:%M:%S",time.gmtime(signal_at))
                )
                continue

        # Account/feed is now authenticated and has a non-empty account asset
        # universe before Brain state for this cycle is created.
        BRAIN.start_cycle(cycle_id)
        STATE["cycle"]=cycle_id
        STATE["cycle_scan_status"]={
            "cycle_id":int(cycle_id),
            "total_passes":CYCLE_SCAN_COUNT,
            "completed_pass":int(resume_completed_pass or 0),
            "scan_offsets_seconds":[int(x) for x in SCAN_OFFSETS],
            "signal_lead_seconds":int(signal_lead),
            "signal_epoch":float(signal_at),
            "target_epoch":float(target),
            "scans":[],
            "protocol":"FLEX_MANUAL",
            "signal_expiry_minutes":1,
        }
        # Recovery metadata is best-effort; never block the signal scheduler on DB I/O.
        asyncio.create_task(save_cycle_state(
            cycle_id,target,signal_at,signal_lead,resume_completed_pass,
            candidate_pool,status="ACTIVE",
            reason="cycle_resumed" if recovered else "cycle_started"
        ))

        log.info(
            "CYCLE_WINDOW_START cycle=%s sequence=%s interval=%ss cycle_start_offset=0s signal_offset=150s signal_lead=%ss "
            "signal_utc=%s target_utc=%s analysis_passes=5 frames=5s,1m..15m",
            cycle_id,cycle_sequence,int(SIGNAL_INTERVAL),int(signal_lead),
            time.strftime("%H:%M:%S",time.gmtime(signal_at)),
            time.strftime("%H:%M:%S",time.gmtime(target))
        )
        log.info(
            "SIGNAL_LEAD_PROFILE cycle=%s sequence=%s lead_seconds=%s block=%s",
            cycle_id,cycle_sequence,int(signal_lead),
            "30S"
        )

        catchup_mode=False
        catchup_next_at=None

        for pass_no,offset in enumerate(SCAN_OFFSETS,1):
            if pass_no<=resume_completed_pass:
                log.info(
                    "SCAN_RESTORED cycle=%s scan=SCAN_%s pass=%s "
                    "reason=already_completed_before_restart",
                    cycle_id,pass_no,pass_no
                )
                continue

            scan_at=target-offset

            if pass_no==1:
                # On a Render restart, the ideal first scan may already be in
                # the past. Do not burn all five passes against an empty account
                # state. Wait for the authenticated demo feed, then run the
                # missed passes in a compressed but still separated window.
                # The first scan is scheduled exactly at the previous
                # signal boundary. A few hundred milliseconds of scheduler
                # jitter must NOT turn a normal cycle into catch-up mode.
                # Enter catch-up only when the scan was genuinely missed.
                catchup_mode=(time.time()-scan_at)>5.0
                if catchup_mode:
                    ready_deadline=signal_at-20.0
                    while time.time()<ready_deadline:
                        account_ready=bool(
                            CLIENT
                            and getattr(CLIENT.connection,"is_connected",False)
                            and STATE.get("account_group")=="demo"
                            and STATE.get("account_id")
                            and str(STATE.get("feed_source") or "").startswith("authenticated_websocket:event_182_account_universe")
                            and len(STATE.get("assets") or [])>0
                        )
                        if account_ready:
                            break
                        await asyncio.sleep(1.0)
                    account_ready=account_ready_now()
                    if not account_ready:
                        log.warning(
                            "FIVE_SCAN_ACCOUNT_NOT_READY cycle=%s signal_utc=%s now_utc=%s",
                            cycle_id,
                            time.strftime("%H:%M:%S",time.gmtime(signal_at)),
                            time.strftime("%H:%M:%S",time.gmtime(time.time()))
                        )
                        break
            elif not catchup_mode:
                await asyncio.sleep(max(0.0,scan_at-time.time()))
            elif catchup_mode:
                # Recovery must not squeeze all five passes toward the signal
                # deadline.  Run an overdue pass immediately, then preserve the
                # original wall-clock timestamp for each future pass.  This keeps
                # pass 5 at target-150s instead of accidentally moving it to the
                # last few seconds before Telegram delivery.
                await asyncio.sleep(max(0.0,scan_at-time.time()))

            remaining=max(0,signal_at-time.time())
            if remaining<=3.0:
                log.info(
                    "FIVE_SCAN_WINDOW_CLOSED cycle=%s scan=SCAN_%s remaining=%.2f",
                    cycle_id,pass_no,remaining
                )
                break

            scan_budget=max(1.0,remaining-4.0)
            try:
                # Pass 5 is delivery-prep critical. Never start a 52-asset candle
                # refresh here: any broker I/O in this 25-second-to-signal window
                # can push the scheduler past the exact 30-second boundary.
                # Passes 1-4 already refreshed the closed-candle dataset; final
                # delivery separately enforces live-tick freshness.
                force_refresh=(pass_no in (1,4))
                if pass_no==5:
                    await asyncio.sleep(0)
                    force_refresh=False
                    log.info(
                        "ACCOUNT_FULL_SCAN_PASS5_REUSE_CANDLES cycle=%s scan=SCAN_%s "
                        "seconds_to_signal=%.2f",
                        cycle_id,pass_no,max(0,signal_at-time.time())
                    )
                else:
                    await asyncio.wait_for(
                        refresh_candles(force=force_refresh),
                        timeout=scan_budget
                    )
                log.info(
                    "ACCOUNT_FULL_SCAN cycle=%s scan=SCAN_%s pass=%s assets=%s analyzed=%s "
                    "seconds_to_signal=%.2f deep=%s catchup=%s force_refresh=%s",
                    cycle_id,pass_no,pass_no,len(STATE["assets"]),
                    len(STATE["analyses"]),max(0,signal_at-time.time()),
                    pass_no==5,catchup_mode,force_refresh
                )
            except asyncio.TimeoutError:
                log.warning(
                    "ACCOUNT_FULL_SCAN_TIMEOUT cycle=%s scan=SCAN_%s pass=%s budget=%.2f "
                    "seconds_to_signal=%.2f",
                    cycle_id,pass_no,pass_no,scan_budget,
                    max(0,signal_at-time.time())
                )
            except Exception as e:
                log.warning(
                    "ACCOUNT_FULL_SCAN_FAILED cycle=%s scan=SCAN_%s pass=%s type=%s message=%s",
                    cycle_id,pass_no,pass_no,type(e).__name__,str(e)[:160]
                )

            remaining=max(0,signal_at-time.time())
            if remaining<=3.0:
                break

            try:
                if pass_no==5:
                    # PASS_5 is delivery preparation only. The expensive Brain/AI
                    # qualification is already completed in pass 4. Reusing the
                    # current deep-qualified pool avoids the boundary race that
                    # previously consumed the last seconds before Telegram delivery.
                    candidate=sorted(
                        [
                            x for x in candidate_pool.values()
                            if isinstance(x,dict)
                            and x.get("pair")
                            and int(x.get("qualified_pass") or 0)>=4
                            and bool(x.get("deep_verified"))
                        ],
                        key=lambda x:(
                            float(x.get("meta_rank_score") or x.get("confidence") or 0),
                            int(x.get("confidence") or 0),
                            float(x.get("strategy_margin") or 0),
                            float(x.get("direction_agreement") or 0),
                            float(x.get("market_quality") or 0)
                        ),
                        reverse=True
                    )[:100]
                else:
                    candidate=await asyncio.wait_for(
                        final_candidate(
                            require_live_price=False,
                            deep_analysis=(pass_no==4),
                            use_cached_only=False,
                            return_ranked=(pass_no in (1,2,3,4))
                        ),
                        timeout=max(1.0,remaining-0.50)
                    )
                if isinstance(candidate,list):
                    selected=candidate[:100]
                    if not selected:
                        log.info(
                            "SCAN_COMPLETE cycle=%s scan=SCAN_%s candidate=none analyzed=%d",
                            cycle_id,pass_no,len(STATE["analyses"])
                        )
                    else:
                        prepared_selected=[]
                        for raw_candidate in selected:
                            if not isinstance(raw_candidate,dict) or not raw_candidate.get("pair"):
                                continue
                            item=raw_candidate.copy()
                            item["expiry_minutes"]=1
                            item["qualified_pass"]=pass_no
                            item["deep_verified"]=pass_no>=4
                            item["final_prepared_pass"]=pass_no if pass_no>=4 else 0
                            prepared_selected.append(item)
                            # Keep every independently-qualified technique.
                            # Pair+direction alone is not a unique candidate: BREAKOUT,
                            # TREND_FOLLOWING, PRICE_ACTION, etc. can legitimately
                            # produce separate validated setups for the same asset.
                            # If we key only by pair/direction, the later technique
                            # overwrites the earlier one and the final boundary can
                            # stop after effectively testing one technique.
                            key=(
                                item.get("pair"),
                                str(item.get("entry_candle_ts")),
                                str(item.get("direction") or "").upper(),
                                str(item.get("strategy") or "").upper(),
                                str(item.get("self_strategy_version") or "")
                            )
                            candidate_pool[key]=item

                        if pass_no==4:
                            # Prepare the authenticated tick slots well before the
                            # exact 30-second signal boundary. Keep up to four
                            # independent final candidates live so the fallback
                            # loop can continue even when one or more fail the
                            # final candle gate. Any broker latency is absorbed
                            # here, never at signal send time.
                            prep_reference=time.time()
                            for _item in prepared_selected:
                                _item["final_delivery_precheck"]=_final_delivery_precheck(_item,prep_reference)
                            _prep_ranked=sorted(
                                prepared_selected,
                                key=lambda x:(
                                    float(x.get("final_delivery_precheck") or -900.0),
                                    float(x.get("meta_rank_score") or x.get("confidence") or 0),
                                    int(x.get("confidence") or 0),
                                    float(x.get("strategy_margin") or 0),
                                    float(x.get("direction_agreement") or 0),
                                    float(x.get("market_quality") or 0)
                                ),
                                reverse=True
                            )
                            prep_items=[]
                            _prep_seen_identities=set()
                            for _item in _prep_ranked:
                                _candidate_identity=(
                                    str(_item.get("pair") or ""),
                                    str(_item.get("entry_candle_ts")),
                                    str(_item.get("direction") or "").upper(),
                                    str(_item.get("strategy") or "").upper(),
                                    str(_item.get("self_strategy_version") or ""),
                                )
                                if not _candidate_identity[0] or _candidate_identity in _prep_seen_identities:
                                    continue
                                _prep_seen_identities.add(_candidate_identity)
                                prep_items.append(_item)
                                if len(prep_items)>=ACCOUNT_TICK_PIN_SLOTS:
                                    break
                            if prep_items:
                                pin_ttl=max(45.0,target-time.time()+20.0)
                                try:
                                    active_prep=await pin_account_tick_pairs(
                                        [x.get("pair") for x in prep_items],
                                        ttl=pin_ttl
                                    )
                                    log.info(
                                        "FINAL_CANDIDATE_TICKS_PREPARED cycle=%s pairs=%s pass=%s active=%s ttl=%.1f target_utc=%s",
                                        cycle_id,[x.get("pair") for x in prep_items],
                                        pass_no,active_prep,pin_ttl,
                                        datetime.fromtimestamp(
                                            target,tz=timezone.utc
                                        ).strftime("%H:%M:%S")
                                    )
                                except Exception as e:
                                    log.warning(
                                        "FINAL_CANDIDATE_TICKS_PREPARE_FAILED cycle=%s pairs=%s pass=%s message=%s",
                                        cycle_id,[x.get("pair") for x in prep_items],
                                        pass_no,type(e).__name__,str(e)[:120]
                                    )
                        if pass_no==5:
                            # Pass 5 is deliberately 15 seconds before the exact signal
                            # boundary. Perform the expensive closed-candle/higher-timeframe
                            # confirmation here and reserve the boundary itself for only
                            # fresh-tick validation + Telegram delivery.
                            remaining_to_signal=max(0.0,signal_at-time.time())
                            final_reference=time.time()
                            _final_ranked=sorted(
                                prepared_selected,
                                key=lambda x:(
                                    float(x.get("final_delivery_precheck") or -900.0),
                                    int(x.get("confidence") or 0),
                                    float(x.get("strategy_margin") or 0),
                                    float(x.get("direction_agreement") or 0),
                                    float(x.get("market_quality") or 0)
                                ),
                                reverse=True
                            )
                            # Final closed-candle preparation is bounded to 12 unique
                            # assets so it always completes before the 30-second lead.
                            # The complete deep-qualified candidate_pool remains intact
                            # for fallback/learning; this only bounds expensive prep.
                            final_items=[]
                            _final_seen_identities=set()
                            for _item in _final_ranked:
                                _candidate_identity=(
                                    str(_item.get("pair") or ""),
                                    str(_item.get("entry_candle_ts")),
                                    str(_item.get("direction") or "").upper(),
                                    str(_item.get("strategy") or "").upper(),
                                    str(_item.get("self_strategy_version") or ""),
                                )
                                if not _candidate_identity[0] or _candidate_identity in _final_seen_identities:
                                    continue
                                _final_seen_identities.add(_candidate_identity)
                                final_items.append(_item)
                                if len(final_items)>=ACCOUNT_TICK_FINAL_PROBE_LIMIT:
                                    break
                            for _item in final_items:
                                p=_item.get("pair")
                                expected=str(_item.get("direction") or "").upper()
                                strategy_name=str(_item.get("strategy") or "").upper()

                                if not await check_broker_asset_tradeability(
                                    p,cycle_id=cycle_id,force=True
                                ):
                                    _item["broker_tradeable"]=False
                                    _item["broker_tradeability_checked_at"]=time.time()
                                    _item["broker_tradeability_source"]="event95+event80"
                                    _item["final_delivery_confirmed"]=False
                                    _item["final_delivery_confirmation_reason"]="broker_asset_not_tradeable"
                                    _item["final_delivery_diagnostic"]={
                                        "broker_tradeable":False,
                                        "source":"event95+event80",
                                    }
                                    _item["final_delivery_prepared_at"]=final_reference
                                    log.info(
                                        "FINAL_TRADEABILITY_REJECTED cycle=%s pair=%s "
                                        "reason=broker_asset_not_tradeable source=event95+event80",
                                        cycle_id,p
                                    )
                                    continue

                                _item["broker_tradeable"]=True
                                _item["broker_tradeability_checked_at"]=time.time()
                                _item["broker_tradeability_source"]="event95+event80"

                                closed_1m=_closed_candles(
                                    STATE["candles"].get(p,[]),final_reference
                                )

                                reason=None
                                diag={}

                                if strategy_name=="AVWAP_VOLUME_PROFILE":
                                    asset=next((a for a in STATE.get("assets") or [] if str(a.get("pair"))==str(p)),None)
                                    analysis_closed_1m=_apply_tick_activity_volume(p,closed_1m)
                                    refreshed=analyze_asset(
                                        asset or {"pair":p,"display_name":p},
                                        analysis_closed_1m,
                                        STATE["prices"].get(p,(None,None))[0]
                                    )
                                    if not refreshed:
                                        reason="avwap_volume_profile_recheck_failed"
                                        diag={"closed_1m":len(closed_1m)}
                                    elif str(refreshed.get("direction") or "").upper()!=expected:
                                        reason="avwap_volume_profile_direction_changed"
                                        diag={
                                            "expected":expected,
                                            "actual":refreshed.get("direction"),
                                            "closed_1m":len(closed_1m),
                                        }
                                    else:
                                        # Replace stale candidate features with the fresh
                                        # closed-candle AVWAP+VP calculation before delivery.
                                        _item.update(refreshed)
                                        _item["expiry_minutes"]=1
                                        _item["qualified_pass"]=pass_no
                                        _item["deep_verified"]=True
                                        _item["final_delivery_precheck"]=_final_delivery_precheck(
                                            _item,final_reference
                                        )
                                        diag={
                                            "engine":"AVWAP_VOLUME_PROFILE",
                                            "direction":refreshed.get("direction"),
                                            "confidence":refreshed.get("confidence"),
                                            "value_area_acceptance":refreshed.get("value_area_acceptance"),
                                            "level_reclaim":(refreshed.get("indicators") or {}).get("level_reclaim"),
                                            "slope_persistent":(refreshed.get("indicators") or {}).get("slope_persistent"),
                                            "poc_migration":(refreshed.get("indicators") or {}).get("profile_poc_migration_norm"),
                                            "volume_quality":(refreshed.get("indicators") or {}).get("volume_quality"),
                                            "volume_coverage":(refreshed.get("indicators") or {}).get("volume_coverage"),
                                            "closed_1m":len(closed_1m),
                                        }
                                else:
                                    reason="legacy_strategy_not_allowed"
                                    diag={"strategy":strategy_name}

                                _item["final_delivery_confirmed"]=not bool(reason)
                                _item["final_delivery_confirmation_reason"]=reason or "avwap_volume_profile_confirmed"
                                _item["final_delivery_diagnostic"]=diag
                                _item["final_delivery_prepared_at"]=final_reference

                                if reason:
                                    log.info(
                                        "FINAL_DELIVERY_PREP_REJECTED cycle=%s pair=%s reason=%s diagnostic=%s",
                                        cycle_id,p,reason,diag
                                    )
                                else:
                                    log.info(
                                        "FINAL_DELIVERY_PREP_CONFIRMED cycle=%s pair=%s diagnostic=%s seconds_to_signal=%.2f",
                                        cycle_id,p,diag,remaining_to_signal
                                    )

                            _pin_items=[]
                            _pin_seen_pairs=set()
                            for _item in final_items:
                                _p=str(_item.get("pair") or "")
                                if not _p or _p in _pin_seen_pairs:
                                    continue
                                _pin_seen_pairs.add(_p)
                                _pin_items.append(_item)
                                if len(_pin_items)>=16:
                                    break
                            if _pin_items:
                                try:
                                    # Probe the full prepared fallback set. The tick manager
                                    # keeps only four assets active, skips unusable broker
                                    # subscriptions, and requires a real fresh event-1 tick.
                                    pin_ttl=max(45.0,target-time.time()+20.0)
                                    # Broker Event-12 capability/freshness probing is bounded here.
                                    # A stalled subscription must never hold cycle_loop past the
                                    # exact signal boundary. Delivery still performs its authoritative
                                    # fresh-tick checks and can fall through to the next candidate.
                                    remaining_to_boundary=max(0.0,signal_at-time.time())
                                    pin_timeout=min(
                                        FINAL_TICK_PREP_TIMEOUT,
                                        max(1.0,remaining_to_boundary-3.0)
                                    )
                                    active_prep=await asyncio.wait_for(
                                        pin_account_tick_pairs(
                                            [x.get("pair") for x in _pin_items],
                                            ttl=pin_ttl,
                                            require_fresh=True,
                                            fresh_wait=ACCOUNT_TICK_FRESH_WAIT
                                        ),
                                        timeout=pin_timeout
                                    )
                                    log.info(
                                        "FINAL_CANDIDATE_TICKS_FINAL_PREPARED cycle=%s probe=%s active=%s pass=%s ttl=%.1f seconds_to_signal=%.2f",
                                        cycle_id,[x.get("pair") for x in _pin_items],
                                        active_prep,pass_no,pin_ttl,
                                        max(0.0,signal_at-time.time())
                                    )
                                except Exception as e:
                                    log.warning(
                                        "FINAL_CANDIDATE_TICKS_FINAL_PREPARE_FAILED cycle=%s probe=%s pass=%s type=%s message=%s",
                                        cycle_id,[x.get("pair") for x in _pin_items],
                                        pass_no,type(e).__name__,str(e)[:120]
                                    )

                        for item in selected:
                            log.info(
                                "SCAN_CANDIDATE_SELECTED cycle=%s scan=SCAN_%s pass=%s pair=%s "
                                "confidence=%s strategy=%s expiry=%s pool=%s deep=%s",
                                cycle_id,pass_no,pass_no,item.get("pair"),
                                item.get("confidence"),item.get("strategy"),1,
                                len(candidate_pool),pass_no==5
                            )
                elif candidate is None:
                    log.info(
                        "SCAN_COMPLETE cycle=%s scan=SCAN_%s candidate=none analyzed=%d",
                        cycle_id,pass_no,len(STATE["analyses"])
                    )
                else:
                    candidate=candidate.copy()
                    candidate["expiry_minutes"]=1
                    candidate["qualified_pass"]=pass_no
                    candidate["deep_verified"]=pass_no>=4
                    candidate["final_prepared_pass"]=pass_no if pass_no>=4 else 0
                    # Preserve the technique identity in the cycle pool.
                    # A single asset may have multiple independently-qualified
                    # strategies; none should overwrite another before final gates.
                    key=(
                        candidate.get("pair"),
                        str(candidate.get("entry_candle_ts")),
                        str(candidate.get("direction") or "").upper(),
                        str(candidate.get("strategy") or "").upper(),
                        str(candidate.get("self_strategy_version") or "")
                    )
                    candidate_pool[key]=candidate
                    log.info(
                        "SCAN_CANDIDATE_SELECTED cycle=%s scan=SCAN_%s pass=%s pair=%s "
                        "confidence=%s strategy=%s expiry=%s pool=%s deep=%s",
                        cycle_id,pass_no,pass_no,candidate.get("pair"),
                        candidate.get("confidence"),candidate.get("strategy"),
                        candidate.get("expiry_minutes"),len(candidate_pool),
                        pass_no==5
                    )
            except asyncio.TimeoutError:
                log.warning(
                    "SCAN_EVALUATION_TIMEOUT cycle=%s scan=SCAN_%s pass=%s remaining=%.2f",
                    cycle_id,pass_no,max(0,signal_at-time.time())
                )
            except Exception as e:
                log.exception(
                    "SCAN_EVALUATION_FAILED cycle=%s scan=SCAN_%s pass=%s type=%s message=%s",
                    cycle_id,pass_no,type(e).__name__,str(e)[:160]
                )

            # Persist a compact visible audit of this pass. The Brain has already
            # evaluated the strategy families using the same indicator snapshot; this
            # state is read-only observability and cannot change ranking/direction.
            try:
                _strategy_scan_counts={}
                _indicator_scoped_assets=0
                for _an in STATE.get("analyses",{}).values():
                    if not isinstance(_an,dict):
                        continue
                    if _an.get("strategy_audit"):
                        _indicator_scoped_assets+=1
                    for _av in (_an.get("strategy_audit") or []):
                        _sid=str(_av.get("strategy") or "").upper()
                        if _sid:
                            _strategy_scan_counts.setdefault(_sid,0)
                            if bool(_av.get("up_qualified")) or bool(_av.get("down_qualified")):
                                _strategy_scan_counts[_sid]+=1
                _scan_state=STATE.setdefault("cycle_scan_status",{})
                _scan_state["completed_pass"]=pass_no
                _scan_state["scans"]=list(_scan_state.get("scans") or [])
                _scan_state["scans"].append({
                    "pass":pass_no,
                    "offset_seconds":int(offset),
                    "scan_utc":datetime.fromtimestamp(scan_at,tz=timezone.utc).isoformat(),
                    "executed_utc":datetime.now(timezone.utc).isoformat(),
                    "assets":len(STATE.get("assets") or []),
                    "analyzed":len(STATE.get("analyses") or {}),
                    "indicator_scoped_assets":_indicator_scoped_assets,
                    "qualified_by_strategy":_strategy_scan_counts,
                    "candidate_pool_size":len(candidate_pool),
                })
                if len(_scan_state["scans"])>CYCLE_SCAN_COUNT:
                    _scan_state["scans"]=_scan_state["scans"][-CYCLE_SCAN_COUNT:]
                log.info(
                    "CANDICE_5SCAN_AUDIT cycle=%s scan=%s/%s offset=%ss assets=%s analyzed=%s "
                    "indicator_scoped=%s qualified_by_strategy=%s pool=%s",
                    cycle_id,pass_no,CYCLE_SCAN_COUNT,int(offset),
                    len(STATE.get("assets") or []),len(STATE.get("analyses") or {}),
                    _indicator_scoped_assets,_strategy_scan_counts,len(candidate_pool)
                )
            except Exception as _scan_state_error:
                log.warning(
                    "CANDICE_5SCAN_AUDIT_FAILED cycle=%s scan=%s/%s type=%s message=%s",
                    cycle_id,pass_no,CYCLE_SCAN_COUNT,
                    type(_scan_state_error).__name__,str(_scan_state_error)[:120]
                )

            # Warm candidate tick subscriptions during passes 1-3 so genuine
            # 30s buckets can accumulate before the final 1-minute gate.
            # This is read-only market-data preparation; Brain direction,
            # strategy, expiry, and the Telegram send rules remain unchanged.
            if pass_no in (1,2,3) and candidate_pool:
                warm_pool=sorted(
                    [x for x in candidate_pool.values()
                     if isinstance(x,dict) and x.get("pair")],
                    key=lambda x:(
                        int(x.get("confidence") or 0),
                        float(x.get("market_quality") or 0),
                    ),
                    reverse=True
                )[:ACCOUNT_TICK_PIN_SLOTS]
                try:
                    warm_ttl=max(75.0,target-time.time()+45.0)
                    active_warm=await pin_account_tick_pairs(
                        [x.get("pair") for x in warm_pool],
                        ttl=warm_ttl
                    )
                    log.info(
                        "EARLY_30S_TICK_WARM cycle=%s pass=%s pairs=%s active=%s ttl=%.1f seconds_to_signal=%.2f",
                        cycle_id,pass_no,[x.get("pair") for x in warm_pool],
                        active_warm,warm_ttl,
                        max(0.0,signal_at-time.time())
                    )
                except Exception as e:
                    log.warning(
                        "EARLY_30S_TICK_WARM_FAILED cycle=%s pass=%s type=%s message=%s",
                        cycle_id,pass_no,type(e).__name__,str(e)[:120]
                    )

            # Final recovery window: a narrow pass-5 result is still recoverable.
            # The previous condition only retried when pass 5 returned NO candidate.
            # If pass 5 returned exactly one candidate and that candidate failed the
            # last-second closed-candle gate, the cycle had no fallback even when an
            # earlier-pass candidate remained available in candidate_pool.
            #
            # Recovery does NOT relax any safety rule and never changes direction:
            # it simply re-runs the Brain/AI qualification against the latest closed
            # candles for unused candidates already discovered in this same cycle.
            if pass_no==5 and candidate_pool:
                prepared_keys=set()
                if isinstance(candidate,list):
                    for _item in candidate:
                        if isinstance(_item,dict) and _item.get("pair"):
                            prepared_keys.add((
                                _item.get("pair"),
                                str(_item.get("entry_candle_ts")),
                                str(_item.get("direction") or "").upper(),
                                str(_item.get("strategy") or "").upper(),
                                str(_item.get("self_strategy_version") or "")
                            ))
                recovery_seeds=[]
                for _key,_item in candidate_pool.items():
                    if not isinstance(_item,dict) or not _item.get("pair"):
                        continue
                    item_key=(
                        _item.get("pair"),
                        str(_item.get("entry_candle_ts")),
                        str(_item.get("direction") or "").upper(),
                        str(_item.get("strategy") or "").upper(),
                        str(_item.get("self_strategy_version") or "")
                    )
                    if item_key in prepared_keys:
                        continue
                    recovery_seeds.append(_item)
                # Recover only when the deep pool is narrower than two candidates.
                # This is an availability/resilience aid; the authoritative send
                # gate below remains unchanged.
                pass5_depth=len(prepared_keys)
                # Three final candidates can all fail the exact live-state
                # gate in the same boundary. Keep at least the configured tick-slot
                # count in the final candidate set by rescuing unused candidates
                # before the boundary.
                recovery_min_pool=max(2,int(ACCOUNT_TICK_PIN_SLOTS))
                if pass5_depth < recovery_min_pool and recovery_seeds:
                    recovery_remaining=max(0,signal_at-time.time())
                    if recovery_remaining>=10.0:
                        recovery_timeout=min(8.0,max(1.0,recovery_remaining-8.0))
                        log.info(
                            "FINAL_RECOVERY_NARROW_POOL cycle=%s pass5_candidates=%s "
                            "unused_seeds=%s timeout=%.2f seconds_to_signal=%.2f",
                            cycle_id,pass5_depth,len(recovery_seeds),recovery_timeout,
                            recovery_remaining
                        )
                        try:
                            log.info(
                                "FINAL_RECOVERY_AI_RETRY cycle=%s seeds=%s timeout=%.2f",
                                cycle_id,len(recovery_seeds),recovery_timeout
                            )
                            recovered=await asyncio.wait_for(
                                final_candidate(
                                    require_live_price=False,
                                    deep_analysis=True,
                                    use_cached_only=False,
                                    seed_candidates=recovery_seeds,
                                    return_ranked=True,
                                ),
                                timeout=recovery_timeout
                            )
                            recovered_list=recovered if isinstance(recovered,list) else (
                                [recovered] if isinstance(recovered,dict) else []
                            )
                            added=0
                            final_reference=time.time()
                            for raw_recovered in recovered_list[:100]:
                                if not isinstance(raw_recovered,dict) or not raw_recovered.get("pair"):
                                    continue
                                item=raw_recovered.copy()
                                item["expiry_minutes"]=1
                                item["qualified_pass"]=5
                                item["deep_verified"]=True
                                item["final_prepared_pass"]=5
                                item["final_delivery_precheck"]=_final_delivery_precheck(
                                    item,final_reference
                                )

                                # Recovery happens during pass 5, after the normal
                                # final-preparation loop has already run. Prepare the
                                # recovered candidate with the same authoritative
                                # closed-candle + strategy quality gate so it cannot
                                # arrive at the signal boundary with prepared_at=None.
                                recovered_pair=str(item.get("pair") or "")
                                recovered_expected=str(item.get("direction") or "").upper()
                                recovered_closed=_closed_candles(
                                    STATE["candles"].get(recovered_pair,[]),final_reference
                                )
                                recovery_reason=""
                                recovery_diag={}
                                try:
                                    recovery_mtf=build_multi_timeframe_context(
                                        recovered_pair,recovered_closed,final_reference,
                                        recovered_expected
                                    )
                                    recovery_confirmed,recovery_confirmation=multi_timeframe_confirmation(
                                        recovery_mtf,recovered_expected
                                    )
                                    recovery_diag=dict(recovery_confirmation or {})
                                    recovery_diag["2m_bars"]=len(
                                        _aggregate_closed_minutes(recovered_closed,2,final_reference)
                                    )
                                    if not recovery_confirmed:
                                        recovery_reason=str(
                                            recovery_confirmation.get("reason") or
                                            "multi_timeframe_rejected"
                                        )
                                except Exception as e:
                                    recovery_reason="multi_timeframe_preparation_error"
                                    recovery_diag={
                                        "type":type(e).__name__,
                                        "message":str(e)[:160]
                                    }

                                strategy_name=str(item.get("strategy") or "").upper()
                                body_ratio=float(item.get("body_ratio") or 0.0)
                                momentum_norm=float(item.get("momentum_norm") or 0.0)
                                efficiency=float(item.get("efficiency") or 0.0)
                                trend_name=str(item.get("trend_15m") or "").upper()
                                direction_name=str(item.get("direction") or "").upper()

                                if not recovery_reason and strategy_name=="BREAKOUT":
                                    breakout_distance=float(
                                        item.get(
                                            "breakout_distance_up"
                                            if direction_name=="UP"
                                            else "breakout_distance_down"
                                        ) or 0.0
                                    )
                                    if (
                                        breakout_distance<0.20
                                        or body_ratio<0.60
                                        or momentum_norm<0.35
                                        or efficiency<0.40
                                        or trend_name not in {"UP","DOWN"}
                                    ):
                                        recovery_reason="breakout_quality_insufficient"
                                        recovery_diag.update({
                                            "breakout_distance":round(breakout_distance,4),
                                            "body_ratio":round(body_ratio,3),
                                            "momentum_norm":round(momentum_norm,3),
                                            "efficiency":round(efficiency,3),
                                        })

                                if not recovery_reason and strategy_name=="PRICE_ACTION":
                                    structure_quality=float(item.get("structure_quality") or 0.0)
                                    aligned_recent=int(
                                        (item.get("evidence") or {}).get(
                                            "recent_aligned_candles",0
                                        ) or 0
                                    )
                                    pattern_name=str(item.get("pattern") or "").upper()
                                    rejection_ok=(
                                        (direction_name=="UP" and pattern_name=="BULLISH_REJECTION")
                                        or (direction_name=="DOWN" and pattern_name=="BEARISH_REJECTION")
                                    )
                                    near_level=(
                                        (direction_name=="UP" and float(item.get("price") or 0.0) <=
                                         float(item.get("support") or 0.0)+float(item.get("atr") or 0.0)*0.35)
                                        or
                                        (direction_name=="DOWN" and float(item.get("price") or 0.0) >=
                                         float(item.get("resistance") or 0.0)-float(item.get("atr") or 0.0)*0.35)
                                    )
                                    if (
                                        structure_quality<0.50
                                        or aligned_recent<2
                                        or momentum_norm<0.10
                                        or not (near_level or rejection_ok)
                                    ):
                                        recovery_reason="price_action_context_insufficient"
                                        recovery_diag.update({
                                            "structure_quality":round(structure_quality,3),
                                            "aligned_recent":aligned_recent,
                                            "momentum_norm":round(momentum_norm,3),
                                            "near_level":bool(near_level),
                                            "rejection":bool(rejection_ok),
                                        })

                                item["final_delivery_confirmed"]=not bool(recovery_reason)
                                item["final_delivery_confirmation_reason"]=recovery_reason or "confirmed"
                                item["final_delivery_diagnostic"]=recovery_diag
                                item["final_delivery_prepared_at"]=final_reference

                                if recovery_reason:
                                    log.info(
                                        "FINAL_RECOVERY_PREP_REJECTED cycle=%s pair=%s reason=%s diagnostic=%s",
                                        cycle_id,recovered_pair,recovery_reason,recovery_diag
                                    )
                                else:
                                    log.info(
                                        "FINAL_RECOVERY_PREP_CONFIRMED cycle=%s pair=%s diagnostic=%s seconds_to_signal=%.2f",
                                        cycle_id,recovered_pair,recovery_diag,
                                        max(0.0,signal_at-time.time())
                                    )

                                # Preserve the recovered technique identity too.
                                # Recovery is not allowed to overwrite another strategy
                                # for the same pair/candle/direction.
                                item_key=(
                                    item.get("pair"),
                                    str(item.get("entry_candle_ts")),
                                    str(item.get("direction") or "").upper(),
                                    str(item.get("strategy") or "").upper(),
                                    str(item.get("self_strategy_version") or "")
                                )
                                candidate_pool[item_key]=item
                                added+=1
                            if added:
                                rescue_items=sorted(
                                    candidate_pool.values(),
                                    key=lambda x:(
                                        float(x.get("final_delivery_precheck") or -900.0),
                                        int(x.get("confidence") or 0),
                                        float(x.get("strategy_margin") or 0),
                                        float(x.get("direction_agreement") or 0),
                                        float(x.get("market_quality") or 0)
                                    ),
                                    reverse=True
                                )[:ACCOUNT_TICK_PIN_SLOTS]
                                try:
                                    pin_ttl=max(30.0,target-time.time()+12.0)
                                    active_prep=await pin_account_tick_pairs(
                                        [x.get("pair") for x in rescue_items],ttl=pin_ttl
                                    )
                                    log.info(
                                        "FINAL_RECOVERY_READY cycle=%s added=%s pool=%s "
                                        "active=%s seconds_to_signal=%.2f",
                                        cycle_id,added,len(candidate_pool),active_prep,
                                        max(0,signal_at-time.time())
                                    )
                                except Exception as e:
                                    log.warning(
                                        "FINAL_RECOVERY_TICK_PIN_FAILED cycle=%s type=%s message=%s",
                                        cycle_id,type(e).__name__,str(e)[:120]
                                    )
                            else:
                                log.info(
                                    "FINAL_RECOVERY_NONE cycle=%s seeds=%s seconds_to_signal=%.2f",
                                    cycle_id,len(recovery_seeds),
                                    max(0,signal_at-time.time())
                                )
                        except asyncio.TimeoutError:
                            log.warning(
                                "FINAL_RECOVERY_TIMEOUT cycle=%s seeds=%s seconds_to_signal=%.2f",
                                cycle_id,len(recovery_seeds),max(0,signal_at-time.time())
                            )
                        except Exception as e:
                            log.warning(
                                "FINAL_RECOVERY_FAILED cycle=%s type=%s message=%s",
                                cycle_id,type(e).__name__,str(e)[:160]
                            )

            # When starting after a restart, compress the remaining missed
            # passes into the available pre-signal window. In normal operation
            # the exact scheduled timestamps above remain unchanged.
            # Persist each completed pass immediately. If Render replaces
            # this process, the next instance resumes from the next unfinished
            # pass and keeps the candidates already discovered.
            # Recovery metadata is best-effort; never block the signal scheduler on DB I/O.
            asyncio.create_task(save_cycle_state(
                cycle_id,target,signal_at,signal_lead,pass_no,
                candidate_pool,status="ACTIVE",
                reason=f"scan_{pass_no}_completed"
            ))
            if catchup_mode and pass_no<5:
                catchup_next_at=target-SCAN_OFFSETS[pass_no]
        # Final delivery prefers pass 5, but a pass-4 candidate that completed
        # the same deep Brain+AI review and tick preparation is an explicit safe
        # fallback. Earlier passes remain selection input only.
        await asyncio.sleep(max(0,signal_at-time.time()))
        sent=False

        final_candidates=[
            x for x in candidate_pool.values()
            if int(x.get("qualified_pass") or 0)>=4
            and bool(x.get("deep_verified"))
        ]
        now_boundary=time.time()
        # Pass 5 now runs 15 seconds before the signal boundary. It prepares the final
        # closed-candle gate ahead of time; the exact signal second is delivery-only.
        # This prevents a valid candidate from being rejected merely because local
        # final verification consumed a few hundred milliseconds at the boundary.
        # The cached precheck is ranking evidence only, never a hard
        # eligibility gate. The authoritative send_cycle_signal() gate below
        # rechecks fresh price, closed 30s/1m/2m candles, higher timeframes,
        # confidence, deadline, and deep qualification. A cached precheck can
        # become conservative/stale while the live candidate remains valid;
        # filtering it here was able to suppress an entire cycle before the
        # authoritative gate had a chance to try the fallback candidate.
        # Keep every deep-qualified technique in the boundary fallback pool.
        # Delivery itself remains authoritative: send_cycle_signal() rechecks the
        # final confirmation, fresh authenticated tick, confidence and deadline.
        # This prevents one late gate failure from deleting otherwise valid
        # techniques that were already researched/qualified in the same cycle.
        boundary_pool=list(final_candidates)
        _boundary_unique=[]
        _boundary_seen=set()
        for _x in sorted(
            boundary_pool,
            key=lambda x:(
                1 if bool(x.get("final_delivery_confirmed")) else 0,
                1 if has_fresh_live_price(x.get("pair"),now_boundary,LIVE_TICK_MAX_AGE) else 0,
                int(x.get("confidence") or 0),
                float(x.get("final_delivery_precheck") or -900.0),
                float(x.get("strategy_margin") or 0),
                float(x.get("direction_agreement") or 0),
            ),
            reverse=True
        ):
            _candidate_identity=(
                str(_x.get("pair") or ""),
                str(_x.get("entry_candle_ts")),
                str(_x.get("direction") or "").upper(),
                str(_x.get("strategy") or "").upper(),
                str(_x.get("self_strategy_version") or ""),
            )
            if not _candidate_identity[0] or _candidate_identity in _boundary_seen:
                continue
            _boundary_seen.add(_candidate_identity)
            _boundary_unique.append(_x)
        boundary_pool=_boundary_unique
        log.info(
            "FINAL_BOUNDARY_POOL cycle=%s candidates=%d prepared_confirmed=%d total_deep=%d unique_assets=%d fresh_now=%d",
            cycle_id,len(boundary_pool),
            sum(1 for x in boundary_pool if bool(x.get("final_delivery_confirmed"))),
            len(final_candidates),len(boundary_pool),
            sum(1 for x in boundary_pool if has_fresh_live_price(
                x.get("pair"),now_boundary,LIVE_TICK_MAX_AGE
            ))
        )
        ranked_pool=sorted(
            boundary_pool,
            key=lambda x:(
                # Fresh authenticated market data is more actionable at the exact
                # boundary than a candidate whose cached quote has already gone stale.
                # This is fallback ordering only; send_cycle_signal() keeps the
                # authoritative freshness, confidence, deep-gate and delivery checks.
                1 if has_fresh_live_price(x.get("pair"),now_boundary,LIVE_TICK_MAX_AGE) else 0,
                1 if bool(x.get("final_delivery_confirmed")) else 0,
                float(x.get("final_delivery_precheck") or -900.0),
                int(x.get("confidence") or 0),
                float(x.get("strategy_margin") or 0),
                float(x.get("direction_agreement") or 0),
                float(x.get("market_quality") or 0),
                float(x.get("learning_bonus") or 0)
            ),
            reverse=True
        )
        log.info(
            "PREFETCH_POOL_READY cycle=%s candidates=%d final_prepared_candidates=%d signal_lead=%ss",
            cycle_id,len(candidate_pool),len(ranked_pool),int(signal_lead)
        )

        # Do not perform broker subscription work at the exact boundary.
        # Wake 80ms early, then yield until signal_at so the final ranking is
        # already prepared without sacrificing the requested timing.
        early_wake=max(0.0,signal_at-0.08-time.time())
        if early_wake>0:
            await asyncio.sleep(early_wake)
        while time.time()<signal_at:
            await asyncio.sleep(0)
        boundary_lag=time.time()-signal_at
        if boundary_lag<=0.35:
            attempted_final=0
            for candidate in ranked_pool:
                attempted_final+=1
                log.info(
                    "FINAL_FALLBACK_ATTEMPT cycle=%s rank=%d/%d pair=%s fresh=%s",
                    cycle_id,attempted_final,len(ranked_pool),candidate.get("pair"),
                    has_fresh_live_price(candidate.get("pair"),time.time(),LIVE_TICK_MAX_AGE)
                )
                try:
                    log.info(
                        "FINAL_SIGNAL_ATTEMPT cycle=%s rank=%s pair=%s confidence=%s direction=%s",
                        cycle_id,attempted_final,candidate.get("pair"),
                        candidate.get("confidence"),candidate.get("direction")
                    )
                    sent=await send_cycle_signal(
                        candidate,target,signal_lead,cycle_id
                    )
                except Exception as e:
                    log.exception(
                        "FINAL_SIGNAL_BUILD_FAILED cycle=%s pair=%s type=%s message=%s",
                        cycle_id,candidate.get("pair"),
                        type(e).__name__,str(e)[:160]
                    )
                    sent=False
                if sent:
                    break

        if not sent:
            if boundary_lag>0.35:
                log.warning(
                    "FINAL_BOUNDARY_MISSED cycle=%s lag_seconds=%.3f candidates=%s max_lag=0.35",
                    cycle_id,boundary_lag,len(ranked_pool)
                )
            if ranked_pool:
                log.info(
                    "FIVE_SCAN_NO_VALID_SIGNAL cycle=%s candidates=%s "
                    "signal_utc=%s now_utc=%s",
                    cycle_id,len(ranked_pool),                    time.strftime("%H:%M:%S",time.gmtime(signal_at)),
                    time.strftime("%H:%M:%S",time.gmtime(time.time()))
                )
            else:
                log.info(
                    "FIVE_SCAN_NO_QUALIFIED_SETUP cycle=%s scans=5 analyzed=%d",
                    cycle_id,len(STATE["analyses"])
                )

        # Do not let the current cycle's durable DB status update block
        # the 24/7 scheduler after a signal/no-signal decision. The next
        # 3-minute target must be scheduled immediately; persistence runs
        # independently and never owns the scan loop.
        if sent:
            asyncio.create_task(
                mark_cycle_state(cycle_id,"SENT","signal_delivered")
            )
            cycle_outcome="SENT"
        elif ranked_pool:
            asyncio.create_task(
                mark_cycle_state(cycle_id,"SKIPPED","final_candidate_failed_delivery")
            )
            cycle_outcome="SKIPPED_DELIVERY"
        else:
            asyncio.create_task(
                mark_cycle_state(cycle_id,"SKIPPED","no_final_pass_candidate")
            )
            cycle_outcome="SKIPPED_NO_SETUP"

        next_target=target+SIGNAL_INTERVAL
        next_cycle_id=int(next_target//SIGNAL_INTERVAL)
        next_signal=next_target-SIGNAL_LEADS[0 if (next_cycle_id%20 or 20)<=10 else 1]
        log.info(
            "CYCLE_CONTINUE_AFTER_SIGNAL cycle=%s outcome=%s next_cycle=%s "
            "next_signal_utc=%s next_target_utc=%s",
            cycle_id,cycle_outcome,next_cycle_id,
            time.strftime("%H:%M:%S",time.gmtime(next_signal)),
            time.strftime("%H:%M:%S",time.gmtime(next_target))
        )
        await asyncio.sleep(0)

        # Immediately iterate to the next 3-minute target. The first scan
        # begins 150s before that target, preserving the fixed 30s delivery lead.

async def learning_practice_supervisor():
    """Run the DEMO-only learning engine with explicit task-state diagnostics."""
    while True:
        try:
            runner=getattr(learning_lab_module,"run_forever",None)
            log.info(
                "LEARNING_PRACTICE_TASK_START module=%s runner=%s coroutine=%s",
                getattr(learning_lab_module,"__name__",""),
                getattr(runner,"__qualname__",""),
                asyncio.iscoroutinefunction(runner),
            )
            if not asyncio.iscoroutinefunction(runner):
                raise RuntimeError("learning_lab.run_forever is not an async coroutine")
            task=asyncio.create_task(
                runner(),
                name="learning_practice_engine",
            )
            await asyncio.sleep(0)
            log.info(
                "LEARNING_PRACTICE_TASK_CREATED done=%s cancelled=%s",
                task.done(),task.cancelled()
            )
            await task
            log.warning("LEARNING_PRACTICE_TASK_RETURNED reason=loop_exited")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception(
                "LEARNING_PRACTICE_TASK_RESTART type=%s message=%s",
                type(e).__name__, str(e)[:180]
            )
        await asyncio.sleep(2.0)

async def cycle_loop_supervisor():
    """Keep the scheduler alive if cycle_loop exits unexpectedly."""
    while True:
        task = asyncio.create_task(cycle_loop())
        try:
            await task
            log.warning(
                "CYCLE_LOOP_SUPERVISOR_RESTART reason=cycle_loop_returned"
            )
        except asyncio.CancelledError:
            task.cancel()
            raise
        except Exception as e:
            log.exception(
                "CYCLE_LOOP_SUPERVISOR_RESTART type=%s message=%s",
                type(e).__name__, str(e)[:180]
            )
        await asyncio.sleep(2.0)

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

async def _account_trade_record(item,event_code=None,source="broker_event"):
    """Normalize broker order/position payloads for read-only live visibility."""
    if not isinstance(item,dict):
        return None
    tid=str(item.get("id") or item.get("trade_id") or item.get("order_id") or item.get("orderId") or "").strip()
    if not tid:
        return None
    rec=dict(item)
    rec["_trade_id"]=tid
    rec["_event"]=int(event_code) if event_code is not None else None
    rec["_source"]=str(source)
    rec["_received_at"]=time.time()
    # Never copy credential/session fields into the live dashboard.
    for k in ("access_token","token","password","cookie","cookies","authorization"):
        rec.pop(k,None)
    STATE["live_orders"][tid]=rec
    events=list(STATE.get("live_order_events") or [])
    events.append({
        "trade_id":tid,
        "event":int(event_code) if event_code is not None else None,
        "source":str(source),
        "received_at":rec["_received_at"],
        "status":str(rec.get("status") or rec.get("state") or "").upper(),
        "pair":str(rec.get("pair") or rec.get("p") or rec.get("symbol") or ""),
        "direction":str(rec.get("direction") or rec.get("dir") or "").upper(),
        "amount":rec.get("amount"),
        "profit":rec.get("profit") if rec.get("profit") is not None else rec.get("pnl"),
        "price":rec.get("price") if rec.get("price") is not None else rec.get("open_price"),
        "expiry_price":rec.get("expiry_price") if rec.get("expiry_price") is not None else rec.get("close_price"),
        "is_flex":bool(rec.get("is_flex",rec.get("flex",False))),
    })
    STATE["live_order_events"]=events[-80:]
    return rec

async def on_account_trade_update(message):
    """Capture unsolicited order/position lifecycle events without placing trades."""
    try:
        event_code=message.get("e") if isinstance(message,dict) else None
        payload=message.get("d") if isinstance(message,dict) else None
        items=payload if isinstance(payload,list) else [payload]
        added=0
        for item in items:
            if isinstance(item,dict) and await _account_trade_record(item,event_code,"broker_event"):
                added+=1
        if added:
            log.info(
                "ACCOUNT_ORDER_EVENT_LIVE event=%s records=%s live_orders=%s",
                event_code,added,len(STATE.get("live_orders") or {})
            )
    except Exception as e:
        log.warning(
            "ACCOUNT_ORDER_EVENT_CAPTURE_FAILED type=%s message=%s",
            type(e).__name__,str(e)[:160]
        )

async def account_order_position_worker():
    """Poll the authenticated Demo account's open-order list for manual trades."""
    while True:
        try:
            client=CLIENT
            account_id=STATE.get("account_id")
            monitor=STATE.setdefault("live_order_monitor",{})
            monitor["connected"]=bool(
                client and getattr(getattr(client,"connection",None),"is_connected",False)
            )
            if client and monitor["connected"] and account_id:
                monitor["last_query_at"]=time.time()
                try:
                    rows=await asyncio.wait_for(
                        client.trade.get_open_trades(int(account_id),group="demo"),
                        timeout=2.5
                    )
                    records=[x for x in (rows or []) if isinstance(x,dict)] if isinstance(rows,list) else []
                    open_ids=set()
                    for item in records:
                        tid=str(item.get("id") or item.get("trade_id") or item.get("order_id") or item.get("orderId") or "").strip()
                        if not tid:
                            continue
                        open_ids.add(tid)
                        await _account_trade_record(item,31,"broker_event31_open_poll")
                    for tid,rec in list((STATE.get("live_orders") or {}).items()):
                        status=str(rec.get("status") or rec.get("state") or "").upper()
                        if tid not in open_ids and status not in {"CLOSED","EXPIRED","SETTLED","FINISHED","DONE","LOSS","WIN","TIE"}:
                            rec["_open_missing_at"]=time.time()
                    monitor["last_error"]=None
                    monitor["updated_at"]=time.time()
                    log.info(
                        "ACCOUNT_ORDER_POSITION_LIVE open_items=%s tracked=%s account_id=%s source=event31_poll+event21_22_26",
                        len(records),len(STATE.get("live_orders") or {}),account_id
                    )
                except Exception as e:
                    monitor["last_error"]=f"{type(e).__name__}: {str(e)[:140]}"
                    monitor["updated_at"]=time.time()
                    log.info(
                        "ACCOUNT_ORDER_POSITION_QUERY_FAILED account_id=%s type=%s message=%s",
                        account_id,type(e).__name__,str(e)[:140]
                    )
            await asyncio.sleep(1.5)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning(
                "ACCOUNT_ORDER_POSITION_WORKER_FAILED type=%s message=%s",
                type(e).__name__,str(e)[:160]
            )
            await asyncio.sleep(1.5)

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
        # Demo-learning trade events are isolated from live signal/result state.
        # They exist only for automatic DEMO learning practice orders.
        client.register_callback(parameters.E_TRADE_ACCEPTED,on_learning_trade_update)
        client.register_callback(parameters.E_TRADE_CLOSED,on_learning_trade_update)
        # Read-only live visibility for manual account orders/positions.
        # These callbacks never create, modify, or close a trade.
        client.register_callback(parameters.E_TRADE_UPDATE_INTERIM,on_account_trade_update)
        client.register_callback(parameters.E_TRADE_ACCEPTED,on_account_trade_update)
        client.register_callback(parameters.E_TRADE_CLOSED,on_account_trade_update)
        live_quote_task=None
        order_position_task=None
        try:
            STATE["status"]="connecting"
            # Let the websocket/auth handshake settle before the first
            # application-level subscription. A deployment/re-authentication
            # can otherwise drop the socket in the small window between
            # websocket establishment and session initialization.
            await client.start()
            await asyncio.sleep(1.0)
            if not client.connection.is_connected:
                raise ConnectionError("WebSocket dropped during startup stabilization")
            STATE["status"]="connected"
            await client.initialize_session()
            if not client.connection.is_connected:
                raise ConnectionError("WebSocket dropped during session initialization")

            # IMPORTANT: e:55 is the balance/account-set feed, but its
            # account_id fields are not the only identity fields used by the
            # authenticated session. In the current broker session the library
            # preserves the explicitly requested account_id and separately
            # reports the e:55 balance accounts. The previous app-level guard
            # incorrectly treated absence from e:55.account_id as proof that
            # the token belonged to another account. That could reject a valid
            # authenticated session before the static asset catalog was loaded.
            event55_accounts=[]
            for msg in client.get_cached_events(parameters.E_BALANCE_UPDATE):
                event55_accounts.extend(extract_account_ids(msg,"demo"))
            if not event55_accounts:
                event55_accounts.extend(extract_account_ids(client.current_balance,"demo"))
            demo_accounts=sorted(set(event55_accounts))
            log.info(
                "TOKEN_DEMO_BALANCE_ACCOUNTS source=event55 count=%d ids=%s",
                len(demo_accounts),demo_accounts
            )

            # The configured OlympTrade ID can be either the trading-account
            # ID or the authenticated user/profile ID. The current session
            # exposes the configured value as e:110.id while e:55 carries the
            # actual demo trading-account ID. Resolve that relationship instead
            # of treating it as a token mismatch.
            selected_account_id=client.account_id
            try:
                selected_account_id=int(selected_account_id) if selected_account_id is not None else None
            except (TypeError,ValueError):
                selected_account_id=None

            if selected_account_id != expected_account_id or str(client.account_group or "").lower() != "demo":
                STATE["account_id"]=None
                STATE["account_group"]="demo"
                STATE["status"]="account_identity_unresolved"
                log.error(
                    "TOKEN_ACCOUNT_SELECTION_FAILED expected_id=%s selected_id=%s selected_group=%s balance_accounts=%s",
                    expected_account_id,selected_account_id,client.account_group,demo_accounts
                )
                raise RuntimeError(
                    f"Authenticated session did not preserve configured identity {expected_account_id}"
                )

            resolved_account_id=expected_account_id
            identity_match=False
            user_identity_hits=[]
            for msg in client.get_cached_events(parameters.E_USER_INFO):
                data=msg.get("d") if isinstance(msg,dict) else None
                records=data if isinstance(data,list) else [data]
                for rec in records:
                    if not isinstance(rec,dict):
                        continue
                    for key in ("id","user_id","userId","uid"):
                        value=rec.get(key)
                        try:
                            if value is not None and int(value)==expected_account_id:
                                identity_match=True
                                user_identity_hits.append(key)
                        except (TypeError,ValueError):
                            pass

            if expected_account_id not in demo_accounts and identity_match:
                # e:110 confirms the configured ID belongs to this authenticated
                # session, while e:55 provides the real demo trading account.
                # Only resolve automatically when exactly one demo account is
                # exposed; this prevents choosing an arbitrary account.
                if len(demo_accounts)==1:
                    resolved_account_id=demo_accounts[0]
                    log.info(
                        "ACCOUNT_ID_RESOLVED profile_id=%s demo_account_id=%s source=e110_identity_plus_e55_demo",
                        expected_account_id,resolved_account_id
                    )
                else:
                    raise RuntimeError(
                        f"Authenticated identity {expected_account_id} maps to multiple demo accounts {demo_accounts}"
                    )
            elif expected_account_id not in demo_accounts:
                log.error(
                    "TOKEN_ID_NOT_FOUND expected_id=%s e110_identity_match=%s demo_accounts=%s",
                    expected_account_id,identity_match,demo_accounts
                )
                raise RuntimeError(
                    f"Configured identity {expected_account_id} is neither a demo account nor the authenticated user identity"
                )

            client.account_id=resolved_account_id
            client.account_group="demo"
            STATE["account_id"]=resolved_account_id
            STATE["account_group"]="demo"
            STATE["status"]="authenticated_account_selected"
            BRAIN.bind_learning_account(resolved_account_id)
            log.info(
                "DEMO_ACCOUNT_SELECTED configured_id=%s account_id=%s group=demo source=authenticated_session",
                expected_account_id,client.account_id
            )

            if not await sync_account_assets(client,reason="initial"):
                STATE["status"]="static_asset_catalog_failed"
                log.error(
                    "STATIC_ACCOUNT_ASSET_CATALOG_VALIDATION_FAILED configured_id=%s account_id=%s balance_accounts=%s",
                    expected_account_id,client.account_id,demo_accounts
                )
                raise RuntimeError(
                    f"Static user asset catalog could not be loaded for {client.account_id}"
                )
            log.info(
                "STATIC_ACCOUNT_ASSET_CATALOG_READY configured_id=%s account_id=%s source=user_pdf_104_assets asset_count=%d api_asset_listing=off",
                expected_account_id,client.account_id,len(STATE["assets"])
            )
            STATE["status"]="live_read_only"
            assets=list(STATE["assets"])
            known_closed=0
            for asset in assets:
                pair=str(asset.get("pair") or "").strip()
                closure_reason=known_broker_closure_reason(pair)
                if closure_reason:
                    asset["broker_schedule_blocked"]=True
                    asset["broker_closed_reason"]=closure_reason
                    asset["signal_eligible"]=False
                    asset["broker_tradeable"]=False
                    known_closed+=1
                    STATE.get("analyses",{}).pop(pair,None)
            log.info(
                "KNOWN_BROKER_CLOSURE_APPLIED source=terminal_observation blocked=%d assets=%d",
                known_closed,len(assets)
            )
            real_n=sum(a["mode"]=="REAL" for a in assets); otc_n=sum(a["mode"]=="OTC" for a in assets)
            log.info("STATIC_ACCOUNT_ASSET_SOURCE account_id=%s catalog_count=%d static_real=%d static_otc=%d static_total=%d",
                     client.account_id,len(assets),real_n,otc_n,len(assets))
            log.info("STATIC_ACCOUNT_ASSETS_READY count=%d source=user_pdf_104_assets api_asset_listing=off",len(assets))
            await ensure_account_tick_subscriptions()
            restored=await restore_pending_result_watches()
            if restored:
                log.info("RESULT_WATCH_RECOVERY_COMPLETE restored=%d",restored)
            # Individual event-12 subscriptions are managed by the dedicated
            # read-only worker below. Event-1 ticks are preferred when delivered;
            # the snapshot scanner remains a timestamped fallback for assets
            # that do not emit an event-1 tick.
            log.info("TICK_SUBSCRIPTION_MODE authenticated_event1 preferred; broker_current_candle_fallback=enabled; asset_inventory_source=user_pdf_104_assets; no asset-list API")
            await refresh_candles(force=True)
            live_quote_task=asyncio.create_task(account_live_quote_worker(),name="account_live_quote_worker")
            order_position_task=asyncio.create_task(account_order_position_worker(),name="account_order_position_worker")
            STATE["live_order_monitor"]["connected"]=True
            log.info("ACCOUNT_BRAIN_FEED_READY source=authenticated_session asset_universe=account_event_182 live_quote=broker_current_candle tick_preferred=true order_position_monitor=live")
            last_asset_sync=time.time()
            while True:
                await asyncio.sleep(15)
                # If the authenticated websocket drops after startup, restart the
                # market worker instead of keeping a dead client alive and repeatedly
                # issuing "Not connected" market requests. The outer retry path
                # recreates the client and re-establishes the demo session.
                if not getattr(client.connection,"is_connected",False):
                    STATE["status"]="reconnecting"
                    raise ConnectionError("WebSocket disconnected during runtime")
                now_sync=time.time()
                if now_sync-last_asset_sync>=60.0:
                    if await sync_account_assets(client,reason="periodic"):
                        last_asset_sync=now_sync
                    else:
                        log.warning("ACCOUNT_ASSET_SYNC_RETAINED count=%d",len(STATE["assets"]))
        except Exception as e:
            STATE["status"]=("account_token_mismatch" if "Access token does not expose configured demo account" in str(e) else "error")
            log.exception("MARKET_WORKER_ERROR %s",e)
            # Connection startup failures are retried quickly so a transient
            # broker websocket drop cannot suppress the next signal cycle.
            retry_delay=4 if (
                "Not connected" in str(e)
                or "WebSocket dropped" in str(e)
                or "WebSocket disconnected" in str(e)
            ) else 30
            await asyncio.sleep(retry_delay)
        finally:
            if live_quote_task is not None:
                try:
                    live_quote_task.cancel()
                    await live_quote_task
                except (Exception, asyncio.CancelledError):
                    pass
            if order_position_task is not None:
                try:
                    order_position_task.cancel()
                    await order_position_task
                except (Exception, asyncio.CancelledError):
                    pass
            STATE["live_order_monitor"]["connected"]=False
            try:await client.stop()
            except Exception:pass
            CLIENT=None

def learning_market_snapshot():
    # Read-only snapshot for the isolated 2-hour learning lab. It never exposes
    # mutable live signal structures and never performs broker I/O.
    return {
        "assets":[dict(a) for a in STATE.get("assets") or []],
        "candles":{str(p):list(v)[-120:] for p,v in (STATE.get("candles") or {}).items()},
        "prices":{str(p):tuple(v) for p,v in (STATE.get("prices") or {}).items()},
    }

async def on_learning_trade_update(message):
    try:
        asyncio.create_task(handle_learning_trade_update(message))
    except Exception as e:
        log.warning("LEARNING_TRADE_EVENT_DISPATCH_FAILED type=%s message=%s",
                    type(e).__name__,str(e)[:120])

async def telegram_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg=update.message
    if not msg: return
    uid=msg.from_user.id if msg.from_user else None
    if uid is not None and str(uid)==ADMIN_TELEGRAM_ID:
        await msg.reply_text("✅ NEXORA-AI Admin online. Use /mbcode to generate a 24-hour member code.")
    elif uid is not None and await member_access_active(uid):
        await msg.reply_text("✅ Access active. Signal + Result delivery is enabled.")
    else:
        await msg.reply_text("🔒 Member access is not active. Contact the administrator.")

async def handle_telegram_command(msg):
    if not msg: return
    uid=msg.get("from",{}).get("id"); chat_id=(msg.get("chat") or {}).get("id")
    txt=str(msg.get("text") or "").strip()
    if uid is None or chat_id is None: return
    cmd=txt.split()[0].lower() if txt else ""
    if cmd=="/adminverify":
        parts=txt.split(maxsplit=1); code=parts[1].strip() if len(parts)==2 else ""
        if str(uid)!=ADMIN_TELEGRAM_ID:
            await audit_access("UNAUTHORIZED_ADMIN_VERIFY_ATTEMPT",int(uid)); await telegram("❌ Admin only.",chat_id=chat_id); return
        if ADMIN_LIFETIME_CODE and code and secrets.compare_digest(code,ADMIN_LIFETIME_CODE):
            snap=BRAIN.start_telegram_evaluation()
            await save_persistent_learning()
            log.info(
                "ADMIN_LIFETIME_VERIFIED evaluation_started=True telegram_eval_signals=%s",
                snap.get("signals",0)
            )
            await telegram(
                "✅ LIFETIME ADMIN ACCESS VERIFIED.\n\n"
                "📊 100-SIGNAL TELEGRAM EVALUATION STARTED\n"
                "🔢 Count starts from the next successfully delivered Telegram signal.\n"
                "🧹 All previous signals are excluded.\n"
                "📈 10-signal updates are OFF.\n"
                "🤖 DEMO learning, Brain, research and result watching continue normally.\n\n"
                "🎯 Progress → 0/100",
                chat_id=chat_id
            )
        else:
            await telegram("❌ Invalid lifetime admin code.",chat_id=chat_id)
        return
    if cmd=="/mbcode":
        if str(uid)!=ADMIN_TELEGRAM_ID:
            await audit_access("UNAUTHORIZED_MBCODE_ATTEMPT",int(uid)); await telegram("❌ Admin only.",chat_id=chat_id); return
        ok=await generate_member_access_code(uid)
        await telegram(("✅ Code generated and sent to your admin email.\n\n📩 Open your admin email and copy the code.\n\n🔐 Then send:\n/mbverify CODE\n\nExample: /mbverify 123456") if ok else "❌ Code generation failed.",chat_id=chat_id); return
    if cmd in ("/mbaccess","/mbverify"):
        parts=txt.split(maxsplit=1); code=parts[1].strip() if len(parts)==2 else ""
        result=await grant_member_access(uid,code)
        messages={"GRANTED":"✅ ACCESS VERIFICATION SUCCESS\n\nAll configured MB members now have access for 24 hours.","DENIED":"❌ Admin only.","INVALID":"❌ Invalid access code.","EXPIRED":"❌ Code expired or no active code.","NO_MEMBERS":"❌ No MB1/MB2/... member IDs configured.","DB_ERROR":"❌ Access system unavailable."}
        await telegram(messages.get(result,"❌ Access verification failed."),chat_id=chat_id); return
    if cmd=="/start":
        await telegram("🔒 NEXORA-AI\n\nAccess is controlled by the administrator.",chat_id=chat_id)
        return




def format_live_indicator_check(indicators, price_source="authenticated_broker_live_candle"):
    d=dict(indicators or {})
    av=float(d.get("anchored_vwap") or 0.0)
    poc=float(d.get("volume_profile_poc") or 0.0)
    vah=float(d.get("volume_profile_vah") or 0.0)
    val=float(d.get("volume_profile_val") or 0.0)
    slope=float(d.get("avwap_slope") or 0.0)
    acceptance=bool(d.get("value_area_acceptance"))
    return (
        "🔎 <b>LIVE INDICATOR CHECK</b>\n"
        f"📡 Feed → {price_source}\n"
        "🕯️ Data → closed M1 + completed 15M anchor\n"
        f"📊 Anchored VWAP → {av:.8f}\n"
        f"📍 Volume Profile POC → {poc:.8f}\n"
        f"🔺 VAH → {vah:.8f} • 🔻 VAL → {val:.8f}\n"
        f"↗️ AVWAP Slope → {slope:.8f}\n"
        f"✅ Value Area Acceptance → {'YES' if acceptance else 'NO'}\n"
    )

def format_candice_signal_message(s, indicator_check, ts, target):
    """Canonical Telegram signal template.

    The live engine is locked to AVWAP + Volume Profile. The formatter therefore
    uses a fixed engine label and never prints a legacy strategy name.
    """
    direction = str(s.direction or "").upper()
    arrow = "🟢 UP" if direction == "UP" else "🔴 DOWN"
    trend = str(s.trend_15m or "").upper()
    trend_label = "BULLISH" if "BULL" in trend else "BEARISH" if "BEAR" in trend else "—"
    structure = str(s.structure_1m or "").upper()
    structure_label = (
        "ABOVE AVWAP + POC" if "ABOVE_AVWAP_POC" in structure
        else "BELOW AVWAP + POC" if "BELOW_AVWAP_POC" in structure
        else "—"
    )
    # Telegram HTML has no font-size control, so use Unicode bold glyphs
    # and isolated lines to make the direction and entry time visually dominant.
    direction_focus = "🟢 𝗨𝗣" if direction == "UP" else "🔴 𝗗𝗢𝗪𝗡"
    bold_digits = str.maketrans("0123456789", "𝟬𝟭𝟮𝟯𝟰𝟱𝟲𝟳𝟴𝟵")
    entry_time = uae_time(target).translate(bold_digits)
    return (
        "🚨 <b>CANDICE AI</b>\n"
        "💎 <b>PRO SIGNAL</b>\n\n"
        f"📊 <b>{s.display_name}</b>\n\n"
        f"<b>{direction_focus}</b>\n"
        "⏱️ <b>𝟭 𝗠𝗜𝗡</b>\n\n"
        f"🎯 <b>𝗘𝗡𝗧𝗥𝗬</b>\n"
        f"<b>{entry_time} UAE</b>\n\n"
        f"💰 Reference → <code>{s.entry_price}</code>\n"
        f"⚡ Confidence → <b>{s.confidence}%</b>\n\n"
        f"📈 15M Bias → <b>{trend_label}</b>\n"
        f"🕯️ 1M Close → <b>{structure_label}</b>\n"
        "🧠 Engine → <b>AVWAP + VOLUME PROFILE</b>\n"
        "✓ Confluence → <b>AVWAP + POC ALIGNED</b>\n\n"
        "🟣 <b>DEMO • MANUAL ENTRY</b>\n"
        "🤖 <b>CANDICE BRAIN • LIVE</b>"
    )



async def build_live_analysis_payload():
    rows=[]
    for asset in list(STATE.get("assets") or []):
        pair=str(asset.get("pair") or "")
        an=STATE.get("analyses",{}).get(pair) or {}
        pr=STATE.get("prices",{}).get(pair)
        rows.append({
            "pair":pair,
            "display_name":str(asset.get("display_name") or asset.get("title") or pair),
            "live_price":pr[0] if pr else an.get("price"),
            "live_price_source":STATE.get("price_source",{}).get(pair,"none"),
            "strategy":an.get("strategy"),
            "direction":an.get("direction"),
            "confidence":an.get("confidence"),
            "trend_15m":an.get("trend_15m"),
            "structure_1m":an.get("structure_1m"),
            "pattern":an.get("pattern"),
            "strategy_candidates":list(an.get("strategy_candidates") or []),
            "strategy_audit":list(an.get("strategy_audit") or []),
            "strategy_audit_count":int(an.get("strategy_audit_count") or 0),
            "indicator_audit_scope":an.get("indicator_audit_scope") or "AVWAP_VOLUME_PROFILE_ONLY",
            "indicators":dict(an.get("indicators") or {}),
        })
    return {
        "service":"CANDICE-AI",
        "mode":"LIVE_SIGNAL",
        "signal_read_only":True,
        "demo_auto_trade":False,
        "authenticated_account_feed":str(STATE.get("feed_source") or "").startswith("authenticated_websocket:"),
        "asset_count":len(rows),
        "qualified_count":len(STATE.get("analyses") or {}),
        "strategy_families_checked":1,
        "strategy_families":["AVWAP_VOLUME_PROFILE"],
        "five_scan_cycle":dict(STATE.get("cycle_scan_status") or {}),
        "protocol":"FLEX_MANUAL",
        "signal_expiry_minutes":1,
        "flex_auto_trade":False,
        "order_position_monitor":dict(STATE.get("live_order_monitor") or {}),
        "live_orders":[dict(v) for v in (STATE.get("live_orders") or {}).values()],
        "live_order_events":list(STATE.get("live_order_events") or [])[-40:],
        "updated_utc":datetime.now(timezone.utc).isoformat(),
        "assets":rows,
    }


async def account_setups_page():
    """Read-only server-rendered live view; no browser JavaScript required."""
    d=await build_live_analysis_payload()
    def esc(v):
        import html as _html
        return _html.escape(str(v if v is not None else "—"))

    css="""*{box-sizing:border-box}body{margin:0;background:#070b0f;color:#eef4f6;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
.wrap{max-width:760px;margin:auto;padding:14px}.top{background:#070b0f;padding:4px 0 12px}
h1{font-size:22px;margin:8px 0 4px}.sub{opacity:.72;font-size:12px}.grid{display:grid;grid-template-columns:repeat(2,1fr);gap:8px;margin:12px 0}
.card,.setup{background:#10161c;border:1px solid #26323b;border-radius:14px;padding:12px}.kpi{font-size:20px;font-weight:800}
.label{font-size:11px;opacity:.65;text-transform:uppercase}.setup{margin:8px 0}.row{display:flex;justify-content:space-between;gap:10px;align-items:center}
.assetname{font-size:16px;font-weight:800}.up{color:#45ef8a;font-weight:800}.down{color:#ff6d7a;font-weight:800}
.meta{font-size:11px;opacity:.75;line-height:1.55}details{margin-top:8px}summary{cursor:pointer;font-weight:700;font-size:12px}
pre{white-space:pre-wrap;word-break:break-word;font-size:11px;opacity:.85}.empty{opacity:.65;text-align:center;padding:30px 10px}
.dot{width:8px;height:8px;border-radius:50%;display:inline-block;background:#45ef8a;margin-right:6px}.small{font-size:11px;opacity:.75}a{color:#8fc7ff}"""

    c=d.get("five_scan_cycle") or {}
    monitor=d.get("order_position_monitor") or {}
    orders=d.get("live_orders") or []
    events=d.get("live_order_events") or []

    setup_rows=[]
    for asset in d.get("assets") or []:
        candidates=asset.get("strategy_candidates") or []
        # Show every live candidate, plus the asset itself when only a single
        # selected strategy is present.
        if candidates:
            for cand in candidates:
                setup_rows.append((asset,cand))
        elif asset.get("strategy"):
            setup_rows.append((asset,{
                "strategy":asset.get("strategy"),
                "direction":asset.get("direction"),
                "score":asset.get("confidence")
            }))

    cards=[]
    for idx,(asset,cand) in enumerate(sorted(
        setup_rows,key=lambda z:float(z[1].get("score") or 0),reverse=True
    ),1):
        direction=str(cand.get("direction") or asset.get("direction") or "—").upper()
        dc="up" if direction=="UP" else "down"
        audits=asset.get("strategy_audit") or []
        audit_html="<br>".join(
            esc(a.get("strategy"))+" → UP "+esc(a.get("up_score"))+
            " / DOWN "+esc(a.get("down_score"))+
            (" • QUALIFIED" if a.get("up_qualified") or a.get("down_qualified") else "")
            for a in audits
        ) or "No strategy audit available"
        ind=asset.get("indicators") or {}
        ind_text="\n".join([
            "Anchored VWAP: %s"%esc(ind.get("anchored_vwap")),
            "Volume Profile POC: %s"%esc(ind.get("volume_profile_poc")),
            "VAH / VAL: %s / %s"%(esc(ind.get("volume_profile_vah")),esc(ind.get("volume_profile_val"))),
            "AVWAP slope: %s"%esc(ind.get("avwap_slope")),
            "Slope persistence: %s"%esc(ind.get("slope_persistent")),
            "POC migration: %s"%esc(ind.get("profile_poc_migration_norm")),
            "Value position: %s"%esc(ind.get("value_position")),
            "Value acceptance: %s"%("YES" if ind.get("value_area_acceptance") else "NO"),
            "Level reclaim: %s"%("YES" if ind.get("level_reclaim") else "NO"),
            "Volume quality: %s (%s)"%(esc(ind.get("volume_quality")),esc(ind.get("volume_mode"))),
        ])
        cards.append(
            '<div class="setup"><div class="row"><div><div class="assetname">%d. %s</div>'
            '<div class="meta">%s • Live %s • %s</div></div>'
            '<div class="%s">%s</div></div>'
            '<div class="meta">🎯 %s • Confidence %s%% • 15m %s • 1m %s</div>'
            '<details><summary>📊 Full live indicators</summary><pre>%s</pre></details>'
            '<details><summary>🧠 Active engine check</summary><pre>%s</pre></details></div>'
            % (idx,esc(asset.get("display_name") or asset.get("pair")),
               esc(asset.get("pair")),esc(asset.get("live_price")),esc(asset.get("live_price_source")),
               dc,esc(direction),esc(cand.get("strategy") or asset.get("strategy")),
               esc(cand.get("score") if cand.get("score") is not None else asset.get("confidence")),
               esc(asset.get("trend_15m")),esc(asset.get("structure_1m")),
               esc(ind_text),audit_html)
        )

    if orders:
        order_html=""
        for o in orders[:12]:
            direction=str(o.get("direction") or o.get("dir") or "—").upper()
            dc="up" if direction=="UP" else "down"
            status=str(o.get("status") or o.get("state") or "OPEN").upper()
            entry=o.get("price")
            if entry is None: entry=o.get("open_price",o.get("entry_price"))
            exitp=o.get("expiry_price")
            if exitp is None: exitp=o.get("close_price",o.get("exit_price"))
            profit=o.get("profit")
            if profit is None: profit=o.get("pnl")
            order_html += (
                '<div class="setup"><div class="row"><div class="assetname">%s</div>'
                '<div class="%s">%s</div></div>'
                '<div class="meta">ID %s • %s • %s • $%s</div>'
                '<div class="meta">Entry %s • Live/Expiry %s • P/L %s</div>'
                '<div class="meta">Source %s</div></div>'
                % (esc(o.get("pair") or o.get("symbol") or "Unknown"),dc,esc(direction),
                   esc(o.get("_trade_id") or o.get("id")),esc(status),
                   "FLEX" if o.get("is_flex") else "ORDER",esc(o.get("amount")),
                   esc(entry),esc(exitp),esc(profit),esc(o.get("_source") or "broker"))
            )
    else:
        order_html='<div class="small">No broker-side open/manual position detected at this moment.</div>'

    if events:
        event_lines=[]
        for e in list(events)[-16:][::-1]:
            try:
                tt=datetime.fromtimestamp(float(e.get("received_at") or 0),tz=timezone.utc).astimezone().strftime("%H:%M:%S")
            except Exception:
                tt="—"
            event_lines.append(
                "%s • e:%s • %s • %s • %s • P/L %s" %
                (tt,esc(e.get("event")),esc(e.get("pair")),esc(e.get("direction")),
                 esc(e.get("status")),esc(e.get("profit")))
            )
        event_html="<pre>%s</pre>"%"\\n".join(event_lines)
    else:
        event_html='<div class="small">Waiting for broker Event 21 / 22 / 26 / 31…</div>'

    body="""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta http-equiv="refresh" content="3">
<title>CANDICE • MY DEMO ACCOUNT</title><style>%s</style></head><body>
<div class="wrap"><div class="top">
<h1>🧠 CANDICE • MY DEMO ACCOUNT</h1>
<div class="sub"><span class="dot"></span>Authenticated live data • READ ONLY • Flex trade is manual</div>
<div class="small">Updated %s • Feed %s • Order monitor %s</div></div>
<div class="grid">
<div class="card"><div class="label">Account Assets</div><div class="kpi">%s</div></div>
<div class="card"><div class="label">Detected Setups</div><div class="kpi">%s</div></div>
<div class="card"><div class="label">Active Engine</div><div class="kpi">AVWAP + VP</div></div>
<div class="card"><div class="label">Signal Expiry</div><div class="kpi">1 MIN</div></div></div>
<div class="card"><b>🔎 5-SCAN CYCLE</b><br>Cycle: %s • Pass: %s/5<br>Protocol: <b>FLEX MANUAL</b> • Bot order placement: <b>OFF</b></div>
<div class="card"><b>💼 LIVE ORDER / POSITION</b>%s</div>
<div class="card"><b>🛰️ BROKER ORDER EVENTS</b>%s</div>
<div><div class="small">Showing %s qualifying setup rows. Refreshes every 3 seconds.</div>%s</div>
</div></body></html>""" % (
        css,esc(datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")),
        "OK" if d.get("authenticated_account_feed") else "WAITING",
        "CONNECTED" if monitor.get("connected") else "WAITING",
        esc(d.get("asset_count")),esc(len(setup_rows)),esc(c.get("cycle_id") or "—"),
        esc(c.get("completed_pass") or 0),order_html,event_html,esc(len(setup_rows)),
        "".join(cards) or '<div class="empty">Candice is scanning the account. No qualifying setup at this moment.</div>'
    )
    return body.encode("utf-8")

async def health(reader,writer):
    try:
        # Read HTTP headers first. reader.read() waits for client EOF, which can
        # deadlock a persistent HTTP/1.1 GET and make the Render/GitHub keepalive
        # curl request time out before /health is answered.
        head=await reader.readuntil(b"\r\n\r\n")
        lines=head.decode("latin1","ignore").split("\r\n")
        first=lines[0] if lines else ""
        parts=first.split(" ")
        path=parts[1] if len(parts)>1 else "/"
        headers={}
        for line in lines[1:]:
            if ":" in line:
                k,v=line.split(":",1);headers[k.strip().lower()]=v.strip()

        try:
            content_length=max(0,int(headers.get("content-length","0") or 0))
        except (TypeError,ValueError):
            content_length=0
        body=await reader.readexactly(content_length) if content_length else b""

        webhook_secret=os.getenv("TELEGRAM_WEBHOOK_SECRET","").strip()
        if path.startswith("/account-setups"):
            payload_out=await account_setups_page()
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: "+str(len(payload_out)).encode()+b"\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n"+payload_out)
            await writer.drain()
            return
        if path == "/ping":
            # Ultra-lightweight keepalive endpoint. It must never trigger
            # market scans, AI review, Telegram delivery, or signal cycles.
            payload=b'{"status":"ok","service":"CANDICE-AI"}'
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "+str(len(payload)).encode()+b"\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n"+payload)
            await writer.drain()
            log.info("PING_REQUEST status=200")
            return
        if path.startswith("/live-analysis"):
            payload_out=json.dumps(await build_live_analysis_payload(),ensure_ascii=False,default=str).encode()
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: "+str(len(payload_out)).encode()+b"\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n"+payload_out)
            await writer.drain()
            return

        if path.startswith("/health"):
            # Public health is intentionally non-sensitive. Keep account identifiers,
            # feed internals and network/public-IP metadata out of the endpoint; the
            # keepalive only needs a liveness/read-only status signal.
            lp_status=learning_practice_status()
            m1_status=m1_learning_status()
            body_out=json.dumps({
                "service":"CANDICE-AI","status":STATE["status"],
                "signal_read_only":True,
                "learning_demo_execution":bool(lp_status.get("enabled")),
                "learning_active":bool(lp_status.get("active")),
                "asset_count":len(STATE["assets"]),"qualified":len(STATE["analyses"]),
                "cycle":STATE["cycle"],"active_results":len(BRAIN.active_signals),
                "account_bound":bool(STATE.get("account_id")),
                "demo_account":str(STATE.get("account_group") or "").lower()=="demo",
                "authenticated_feed":str(STATE.get("feed_source") or "").startswith("authenticated_websocket:"),
                "m1_world_learning_enabled":bool(m1_status.get("enabled")),
                "live_external_ai":False
            }).encode()
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "+str(len(body_out)).encode()+b"\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n"+body_out)
            await writer.drain()
            log.info("HEALTH_REQUEST status=200 path=%s",path)
            return
        if path.startswith("/telegram/webhook") and webhook_secret and headers.get("x-telegram-bot-api-secret-token") != webhook_secret:
            writer.write(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            await writer.drain()
            return
        if path.startswith("/telegram/webhook") and not body:
            payload=b"NEXORA Telegram webhook is ready"
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: "+str(len(payload)).encode()+b"\r\nConnection: close\r\n\r\n"+payload)
            await writer.drain()
            return
        if path.startswith("/telegram/webhook") and body:
            try:
                upd=json.loads(body.decode("utf-8"))
                callback=upd.get("callback_query")
                if callback:
                    msg=(callback.get("message") or {})
                    chat_id=(msg.get("chat") or {}).get("id")
                    if chat_id is not None:
                        STATE["telegram_chat_id"]=chat_id
                        log.info("TELEGRAM_CHAT_ID_CAPTURED chat_id=%s source=callback",chat_id)
                    # Acknowledge the webhook immediately; the demo order is
                    # executed asynchronously only after the human ACCEPT gate.
                    asyncio.create_task(
                        handle_learning_callback(callback)
                    )
                else:
                    msg=upd.get("message") or upd.get("edited_message") or {}
                    chat_id=(msg.get("chat") or {}).get("id")
                    if chat_id is not None:
                        STATE["telegram_chat_id"]=chat_id
                        log.info("TELEGRAM_CHAT_ID_CAPTURED chat_id=%s",chat_id)
                    # Webhook HTTP 200 must be immediate; command logic is background.
                    asyncio.create_task(handle_telegram_command(msg))
            except Exception as e:
                log.warning("TELEGRAM_WEBHOOK_PARSE_FAILED %s",e)
        body_out=json.dumps({"service":"CANDICE-AI","status":"ok","read_only":True}).encode()
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "+str(len(body_out)).encode()+b"\r\nConnection: close\r\n\r\n"+body_out)
        await writer.drain()
    except (asyncio.IncompleteReadError,asyncio.LimitOverrunError):
        try:
            writer.write(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            await writer.drain()
        except Exception:
            pass
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

async def configure_telegram_webhook():
    token=os.getenv("TELEGRAM_BOT_TOKEN","").strip()
    if not token:
        log.warning("TELEGRAM_NOT_CONFIGURED"); return
    url=os.getenv("TELEGRAM_WEBHOOK_URL","https://priyanithan-zflv.onrender.com/telegram/webhook").strip()
    secret=os.getenv("TELEGRAM_WEBHOOK_SECRET","").strip()
    try:
        payload={"url":url}
        if secret: payload["secret_token"]=secret
        h=await _get_telegram_http_client()
        await h.post(f"https://api.telegram.org/bot{token}/deleteWebhook",
                     json={"drop_pending_updates":False},timeout=8.0)
        r=await h.post(f"https://api.telegram.org/bot{token}/setWebhook",
                       json=payload,timeout=8.0)
        r.raise_for_status()
        info=await h.get(f"https://api.telegram.org/bot{token}/getWebhookInfo",timeout=8.0)
        try: data=info.json().get("result",{})
        except Exception: data={}
        log.info("TELEGRAM_WEBHOOK_READY url=%s pending=%s last_error=%s",data.get("url",""),data.get("pending_update_count",0),str(data.get("last_error_message",""))[:160])
    except Exception as e:
        log.warning("TELEGRAM_WEBHOOK_SETUP_FAILED %s",e)

async def main():
    await load_persistent_learning()
    await ensure_ai_review_queue_table()
    await ensure_result_watch_queue_table()
    await ensure_cycle_state_table()
    await ensure_access_table()
    # The knowledge DB is an isolated bridge: learning writes compact validated
    # strategy/technique knowledge; the Signal Brain reads an in-memory snapshot.
    await ensure_strategy_knowledge_tables()
    await refresh_strategy_knowledge(force=True)
    configure_learning_lab(
        snapshot_provider=learning_market_snapshot,
        client_provider=lambda: CLIENT,
        send_message=telegram,
        answer_callback=telegram_answer_callback,
        admin_id=ADMIN_TELEGRAM_ID,
    )
    lp=learning_practice_status()
    log.info(
        "LEARNING_PRACTICE_CONFIG enabled=%s start=%s end=%s duration=%ss demo_only=%s approval_required=%s",
        lp.get("enabled"),lp.get("start_uae"),lp.get("end_uae"),
        lp.get("duration_seconds"),lp.get("demo_only"),lp.get("approval_required")
    )
    port=int(os.getenv("PORT","10000"));server=await asyncio.start_server(health,"0.0.0.0",port)
    await configure_telegram_webhook()
    # Start the DEMO learning supervisor first, then yield once so it enters
    # its own loop before the heavier market/research workers begin. This prevents
    # a long-running startup task from starving the overnight DEMO scheduler.
    learning_task=asyncio.create_task(
        learning_practice_supervisor(),
        name="learning_practice_supervisor",
    )
    await asyncio.sleep(0)
    await asyncio.gather(
        learning_task,
        market_worker(),
        account_tick_subscription_worker(),
        account_live_feed_worker(),
        cycle_loop_supervisor(),
        ai_review_worker(),
        world_learning_loop(),
        strategy_knowledge_refresh_loop(),
        server.serve_forever(),
    )
if __name__=="__main__":asyncio.run(main())
