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
# Cycle-state persistence is recovery metadata only. It must never be allowed
# to block the market/signal scheduler when the database stalls.
CYCLE_STATE_IO_TIMEOUT=2.5
AI_REVIEW_QUEUE_POLL_SECONDS=2.0
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
    if not LEARNING_DB_URL:
        return False
    try:
        import psycopg
        payload=json.dumps(
            _cycle_state_json(candidate_pool),
            separators=(",",":"),ensure_ascii=False,default=str
        )
        def put():
            with psycopg.connect(LEARNING_DB_URL,connect_timeout=8) as db:
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
                                WHEN EXCLUDED.status IN ('SENT','SKIPPED')
                                THEN NOW()
                                ELSE candice_cycle_state.completed_at
                            END
                    """,(
                        int(cycle_id),float(target_epoch),float(signal_epoch),
                        int(signal_lead),int(completed_pass),payload,status,
                        str(reason)[:500],SCHEDULER_OWNER
                    ))
                db.commit()
        await asyncio.wait_for(
            asyncio.to_thread(put),
            timeout=CYCLE_STATE_IO_TIMEOUT,
        )
        return True
    except asyncio.TimeoutError:
        log.warning(
            "CYCLE_STATE_SAVE_FAILED cycle=%s type=TimeoutError message=database_write_timeout timeout=%.1fs",
            cycle_id,CYCLE_STATE_IO_TIMEOUT
        )
        return False
    except Exception as e:
        log.warning("CYCLE_STATE_SAVE_FAILED cycle=%s type=%s message=%s",
                    cycle_id,type(e).__name__,str(e)[:160])
        return False

async def load_recoverable_cycle_state(now=None):
    if not LEARNING_DB_URL:
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
        log.warning(
            "CYCLE_STATE_RECOVERY_READ_FAILED type=TimeoutError message=database_read_timeout timeout=%.1fs",
            CYCLE_STATE_IO_TIMEOUT
        )
        return None
    except Exception as e:
        log.warning(
            "CYCLE_STATE_RECOVERY_READ_FAILED type=%s message=%s",
            type(e).__name__,str(e)[:160]
        )
        return None

async def mark_cycle_state(cycle_id,status,reason=""):
    if not LEARNING_DB_URL:
        return False
    try:
        import psycopg
        def done():
            with psycopg.connect(LEARNING_DB_URL,connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        UPDATE candice_cycle_state
                        SET status=%s,last_reason=%s,updated_at=NOW(),
                            completed_at=NOW(),owner=%s
                        WHERE cycle_id=%s
                    """,(status,str(reason)[:500],SCHEDULER_OWNER,int(cycle_id)))
                db.commit()
        await asyncio.to_thread(done)
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
                        WHERE status='PENDING'
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
                asyncio.create_task(result_watch(key))
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

# Operational throughput target for DEMO signal generation. This is a target,
# not a forced-signal quota: weak setups are never manufactured just to hit it.
DAILY_SIGNAL_TARGET=150
DAILY_SIGNAL_TARGET_SECONDS=86400.0
DAILY_SIGNAL_TARGET_STATE={"date":"","sent":0}

def daily_signal_target_snapshot(now=None):
    now=float(time.time() if now is None else now)
    day=datetime.fromtimestamp(now,tz=UAE_TZ).strftime("%Y-%m-%d")
    if DAILY_SIGNAL_TARGET_STATE.get("date")!=day:
        DAILY_SIGNAL_TARGET_STATE["date"]=day
        DAILY_SIGNAL_TARGET_STATE["sent"]=0
    sent=int(DAILY_SIGNAL_TARGET_STATE.get("sent") or 0)
    elapsed=datetime.fromtimestamp(now,tz=UAE_TZ).hour*3600+datetime.fromtimestamp(now,tz=UAE_TZ).minute*60+datetime.fromtimestamp(now,tz=UAE_TZ).second
    expected=DAILY_SIGNAL_TARGET*(elapsed/DAILY_SIGNAL_TARGET_SECONDS)
    return {
        "date":day,
        "sent":sent,
        "target":DAILY_SIGNAL_TARGET,
        "remaining":max(0,DAILY_SIGNAL_TARGET-sent),
        "pace_expected":round(expected,1),
        "ahead_by":round(sent-expected,1),
    }

def record_daily_signal_target_sent(now=None):
    snap=daily_signal_target_snapshot(now)
    DAILY_SIGNAL_TARGET_STATE["sent"]=int(snap["sent"])+1
    return daily_signal_target_snapshot(now)

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
# Bounded local tick history powers a real 5-second micro-candle view. It never
# fabricates missing ticks; unavailable coverage is reported explicitly.
TICK_HISTORY=defaultdict(lambda: deque(maxlen=720))
MULTI_TF_FRAMES=tuple(range(1,16))
MULTI_TF_MIN_COMPLETE_BARS=3
MULTI_TF_5S_SECONDS=5
MULTI_TF_5S_MIN_COMPLETE_BARS=4
TICK_RESUB_SEM=asyncio.Semaphore(3)
TICK_RESUB_TIMEOUT=1.5
LIVE_TICK_MAX_AGE=5.0
# Rolling coverage window for the account-wide rotating live feed. The broker
# only exposes a small number of simultaneous tick subscriptions, so an asset
# can be live-covered without having a fresh tick at every one-second audit.
LIVE_TICK_COVERAGE_MAX_AGE=60.0
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
# Once a specific candidate/candle is explicitly rejected by a hard
# qualification gate, do not resurrect that same decision through the
# exact-boundary cache fallback. A later scan can still produce a new key
# when a new closed candle forms.
CANDIDATE_CACHE_HARD_REJECTED={}
AI_PROVIDER_COOLDOWN={}
AI_REVIEW_TIMEOUT=2.4
# Rotating account-wide live quote scan. It does not touch Brain timing; it only
# keeps current account prices warm for analysis/candidate selection.
ACCOUNT_LIVE_SCAN_BATCH=16
ACCOUNT_LIVE_SCAN_INTERVAL=1.0
ACCOUNT_LIVE_SCAN_CURSOR=0
# Account-wide event-1 tick subscription manager. Subscriptions are read-only;
# the worker only requests market quotes and never places/modifies trades.
ACCOUNT_TICK_SUB_SEM=asyncio.Semaphore(1)
ACCOUNT_TICK_SUB_BATCH=4
ACCOUNT_TICK_SUB_RETRY=2.0
ACCOUNT_TICK_SUB_DELAY=0.15
ACCOUNT_TICK_MAX_SLOTS=2
ACCOUNT_TICK_ROTATE_INTERVAL=1.0
ACCOUNT_TICK_SUBSCRIBED=set()
ACCOUNT_TICK_LAST_ATTEMPT={}
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
    "ARAB_X":"Arabian General Index","OASIS_X":"Oasis Index","QAHWA_X":"Qahwa Index",
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

# The following is the exact OPEN asset set from the account screenshots the user
# supplied. Closed/hidden assets are deliberately not included. The broker feed
# remains the source for live prices, but it is NOT allowed to widen this universe.
SCREENSHOT_OPEN_PAIRS={
    # Exact 53 unique Flex assets visible in the user's supplied screenshots.
    # Raw IDs below are the authenticated OlympTrade instrument IDs used by
    # the account-scoped WebSocket event-182 asset feed.
    "BNBUSD_OTC","PEPEUSD_OTC","SHIBUSD_OTC",
    "ASIA_X","EUROPE_X","CRYPTO_X","GOAL_X","ETHUSD_OTC","MCI_X",
    "BTCUSD_OTC","LTCUSD_OTC","EURUSD_OTC","DOGUSD_OTC","XRPUSD_OTC",
    "HMA_X","NZDUSD_OTC","Bitcoin","AUDUSD_OTC","GBPUSD_OTC","ULTRA_X",
    "XAUUSD_OTC","USDCHF_OTC","AUDCAD_OTC","USDCAD_OTC","GBPJPY_OTC",
    "CADJPY_OTC","USDJPY_OTC","STABLE_X","GBPCAD_OTC","EURGBP_OTC",
    "AUDCHF_OTC","AUDNZD_OTC","AUDJPY_OTC","XAGUSD_OTC","CHFJPY_OTC",
    "EURAUD_OTC","CADCHF_OTC","EURCAD_OTC","EURJPY_OTC","EURCHF_OTC",
    "EURNZD_OTC","GBPCHF_OTC","GBPAUD_OTC","NZDCHF_OTC","GBPNZD_OTC",
    "ETHUSD","NZDJPY_OTC","NZDCAD_OTC","ALTCOIN","QAHWA_X","OASIS_X",
    "ARAB_X","LTCUSD",
}

def asset_key(value):
    text=_norm_text(value)
    return " ".join(text.replace("/"," ").replace("_"," ").split())
def screenshot_asset_allowed(x):
    if not isinstance(x,dict): return False
    p=pair_name(x)
    # Current event-182 payloads expose the exact instrument ID (p/id), while
    # some older payloads also carry a human-readable title. The screenshot
    # baseline is therefore enforced primarily by the authenticated raw pair.
    if p in SCREENSHOT_OPEN_PAIRS:
        return True
    title=display_name(x)
    return asset_key(title) in SCREENSHOT_OPEN_PAIRS

def is_flex_time_asset(x):
    if not isinstance(x,dict): return False
    # The user explicitly asked for the exact 53-asset Flex universe shown in
    # the supplied OlympTrade screenshots: no broker-side extras and no
    # synthetic/global market symbols. The account WebSocket remains the source
    # of the raw asset payload and live prices.
    return screenshot_asset_allowed(x)

def build_assets(client,raw):
    # Account-scoped assets are authoritative, but the user's requested
    # screenshot baseline is the exact permitted Flex universe.
    prof={}
    for x in raw or []:
        if not isinstance(x,dict): continue
        p=pair_name(x)
        v=x.get("profitability")
        if p and isinstance(v,(int,float)): prof[p]=int(v)

    out=[]; seen=set(); rejected=[]
    raw_pairs={pair_name(x) for x in (raw or []) if isinstance(x,dict) and pair_name(x)}
    matched=raw_pairs & SCREENSHOT_OPEN_PAIRS
    missing=sorted(SCREENSHOT_OPEN_PAIRS-raw_pairs)
    extra=sorted(raw_pairs-SCREENSHOT_OPEN_PAIRS)
    log.info(
        "ACCOUNT_SCREENSHOT_ASSET_COMPARE expected=%d matched=%d missing=%d extra=%d",
        len(SCREENSHOT_OPEN_PAIRS),len(matched),len(missing),len(extra)
    )
    if missing:
        log.warning("ACCOUNT_SCREENSHOT_ASSET_MISSING %s",missing)
    if extra:
        log.info("ACCOUNT_SCREENSHOT_ASSET_EXTRA %s",extra)
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
        title=display_name(x)
        if not title:
            # Never expose a raw instrument ID in a user-facing message. An
            # unmapped asset remains in the account scan, but is not allowed to
            # produce a signal until an account-facing label is available.
            log.warning("ACCOUNT_ASSET_DISPLAY_NAME_MISSING pair=%s",p)
            continue
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
    # Audit the exact account-facing/raw names that Candice accepted.
    log.info(
        "ACCOUNT_ASSET_NAMES %s",
        [{"pair":a["pair"],"account_name":a["display_name"]} for a in out]
    )
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
        TICK_HISTORY.pop(pair,None)
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
    recent_missing_pairs=[
        a["pair"] for a in assets
        if not has_fresh_live_price(a["pair"],now,LIVE_TICK_COVERAGE_MAX_AGE)
    ]
    recent=total-len(recent_missing_pairs)
    log.info(
        "ACCOUNT_LIVE_FEED_SCAN source=authenticated_websocket:event_1 assets=%d fresh_tick=%d recent_tick=%d recent_missing=%d",
        total,fresh,recent,len(recent_missing_pairs)
    )
    # Keep production logs compact; the aggregate coverage count above is
    # sufficient for the 1-minute account-feed health audit.


async def account_live_feed_worker():
    while True:
        try:
            await scan_account_live_feed()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("ACCOUNT_LIVE_FEED_WORKER_ERROR type=%s message=%s",type(e).__name__,str(e)[:160])
        await asyncio.sleep(1.0)


async def _unsubscribe_account_tick(pair):
    client=CLIENT
    if not client or not pair:
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
    if not client or not pair:
        return False
    async with ACCOUNT_TICK_SUB_SEM:
        try:
            await asyncio.wait_for(client.market.subscribe_ticks(pair),timeout=6.0)
            ACCOUNT_TICK_SUBSCRIBED.add(pair)
            ACCOUNT_TICK_LAST_ATTEMPT[pair]=time.time()
            log.info("ACCOUNT_TICK_SUBSCRIBE pair=%s status=accepted",pair)
            return True
        except asyncio.CancelledError:
            raise
        except Exception as e:
            ACCOUNT_TICK_LAST_ATTEMPT[pair]=time.time()
            log.warning("ACCOUNT_TICK_SUBSCRIBE pair=%s status=rejected type=%s message=%s",
                        pair,type(e).__name__,str(e)[:120])
            return False

def _tick_pinned_pairs():
    now=time.time()
    expired=[p for p,until in ACCOUNT_TICK_PINNED.items() if float(until)<=now]
    for p in expired:
        ACCOUNT_TICK_PINNED.pop(p,None)
    return [p for p,until in ACCOUNT_TICK_PINNED.items() if float(until)>now]

async def pin_account_tick_pairs(pairs,ttl=12.0):
    """Pin final candidate tick slots atomically against the rotation worker."""
    global ACCOUNT_TICK_LAST_ROTATION
    async with ACCOUNT_TICK_CONTROL_LOCK:
        unique=[]
        seen=set()
        for p in pairs or []:
            p=str(p or "")
            if p and p not in seen:
                seen.add(p);unique.append(p)
        targets=unique[:ACCOUNT_TICK_MAX_SLOTS]
        until=time.time()+float(ttl)
        for p in targets:
            ACCOUNT_TICK_PINNED[p]=until
        current=set(ACCOUNT_TICK_SUBSCRIBED)
        for p in list(current):
            if p not in targets:
                await _unsubscribe_account_tick(p)

        for p in targets:
            if p not in ACCOUNT_TICK_SUBSCRIBED:
                await _subscribe_account_tick(p)

        ACCOUNT_TICK_LAST_ROTATION=time.time()
        log.info("ACCOUNT_TICK_PIN targets=%s active=%s ttl=%.1f",
                 targets,sorted(ACCOUNT_TICK_SUBSCRIBED),float(ttl))
        return sum(1 for p in targets if p in ACCOUNT_TICK_SUBSCRIBED)

async def ensure_account_tick_subscriptions():
    """Rotate the authenticated event-1 tick slots across the exact account asset universe.

    The broker currently accepts only a small number of simultaneous per-connection
    pair subscriptions (observed at four). Do not hammer the same connection with
    requests for all 53 pairs; rotate the four live slots and pin final candidates.
    """
    global ACCOUNT_TICK_ROTATE_CURSOR, ACCOUNT_TICK_LAST_ROTATION
    client=CLIENT
    if not client or not client.connection.is_connected:
        return
    # Live-feed coverage must include every account asset. Signal eligibility is
    # a Brain decision-layer concern and must not remove an asset from the
    # authenticated market-data rotation.
    assets=[a for a in list(STATE["assets"]) if a.get("pair")]
    if not assets:
        return

    async with ACCOUNT_TICK_CONTROL_LOCK:
        pinned=_tick_pinned_pairs()
        if pinned:
            # pin_account_tick_pairs() owns this same lock and already made the
            # pinned subscriptions live. Never run normal rotation while a
            # final candidate is pinned; doing so could unsubscribe it between
            # the final-candidate pin and the signal deadline.
            return

        now=time.time()
        if now-ACCOUNT_TICK_LAST_ROTATION < ACCOUNT_TICK_ROTATE_INTERVAL:
            return

        pairs=[str(a["pair"]) for a in assets]
        n=len(pairs)
        start=ACCOUNT_TICK_ROTATE_CURSOR % n
        targets=[pairs[(start+i) % n] for i in range(min(ACCOUNT_TICK_MAX_SLOTS,n))]
        ACCOUNT_TICK_ROTATE_CURSOR=(start+len(targets)) % n

        # Free existing slots first; event 13 is the authenticated tick unsubscribe.
        for p in list(ACCOUNT_TICK_SUBSCRIBED):
            if p not in targets:
                await _unsubscribe_account_tick(p)

        accepted=0
        for p in targets:
            if p in ACCOUNT_TICK_SUBSCRIBED:
                continue
            if await _subscribe_account_tick(p):
                accepted+=1
            await asyncio.sleep(ACCOUNT_TICK_SUB_DELAY)

        ACCOUNT_TICK_LAST_ROTATION=now
        log.info("ACCOUNT_TICK_SLOT_ROTATION targets=%s accepted=%d active=%d cursor=%d",
                 targets,accepted,len(ACCOUNT_TICK_SUBSCRIBED),ACCOUNT_TICK_ROTATE_CURSOR)

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
        await asyncio.sleep(2.0)

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

async def send_learning_summary(summary):
    if not summary:
        return
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
    """Refresh candidate quotes from the authenticated event-1 stream, never from a snapshot API."""
    client=CLIENT
    if not client or not pairs:
        return 0
    await ensure_candidate_ticks(pairs)
    unique=list(dict.fromkeys(str(p) for p in pairs if p))
    deadline=time.time()+2.0
    fresh=0
    while time.time()<deadline:
        now=time.time()
        fresh=sum(1 for p in unique if has_fresh_live_price(p,now,LIVE_TICK_MAX_AGE))
        if fresh>=min(len(unique),ACCOUNT_TICK_MAX_SLOTS):
            break
        await asyncio.sleep(0.10)
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
        # keeps the account-wide 52/52 scan complete without allowing a
        # non-signal asset into the send gate.
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
    analyzed_count=0
    for a in assets:
        p=a["pair"]
        price=STATE["prices"].get(p,(None,None))[0]
        closed=_closed_candles(STATE["candles"].get(p,[]),reference)
        if len(closed)<45:
            if a.get("signal_eligible",True):
                STATE["analyses"].pop(p,None)
            continue
        # Count every successfully analyzed account asset, even when its
        # technical setup does not qualify as a signal. The latter remains
        # represented separately by STATE["analyses"] for candidate ranking.
        analyzed_count+=1
        an=analyze_asset(a,closed,price)
        if an and a.get("signal_eligible",True):
            an["profitability"]=a["profitability"]
            STATE["analyses"][p]=an
        elif a.get("signal_eligible",True):
            STATE["analyses"].pop(p,None)

    stale_count=sum(1 for a in assets if _candle_data_stale(a["pair"],reference))
    log.info(
        "LIVE_ANALYSIS_REFRESH assets=%d analyzed=%d signal_eligible=%d fetched=%d stale=%d qualified=%d",
        len(assets),analyzed_count,
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
    """Final local confirmation gate using all available requested frames."""
    expected=str(expected or "").upper()
    frames=dict((context or {}).get("frames") or {})
    if expected not in {"UP","DOWN"}:
        return False,{"reason":"invalid_direction"}

    five=frames.get("5s",{})
    thirty=frames.get("30s",{})
    one=frames.get("1m",{})
    two=frames.get("2m",{}).get("candle_confirmation") or {}

    # Candle-first DEMO 1-minute gate: closed 30s, 1m and 2m trends must agree.
    # The 2m candle trend is primary; indicators are not hard gates.
    if thirty.get("status")!="READY":
        return False,{"reason":"30s_confirmation_insufficient","bars":thirty.get("bars",0)}
    if thirty.get("direction")!=expected:
        return False,{"reason":"30s_candle_trend_conflict","direction":thirty.get("direction","NEUTRAL")}
    if one.get("status")!="READY":
        return False,{"reason":"1m_confirmation_insufficient","bars":one.get("bars",0)}
    if one.get("direction")!=expected:
        return False,{"reason":"1m_candle_trend_conflict","direction":one.get("direction","NEUTRAL")}
    if two.get("status")!="READY":
        return False,{"reason":"2m_confirmation_insufficient"}
    if two.get("direction")!=expected:
        return False,{"reason":"2m_candle_trend_conflict","direction":two.get("direction","NEUTRAL"),"same_direction_candles":two.get("same_direction_candles",0)}
    if not two.get("trend_ok") or not two.get("candle_ok"):
        return False,{"reason":"2m_candle_strength_insufficient","trend_ok":two.get("trend_ok",False),"candle_ok":two.get("candle_ok",False),"body_ratio":two.get("body_ratio",0)}
    if not two.get("volume_ok"):
        return False,{"reason":"2m_volume_confirmation_failed","volume_ratio":two.get("volume_ratio",0),"volume_available":two.get("volume_available",False)}
    # 5s remains diagnostic microstructure; it is not promoted to a hard veto.
    # This preserves the earlier structural fix where a brief 5s reversal cannot
    # erase a confirmed 1m/2m setup.
    # 5s is microstructure context, not a hard veto on the Candice Brain.
    # A brief 5s reversal must not erase an otherwise valid 1m..15m setup.
    five_status=five.get("status")
    five_direction=five.get("direction","NEUTRAL")
    if five_status!="READY":
        five_warning="5s_insufficient"
    elif five_direction not in {expected,"NEUTRAL"}:
        five_warning="5s_opposite"
    else:
        five_warning="none"

    short=[frames.get(f"{m}m",{}) for m in (1,2,3,4)]
    short=[x for x in short if x.get("status")=="READY"]
    if len(short)<3:
        return False,{"reason":"short_frames_insufficient","ready":len(short)}
    short_align=sum(1 for x in short if x.get("direction")==expected)
    short_opp=sum(1 for x in short if x.get("direction") not in {expected,"NEUTRAL"})
    if short_align<3 or short_opp>1:
        return False,{"reason":"short_frame_conflict","align":short_align,"opp":short_opp}

    all_ready=[]
    for m in range(5,16):
        x=frames.get(f"{m}m",{})
        if x.get("status")=="READY":
            all_ready.append(x)
    directional=[x for x in all_ready if x.get("direction") in {"UP","DOWN"}]
    align=sum(1 for x in directional if x.get("direction")==expected)
    opp=sum(1 for x in directional if x.get("direction") not in {expected,"NEUTRAL"})
    agreement=(align/max(1,len(directional)))
    if directional and (agreement<0.60 or opp>2):
        return False,{"reason":"higher_frame_conflict","align":align,"opp":opp,"directional":len(directional),"agreement":round(agreement,3)}

    return True,{
        "reason":"multi_timeframe_confirmed",
        "30s":thirty.get("direction","NEUTRAL"),
        "30s_bars":thirty.get("bars",0),
        "1m":one.get("direction","NEUTRAL"),
        "2m":two.get("direction","NEUTRAL"),
        "2m_same_direction_candles":two.get("same_direction_candles",0),
        "2m_candle_direction":two.get("candle_direction","NEUTRAL"),
        "2m_body_ratio":two.get("body_ratio",0),
        "2m_volume_ratio":two.get("volume_ratio",0),
        "2m_volume_available":two.get("volume_available",False),
        "5s":five_direction,
        "5s_warning":five_warning,
        "short_align":short_align,
        "short_opp":short_opp,
        "higher_align":align,
        "higher_opp":opp,
        "higher_directional":len(directional),
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
    # authenticated OlympTrade account-scoped asset feed is ready.
    if not CLIENT or STATE.get("account_group") != "demo" or not STATE.get("account_id"):
        log.info("BRAIN_ACCOUNT_GATE blocked=account_not_ready")
        return None
    if STATE.get("feed_source") not in {
        "authenticated_websocket:event_182",
        "authenticated_websocket:event_183",
    }:
        log.info("BRAIN_ACCOUNT_GATE blocked=non_account_feed source=%s",
                 STATE.get("feed_source"))
        return None
    eligible=[
        a for a in BRAIN.filter_candidates(STATE["assets"])
        if a.get("signal_eligible",True)
    ]
    analyzed=[STATE["analyses"][a["pair"]].copy() for a in eligible if a["pair"] in STATE["analyses"]]
    adapted=[BRAIN.adaptive_candidate(x) for x in analyzed]
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
        for seed in seed_list[:20]:
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
                if len(current_candles)<45:
                    log.info(
                        "FINAL_RECOVERY_REJECTED pair=%s reason=closed_candles=%s",
                        pair,len(current_candles)
                    )
                    continue
                live_price=STATE["prices"].get(
                    pair,(None,None)
                )[0]
                refreshed=analyze_asset(asset,current_candles,live_price)
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

    candidate_inputs=adapted+recovery_adapted
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
    top=raw[:(20 if deep_analysis else 5)]
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
        mtf=build_multi_timeframe_context(x["pair"],closed,time.time())
        x["multi_timeframe"]=mtf
        x["multi_timeframe_confirmed"]=False
        x["multi_timeframe_diagnostic"]={"reason":"FINAL_ONLY"}
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
        return ranked[:20]
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
        log.warning("RESULT_PENDING_NO_CLOSED_CANDLE pair=%s entry=%s durable_watch=%s",s.pair,s.entry_price,watch_id)
        if key in BRAIN.active_signals:
            await asyncio.sleep(1.0)
            return await result_watch(key)
        return

    rec=BRAIN.finish_signal(key,expiry_price)

    # Result classification is complete. Never block result delivery on an external
    # AI provider; enqueue the full evidence for durable History-AI processing.
    review_id=f"{rec['cycle_id']}:{rec['pair']}:{rec['entry_ts']}"
    await enqueue_ai_review(review_id,rec)
    await save_persistent_learning()
    batch_summary=BRAIN.consume_batch_summary()
    if batch_summary:
        log.info("LEARNING_10_SIGNAL_SUMMARY batch=%s wins=%s losses=%s ties=%s cooldown_seconds=%s account_id=%s",
                 batch_summary.get("batch_no"),batch_summary.get("wins"),batch_summary.get("losses"),
                 batch_summary.get("ties"),batch_summary.get("cooldown_seconds"),batch_summary.get("account_id"))
        await send_learning_summary(batch_summary)
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
    await complete_result_watch(watch_id)
    log.info(
        "RESULT pair=%s result=%s strategy=%s self_strategy=%s self_version=%s confidence=%s trend=%s structure=%s pattern=%s expiry=%s entry=%s exit=%s source=%s cooldown=%s",
        rec["pair"],rec["result"],rec["strategy"],rec.get("self_strategy",""),rec.get("self_strategy_version",""),rec["confidence"],rec["trend_15m"],
        rec["structure_1m"],rec["pattern"],rec["expiry_minutes"],rec["entry_price"],rec["exit_price"],
        expiry_source,rec["result"]=="LOSS"
    )

async def cycle_loop():
    # Fast 3-minute signal scheduler.
    # For each target T:
    #   - target = exact next 3-minute boundary
    #   - Telegram signal = target minus an exact 30s lead
    #   - five Brain passes are completed inside the same 150s pre-signal window
    #   - pass 5 is the deep/final qualification pass
    #   - 5s + every complete 1m..15m frame are checked in each pass
    #   - result watching and History-AI remain background tasks and never block
    #     the scheduler, so one completed signal cannot stop the next cycle.
    # The Brain/AI strategy itself is unchanged; only the scheduling cadence
    # and the requested expiry are changed for DEMO analysis.
    SIGNAL_INTERVAL=180.0
    SIGNAL_LEADS=(30.0,30.0)
    SCAN_OFFSETS=(150.0,125.0,100.0,75.0,55.0)

    async def send_cycle_signal(candidate,target,signal_lead,cycle_id):
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
        now=time.time()

        # Keep the final quote fresh without turning a transient rotating-tick
        # miss into a dead cycle. Pass 5 pre-pins the top candidates; this quick
        # refresh is only a safety net for the exact asset being attempted.
        if not has_fresh_live_price(p,now,LIVE_TICK_MAX_AGE):
            try:
                await pin_account_tick_pairs(
                    [p],
                    ttl=max(8.0,target-time.time()+6.0)
                )
            except Exception as e:
                log.warning(
                    "FINAL_LIVE_QUOTE_PIN_FAILED cycle=%s pair=%s type=%s message=%s",
                    cycle_id,p,type(e).__name__,str(e)[:120]
                )
            refresh_deadline=min(target-0.25,time.time()+1.0)
            while time.time()<refresh_deadline and not has_fresh_live_price(
                p,time.time(),LIVE_TICK_MAX_AGE
            ):
                await asyncio.sleep(0.05)

        if not has_fresh_live_price(p,time.time(),LIVE_TICK_MAX_AGE):
            log.info(
                "NO_VALID_SIGNAL_AT_SEND cycle=%s pair=%s reason=quote_not_fresh next_asset=TRUE",
                cycle_id,p
            )
            return False

        entry=STATE["prices"].get(p,(None,None))[0]
        if entry is None:
            log.info(
                "NO_VALID_SIGNAL_AT_SEND cycle=%s pair=%s reason=price_missing next_asset=TRUE",
                cycle_id,p
            )
            return False

        # LAST-SECOND FINAL CONFIRMATION:
        # 30s is intentionally removed. Immediately before delivery, keep the
        # requested 2m candle confirmation PLUS volume/body-strength, 1m candle
        # gate, and higher-timeframe gate. Indicators are not used here.
        reference=time.time()
        closed_1m=_closed_candles(STATE["candles"].get(p,[]),reference)
        bars2=_aggregate_closed_minutes(closed_1m,2,reference)
        two=_candle_confirmation_2m(bars2,expected)
        one=_latest_closed_candle_direction(closed_1m)
        final_mtf=build_multi_timeframe_context(p,closed_1m,reference,expected)
        frames=final_mtf.get("frames") or {}
        higher=[frames.get(f"{m}m",{}) for m in range(5,16)]
        higher=[x for x in higher if x.get("status")=="READY" and x.get("direction") in {"UP","DOWN"}]
        higher_align=sum(1 for x in higher if x.get("direction")==expected)
        higher_opp=sum(1 for x in higher if x.get("direction")!=expected)
        higher_agreement=higher_align/max(1,len(higher))
        final_mtf_diag={
            "2m":two.get("candle_direction",two.get("direction","NEUTRAL")),
            "2m_bars":len(bars2),
            "2m_volume_ok":two.get("volume_ok",False),
            "2m_volume_ratio":two.get("volume_ratio",0),
            "2m_body_ratio":two.get("body_ratio",0),
            "1m":one.get("direction","NEUTRAL"),
            "higher_align":higher_align,
            "higher_opp":higher_opp,
            "higher_agreement":round(higher_agreement,3),
        }

        confirm_reason=None
        if two.get("status")!="READY":
            confirm_reason="2m_candle_not_ready"
        elif two.get("candle_direction")!=expected:
            confirm_reason="2m_candle_trend_conflict"
        elif not two.get("candle_ok"):
            confirm_reason="2m_body_strength_failed"
        elif two.get("volume_available") and not two.get("volume_ok"):
            # Broker 2m candle payloads can legitimately omit volume. In that
            # case volume cannot be used as a hard reject; preserve the volume
            # rule whenever real volume data is actually available.
            confirm_reason="2m_volume_confirmation_failed"
        elif one.get("status")!="READY":
            confirm_reason="1m_candle_not_ready"
        elif one.get("direction")!=expected:
            confirm_reason="1m_candle_trend_conflict"
        elif len(higher)<1:
            confirm_reason="higher_timeframe_not_ready"
        elif higher_agreement<0.60 or higher_opp>2:
            confirm_reason="higher_timeframe_conflict"

        if confirm_reason:
            log.info(
                "FINAL_2M_VOLUME_BODY_1M_HIGHER_REJECTED cycle=%s pair=%s direction=%s reason=%s diagnostic=%s next_asset=TRUE",
                cycle_id,p,expected,confirm_reason,final_mtf_diag
            )
            return False

        log.info(
            "FINAL_2M_VOLUME_BODY_1M_HIGHER_CONFIRMED cycle=%s pair=%s direction=%s diagnostic=%s",
            cycle_id,p,expected,final_mtf_diag
        )

        confidence=int(candidate.get("confidence") or 0)
        if confidence < 90:
            log.info(
                "NO_VALID_SIGNAL_AT_SEND cycle=%s pair=%s reason=confidence_%s",
                cycle_id,p,confidence
            )
            return False

        ts=target-signal_lead
        if time.time() > ts+0.25:
            log.info(
                "NO_VALID_SIGNAL_AT_SEND cycle=%s pair=%s reason=deadline_passed",
                cycle_id,p
            )
            return False

        # Only a candidate that survived pass 5 is eligible for final delivery.
        if int(candidate.get("qualified_pass") or 0) < 5:
            log.info(
                "NO_VALID_SIGNAL_AT_SEND cycle=%s pair=%s reason=not_final_pass_qualified",
                cycle_id,p
            )
            return False

        s=BRAIN.mark_signal_sent(
            account_id=STATE.get("account_id"),
            pair=p,display_name=candidate["display_name"],
            direction=candidate["direction"],expiry_minutes=1,
            # This is the pre-entry reference shown in the alert. The actual
            # entry price is captured at target by result_watch().
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
        key=f"{s.cycle_id}:{s.pair}:{s.entry_ts}"
        msg=(f"🚨 CANDICE AI SIGNAL\n\n"
             f"👋 Market setup detected!\n\n"
             f"📊 {s.display_name}\n\n"
             f"<b>{'🔻 DOWN' if s.direction.upper() == 'DOWN' else '🟢 UP'}</b>\n"
             f"<b>⏱️ {s.expiry_minutes} MIN EXPIRY</b>\n\n"
             f"🕒 {uae_time(ts)} UAE\n"
             f"🎯 Entry → {uae_time(target)}\n\n"
             f"💰 Reference → {s.entry_price}\n"
             f"🎯 Confidence → {s.confidence}%\n\n"
             f"📈 Trend → {s.trend_15m or '—'}\n"
             f"🕯️ Structure → {s.structure_1m or '—'}\n"
             f"🧠 Strategy → {s.strategy}\n\n"
             f"🟢 DEMO • READ ONLY\n"
             f"🤖 CANDICE BRAIN")
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
        asyncio.create_task(
            telegram_background(msg,f"{s.cycle_id}:{s.pair}:{s.entry_ts}")
        )
        # Durable result-watch persistence is deliberately asynchronous. A slow
        # database must never consume the 30-second signal window or delay the
        # scheduler; result_watch() remains the live source of truth.
        asyncio.create_task(persist_result_watch_background(key,s))
        asyncio.create_task(result_watch(key))
        return True

    cycle_sequence=0
    target=None

    def account_ready_now():
        return bool(
            CLIENT
            and getattr(CLIENT.connection,"is_connected",False)
            and STATE.get("account_group")=="demo"
            and STATE.get("account_id")
            and STATE.get("feed_source") in {
                "authenticated_websocket:event_182",
                "authenticated_websocket:event_183",
            }
            and len(STATE.get("assets") or [])>0
        )

    while True:
        now=time.time()
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
                        key=(item.get("pair"),str(item.get("entry_candle_ts")),
                             str(item.get("direction") or "").upper())
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

        # Root-cause guard: cycle analysis must never start with an empty or
        # unauthenticated account snapshot. A restart immediately before the
        # signal boundary cannot complete five passes safely, so skip that
        # boundary and preserve the next full 3-minute window.
        signal_in=max(0.0,signal_at-time.time())
        log.info(
            "CYCLE_ACCOUNT_READY_GUARD cycle=%s account_ready=%s assets=%d "
            "signal_in=%.2f signal_utc=%s",
            cycle_id,account_ready_now(),len(STATE.get("assets") or []),signal_in,
            time.strftime("%H:%M:%S",time.gmtime(signal_at))
        )
        if signal_in < 20.0:
            log.warning(
                "CYCLE_SKIPPED_LATE_START cycle=%s signal_utc=%s signal_in=%.2f "
                "reason=insufficient_window_for_account_and_five_passes",
                cycle_id,time.strftime("%H:%M:%S",time.gmtime(signal_at)),signal_in
            )
            continue

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
        await save_cycle_state(
            cycle_id,target,signal_at,signal_lead,resume_completed_pass,
            candidate_pool,status="ACTIVE",
            reason="cycle_resumed" if recovered else "cycle_started"
        )

        log.info(
            "CYCLE_WINDOW_START cycle=%s sequence=%s interval=%ss signal_lead=%ss "
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
                            and STATE.get("feed_source") in {
                                "authenticated_websocket:event_182",
                                "authenticated_websocket:event_183",
                            }
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
                await asyncio.wait_for(
                    refresh_candles(force=True),
                    timeout=scan_budget
                )
                log.info(
                    "ACCOUNT_FULL_SCAN cycle=%s scan=SCAN_%s pass=%s assets=%d analyzed=%d "
                    "seconds_to_signal=%.2f deep=%s catchup=%s",
                    cycle_id,pass_no,pass_no,len(STATE["assets"]),
                    len(STATE["analyses"]),max(0,signal_at-time.time()),
                    pass_no==5,catchup_mode
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
                candidate=await asyncio.wait_for(
                    final_candidate(
                        # Scan passes select from the full authenticated account/candle
                        # universe. Live quote freshness is a delivery concern; it
                        # must not collapse pass 5 to a single asset because the
                        # broker rotates a small number of tick slots.
                        require_live_price=False,
                        deep_analysis=(pass_no==5),
                        return_ranked=(pass_no==5)
                    ),
                    timeout=max(1.0,remaining-0.50)
                )
                if isinstance(candidate,list):
                    selected=candidate[:20]
                    if not selected:
                        log.info(
                            "SCAN_COMPLETE cycle=%s scan=SCAN_%s candidate=none analyzed=%d",
                            cycle_id,pass_no,len(STATE["analyses"])
                        )
                    else:
                        for raw_candidate in selected:
                            if not isinstance(raw_candidate,dict) or not raw_candidate.get("pair"):
                                continue
                            item=raw_candidate.copy()
                            item["expiry_minutes"]=1
                            item["qualified_pass"]=pass_no
                            key=(
                                item.get("pair"),
                                str(item.get("entry_candle_ts")),
                                str(item.get("direction") or "").upper()
                            )
                            candidate_pool[key]=item

                        if pass_no==5:
                            # Pre-pin the strongest two final candidates so a
                            # final candle rejection can immediately fall through
                            # to the next asset without waiting for tick rotation.
                            final_items=sorted(
                                [x for x in candidate_pool.values()
                                 if int(x.get("qualified_pass") or 0)==5],
                                key=lambda x:(
                                    int(x.get("confidence") or 0),
                                    float(x.get("strategy_margin") or 0),
                                    float(x.get("direction_agreement") or 0),
                                    float(x.get("market_quality") or 0)
                                ),
                                reverse=True
                            )[:ACCOUNT_TICK_MAX_SLOTS]
                            if final_items:
                                pin_ttl=max(15.0,target-time.time()+8.0)
                                try:
                                    await pin_account_tick_pairs(
                                        [x.get("pair") for x in final_items],
                                        ttl=pin_ttl
                                    )
                                    log.info(
                                        "FINAL_CANDIDATE_TICKS_PINNED cycle=%s pairs=%s pass=%s ttl=%.1f target_utc=%s",
                                        cycle_id,[x.get("pair") for x in final_items],
                                        pass_no,pin_ttl,
                                        datetime.fromtimestamp(
                                            target,tz=timezone.utc
                                        ).strftime("%H:%M:%S")
                                    )
                                except Exception as e:
                                    log.warning(
                                        "FINAL_CANDIDATE_TICKS_PIN_FAILED cycle=%s pairs=%s pass=%s type=%s message=%s",
                                        cycle_id,[x.get("pair") for x in final_items],
                                        pass_no,type(e).__name__,str(e)[:120]
                                    )
                        for item in selected:
                            log.info(
                                "SCAN_CANDIDATE_SELECTED cycle=%s scan=SCAN_%s pass=%s pair=%s "
                                "confidence=%s strategy=%s expiry=%s pool=%s deep=%s",
                                cycle_id,pass_no,pass_no,item.get("pair"),
                                item.get("confidence"),item.get("strategy"),item.get("expiry_minutes"),
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
                    key=(
                        candidate.get("pair"),
                        str(candidate.get("entry_candle_ts")),
                        str(candidate.get("direction") or "").upper()
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

            # Final recovery window: if the scheduled deep pass returns no
            # eligible candidate, re-check the candidates found in earlier passes using
            # the latest closed candles. This is a fresh Brain decision, not a stale
            # candidate bypass. Keep a hard time budget so the exact 30s lead is never
            # sacrificed.
            if candidate is None and pass_no==5 and candidate_pool:
                recovery_remaining=max(0,signal_at-time.time())
                if recovery_remaining>=8.0:
                    try:
                        recovery_timeout=min(10.0,max(1.0,recovery_remaining-6.0))
                        log.info(
                            "FINAL_RECOVERY_WINDOW cycle=%s seeds=%d timeout=%.2f "
                            "seconds_to_signal=%.2f",
                            cycle_id,len(candidate_pool),recovery_timeout,
                            recovery_remaining
                        )
                        candidate=await asyncio.wait_for(
                            final_candidate(
                                require_live_price=True,
                                deep_analysis=True,
                                seed_candidates=list(candidate_pool.values())
                            ),
                            timeout=recovery_timeout
                        )
                        if candidate is not None:
                            candidate=candidate.copy()
                            candidate["expiry_minutes"]=1
                            candidate["qualified_pass"]=5
                            key=(
                                candidate.get("pair"),
                                str(candidate.get("entry_candle_ts")),
                                str(candidate.get("direction") or "").upper()
                            )
                            candidate_pool[key]=candidate
                            try:
                                pin_ttl=max(
                                    8.0,
                                    target-time.time()+8.0
                                )
                                await pin_account_tick_pairs(
                                    [candidate.get("pair")],ttl=pin_ttl
                                )
                            except Exception as e:
                                log.warning(
                                    "FINAL_RECOVERY_TICK_PIN_FAILED cycle=%s pair=%s type=%s message=%s",
                                    cycle_id,candidate.get("pair"),
                                    type(e).__name__,str(e)[:120]
                                )
                            log.info(
                                "FINAL_RECOVERY_READY cycle=%s pair=%s confidence=%s "
                                "strategy=%s direction=%s seconds_to_signal=%.2f",
                                cycle_id,candidate.get("pair"),
                                candidate.get("confidence"),
                                candidate.get("strategy"),
                                candidate.get("direction"),
                                max(0,signal_at-time.time())
                            )
                        else:
                            log.info(
                                "FINAL_RECOVERY_NONE cycle=%s seeds=%d seconds_to_signal=%.2f",
                                cycle_id,len(candidate_pool),
                                max(0,signal_at-time.time())
                            )
                    except asyncio.TimeoutError:
                        log.warning(
                            "FINAL_RECOVERY_TIMEOUT cycle=%s seeds=%d seconds_to_signal=%.2f",
                            cycle_id,len(candidate_pool),
                            max(0,signal_at-time.time())
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
            await save_cycle_state(
                cycle_id,target,signal_at,signal_lead,pass_no,
                candidate_pool,status="ACTIVE",
                reason=f"scan_{pass_no}_completed"
            )
            if catchup_mode and pass_no<5:
                catchup_next_at=target-SCAN_OFFSETS[pass_no]
        # The final signal can only use a candidate produced by the fifth/deep
        # pass. Earlier passes continue to inform Brain state and can keep ticks
        # warm, but they cannot directly become the final signal.
        await asyncio.sleep(max(0,signal_at-time.time()))
        sent=False

        final_candidates=[
            x for x in candidate_pool.values()
            if int(x.get("qualified_pass") or 0)==5
        ]
        ranked_pool=sorted(
            final_candidates,
            key=lambda x:(
                int(x.get("confidence") or 0),
                float(x.get("strategy_margin") or 0),
                float(x.get("direction_agreement") or 0),
                float(x.get("market_quality") or 0),
                float(x.get("learning_bonus") or 0)
            ),
            reverse=True
        )
        log.info(
            "PREFETCH_POOL_READY cycle=%s candidates=%d final_pass_candidates=%d signal_lead=%ss",
            cycle_id,len(candidate_pool),len(ranked_pool),int(signal_lead)
        )

        if time.time()<=signal_at+0.20:
            for candidate in ranked_pool:
                try:
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

        # Immediately iterate to the next 3-minute target. Because the first
        # scan of the next target is 2m30s before that target, the next cycle
        # is queued as soon as the current signal boundary is released.

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
            # the token belonged to another account. That caused a valid session
            # to be rejected before the account-scoped e:182 asset API was even
            # tested.
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
                STATE["status"]="account_asset_api_failed"
                log.error(
                    "TOKEN_ACCOUNT_ASSET_VALIDATION_FAILED configured_id=%s account_id=%s balance_accounts=%s",
                    expected_account_id,client.account_id,demo_accounts
                )
                raise RuntimeError(
                    f"Authenticated demo account asset scan returned no usable assets for {client.account_id}"
                )
            log.info(
                "TOKEN_ACCOUNT_ASSET_VALIDATED configured_id=%s account_id=%s source=authenticated_websocket:event_182 asset_count=%d",
                expected_account_id,client.account_id,len(STATE["assets"])
            )
            STATE["status"]="live_read_only"
            assets=list(STATE["assets"])
            real_n=sum(a["mode"]=="REAL" for a in assets); otc_n=sum(a["mode"]=="OTC" for a in assets)
            log.info("ACCOUNT_ASSET_SOURCE account_id=%s source_count=%d open_real=%d open_otc=%d open_total=%d",
                     client.account_id,len(assets),real_n,otc_n,len(assets))
            log.info("ALL_ACCOUNT_OPEN_ASSETS_READY count=%d",len(assets))
            await ensure_account_tick_subscriptions()
            restored=await restore_pending_result_watches()
            if restored:
                log.info("RESULT_WATCH_RECOVERY_COMPLETE restored=%d",restored)
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
            STATE["status"]=("account_token_mismatch" if "Access token does not expose configured demo account" in str(e) else "error")
            log.exception("MARKET_WORKER_ERROR %s",e)
            # Connection startup failures are retried quickly so a transient
            # broker websocket drop cannot suppress the next signal cycle.
            retry_delay=4 if "Not connected" in str(e) or "WebSocket dropped" in str(e) else 30
            await asyncio.sleep(retry_delay)
        finally:
            try:await client.stop()
            except Exception:pass
            CLIENT=None

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
    if cmd=="/mbcode":
        if str(uid)!=ADMIN_TELEGRAM_ID:
            await audit_access("UNAUTHORIZED_MBCODE_ATTEMPT",int(uid)); await telegram("❌ Admin only.",chat_id=chat_id); return
        ok=await generate_member_access_code(uid)
        await telegram("✅ Code generated and sent to your admin email." if ok else "❌ Code generation failed.",chat_id=chat_id); return
    if cmd=="/mbaccess":
        parts=txt.split(maxsplit=1); code=parts[1].strip() if len(parts)==2 else ""
        result=await grant_member_access(uid,code)
        messages={"GRANTED":"✅ ACCESS VERIFICATION SUCCESS\n\nAll configured MB members now have access for 24 hours.","DENIED":"❌ Admin only.","INVALID":"❌ Invalid access code.","EXPIRED":"❌ Code expired or no active code.","NO_MEMBERS":"❌ No MB1/MB2/... member IDs configured.","DB_ERROR":"❌ Access system unavailable."}
        await telegram(messages.get(result,"❌ Access verification failed."),chat_id=chat_id); return
    if cmd=="/start":
        await telegram("🔒 NEXORA-AI\n\nAccess is controlled by the administrator.",chat_id=chat_id)
        return



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
        if path == "/ping":
            # Ultra-lightweight keepalive endpoint. It must never trigger
            # market scans, AI review, Telegram delivery, or signal cycles.
            payload=b'{"status":"ok","service":"CANDICE-AI"}'
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "+str(len(payload)).encode()+b"\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n"+payload)
            await writer.drain()
            log.info("PING_REQUEST status=200")
            return
        if path.startswith("/health"):
            body_out=json.dumps({"service":"CANDICE-AI","status":STATE["status"],"read_only":True,"asset_count":len(STATE["assets"]),"qualified":len(STATE["analyses"]),"cycle":STATE["cycle"],"active_results":len(BRAIN.active_signals),"account_id":STATE.get("account_id"),"account_group":STATE.get("account_group"),"feed_source":STATE.get("feed_source"),"network":STATE.get("network",{})}).encode()
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
                msg=upd.get("message") or upd.get("edited_message") or {}
                chat_id=(msg.get("chat") or {}).get("id")
                if chat_id is not None:
                    STATE["telegram_chat_id"]=chat_id
                    log.info("TELEGRAM_CHAT_ID_CAPTURED chat_id=%s",chat_id)
                await handle_telegram_command(msg)
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
    await ensure_ai_review_queue_table()
    await ensure_result_watch_queue_table()
    await ensure_cycle_state_table()
    await ensure_access_table()
    port=int(os.getenv("PORT","10000"));server=await asyncio.start_server(health,"0.0.0.0",port)
    await configure_telegram_webhook()
    await asyncio.gather(market_worker(),account_tick_subscription_worker(),account_live_feed_worker(),cycle_loop_supervisor(),ai_review_worker(),server.serve_forever())
if __name__=="__main__":asyncio.run(main())