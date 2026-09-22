"""Compact validated strategy/technique knowledge shared with the live Brain.

The learning lab is the writer. The signal Brain only reads an in-memory snapshot.
No database I/O is performed from self_strategy.discover().
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from collections import defaultdict

DB_URL = os.getenv("DATABASE_URL", "").strip()
VERSION = "KNOWLEDGE-V1"
_REFRESH_SECONDS = 120.0
_CACHE: dict[str, dict] = {}
_CACHE_TS = 0.0
_REFRESH_LOCK = asyncio.Lock()

def _rate(wins: float, samples: float) -> float:
    return (float(wins) + 0.5) / (float(samples) + 1.0) if samples > 0 else 0.5

def _signature(context: dict | None) -> str:
    c = context or {}
    return "|".join([
        str(c.get("trend_15m") or "UNKNOWN").upper(),
        str(c.get("structure_1m") or "UNKNOWN").upper(),
        str(c.get("pattern") or "UNKNOWN").upper(),
        str(c.get("donchian_state") or "UNKNOWN").upper(),
        "EXPANDING" if bool(c.get("donchian_expansion")) else "FLAT",
        str(c.get("stochastic_cross") or "NEUTRAL").upper(),
    ])

def _status(samples: int, wins: int, losses: int, recent: str) -> str:
    if samples < 20:
        return "CANDIDATE"
    acc = wins / max(1, wins + losses)
    streak = 0
    for ch in reversed(recent[-30:]):
        if ch == "L":
            streak += 1
        elif ch in "WT":
            break
    if acc >= 0.60 and streak < 4:
        return "VALIDATED"
    return "RESEARCH_REJECT"

def _rebuild_cache(rows):
    global _CACHE, _CACHE_TS
    cache = {}
    for row in rows:
        sid = str(row[0])
        try:
            payload = row[8] if isinstance(row[8], dict) else json.loads(row[8] or "{}")
        except Exception:
            payload = {}
        recent = str(row[6] or "")
        samples = int(row[1] or 0)
        wins = int(row[2] or 0)
        losses = int(row[3] or 0)
        status = str(row[7] or _status(samples, wins, losses, recent))
        cache[sid] = {
            "samples": samples,
            "wins": wins,
            "losses": losses,
            "ties": int(row[4] or 0),
            "recent": recent,
            "status": status,
            "payload": payload,
        }
    _CACHE = cache
    _CACHE_TS = time.time()

async def ensure_tables():
    if not DB_URL:
        return False
    import psycopg
    def init():
        with psycopg.connect(DB_URL, connect_timeout=8) as db:
            with db.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS nexora_strategy_knowledge (
                        strategy_id TEXT PRIMARY KEY,
                        samples INTEGER NOT NULL DEFAULT 0,
                        wins INTEGER NOT NULL DEFAULT 0,
                        losses INTEGER NOT NULL DEFAULT 0,
                        ties INTEGER NOT NULL DEFAULT 0,
                        recent TEXT NOT NULL DEFAULT '',
                        status TEXT NOT NULL DEFAULT 'CANDIDATE',
                        version TEXT NOT NULL DEFAULT 'KNOWLEDGE-V1',
                        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS nexora_strategy_techniques (
                        strategy_id TEXT NOT NULL,
                        signature TEXT NOT NULL,
                        samples INTEGER NOT NULL DEFAULT 0,
                        wins INTEGER NOT NULL DEFAULT 0,
                        losses INTEGER NOT NULL DEFAULT 0,
                        ties INTEGER NOT NULL DEFAULT 0,
                        recent TEXT NOT NULL DEFAULT '',
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY(strategy_id, signature)
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS nexora_learning_errors (
                        strategy_id TEXT NOT NULL,
                        error_code TEXT NOT NULL,
                        signature TEXT NOT NULL DEFAULT '',
                        occurrences INTEGER NOT NULL DEFAULT 0,
                        loss_count INTEGER NOT NULL DEFAULT 0,
                        last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY(strategy_id, error_code, signature)
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS nexora_learning_practice_log (
                        id BIGSERIAL PRIMARY KEY,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        strategy_id TEXT NOT NULL,
                        pair TEXT,
                        result TEXT,
                        confidence INTEGER,
                        signature TEXT,
                        error_code TEXT
                    )
                """)
                cur.execute("""
                    DELETE FROM nexora_learning_practice_log
                    WHERE id NOT IN (
                        SELECT id FROM nexora_learning_practice_log
                        ORDER BY id DESC LIMIT 500
                    )
                """)
            db.commit()
    await asyncio.to_thread(init)
    return True

async def refresh(force: bool = False):
    global _CACHE_TS
    if not DB_URL:
        return False
    if not force and time.time() - _CACHE_TS < _REFRESH_SECONDS:
        return True
    async with _REFRESH_LOCK:
        if not force and time.time() - _CACHE_TS < _REFRESH_SECONDS:
            return True
        import psycopg
        def read():
            with psycopg.connect(DB_URL, connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT strategy_id,samples,wins,losses,ties,recent,status,version,payload
                        FROM nexora_strategy_knowledge
                    """)
                    return cur.fetchall()
        try:
            rows = await asyncio.to_thread(read)
            _rebuild_cache(rows)
            return True
        except Exception:
            return False

async def refresh_loop():
    while True:
        try:
            await refresh(force=True)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        await asyncio.sleep(_REFRESH_SECONDS)

def knowledge_priors(context: dict | None = None) -> dict[str, float]:
    """Zero-I/O bounded priors from already validated knowledge only."""
    out = {}
    for sid, b in _CACHE.items():
        if b.get("status") != "VALIDATED":
            continue
        n = int(b.get("samples") or 0)
        if n < 20:
            continue
        acc = _rate(b.get("wins", 0), n)
        if acc < 0.60:
            continue
        # Base validated strategy prior is deliberately small.
        out[sid] = round(min(2.0, max(0.0, (acc - 0.60) * 20.0)), 2)

    c = context or {}
    sig = _signature(c)
    for sid in list(out):
        try:
            import_cache = TECHNIQUE_CACHE.get((sid, sig))
        except NameError:
            import_cache = None
        if import_cache:
            n, w, l = import_cache
            if n >= 12 and (w / max(1, w + l)) >= 0.65:
                out[sid] = round(min(3.0, out[sid] + 1.0), 2)
    return out

TECHNIQUE_CACHE = {}

def reload_technique_cache(rows):
    TECHNIQUE_CACHE.clear()
    for strategy_id, signature, samples, wins, losses in rows:
        TECHNIQUE_CACHE[(str(strategy_id), str(signature))] = (
            int(samples or 0), int(wins or 0), int(losses or 0)
        )

async def record_practice_result(strategy_id: str, result: str, *, pair="", confidence=0,
                                 context: dict | None = None, error_code=""):
    if not DB_URL or not strategy_id:
        return False
    result = str(result or "").upper()
    if result not in {"WIN", "LOSS", "TIE"}:
        return False
    sig = _signature(context)
    payload = {
        "technique_signature": sig,
        "last_pair": str(pair or ""),
        "last_confidence": int(confidence or 0),
        "source": "DEMO_HUMAN_APPROVED_PRACTICE",
    }
    import psycopg
    def write():
        with psycopg.connect(DB_URL, connect_timeout=8) as db:
            with db.cursor() as cur:
                cur.execute("""
                    INSERT INTO nexora_strategy_knowledge(strategy_id,payload)
                    VALUES(%s,%s::jsonb)
                    ON CONFLICT(strategy_id) DO NOTHING
                """, (strategy_id, json.dumps(payload, separators=(",", ":"))))
                cur.execute("""
                    SELECT samples,wins,losses,ties,recent
                    FROM nexora_strategy_knowledge
                    WHERE strategy_id=%s
                    FOR UPDATE
                """, (strategy_id,))
                samples,wins,losses,ties,recent = cur.fetchone()
                samples += 1
                if result == "WIN":
                    wins += 1; token = "W"
                elif result == "LOSS":
                    losses += 1; token = "L"
                else:
                    ties += 1; token = "T"
                recent = (str(recent or "") + token)[-50:]
                status = _status(samples,wins,losses,recent)
                cur.execute("""
                    UPDATE nexora_strategy_knowledge
                    SET samples=%s,wins=%s,losses=%s,ties=%s,recent=%s,status=%s,
                        version=%s,payload=%s::jsonb,updated_at=NOW()
                    WHERE strategy_id=%s
                """, (samples,wins,losses,ties,recent,status,VERSION,
                      json.dumps(payload,separators=(",", ":")),strategy_id))
                cur.execute("""
                    INSERT INTO nexora_strategy_techniques(strategy_id,signature)
                    VALUES(%s,%s)
                    ON CONFLICT(strategy_id,signature) DO NOTHING
                """, (strategy_id,sig))
                cur.execute("""
                    SELECT samples,wins,losses,ties,recent
                    FROM nexora_strategy_techniques
                    WHERE strategy_id=%s AND signature=%s
                    FOR UPDATE
                """, (strategy_id,sig))
                tn,tw,tl,tt,tr = cur.fetchone()
                tn += 1
                if result == "WIN": tw += 1; tr=(str(tr or "")+"W")[-50:]
                elif result == "LOSS": tl += 1; tr=(str(tr or "")+"L")[-50:]
                else: tt += 1; tr=(str(tr or "")+"T")[-50:]
                cur.execute("""
                    UPDATE nexora_strategy_techniques
                    SET samples=%s,wins=%s,losses=%s,ties=%s,recent=%s,updated_at=NOW()
                    WHERE strategy_id=%s AND signature=%s
                """,(tn,tw,tl,tt,tr,strategy_id,sig))
                if result == "LOSS":
                    code = str(error_code or "LOSS_CONTEXT")[:80]
                    cur.execute("""
                        INSERT INTO nexora_learning_errors(strategy_id,error_code,signature,occurrences,loss_count)
                        VALUES(%s,%s,%s,1,1)
                        ON CONFLICT(strategy_id,error_code,signature)
                        DO UPDATE SET occurrences=nexora_learning_errors.occurrences+1,
                                      loss_count=nexora_learning_errors.loss_count+1,
                                      last_seen=NOW()
                    """,(strategy_id,code,sig))
                cur.execute("""
                    INSERT INTO nexora_learning_practice_log(strategy_id,pair,result,confidence,signature,error_code)
                    VALUES(%s,%s,%s,%s,%s,%s)
                """,(strategy_id,str(pair or ""),result,int(confidence or 0),sig,str(error_code or "")[:80]))
                cur.execute("""
                    DELETE FROM nexora_learning_practice_log
                    WHERE id NOT IN (
                        SELECT id FROM nexora_learning_practice_log
                        ORDER BY id DESC LIMIT 500
                    )
                """)
            db.commit()
    try:
        await asyncio.to_thread(write)
        await refresh(force=True)
        return True
    except Exception:
        return False

async def save_error(strategy_id: str, error_code: str, context: dict | None = None):
    if not DB_URL or not strategy_id or not error_code:
        return False
    sig = _signature(context)
    import psycopg
    def write():
        with psycopg.connect(DB_URL, connect_timeout=8) as db:
            with db.cursor() as cur:
                cur.execute("""
                    INSERT INTO nexora_learning_errors(strategy_id,error_code,signature,occurrences,loss_count)
                    VALUES(%s,%s,%s,1,0)
                    ON CONFLICT(strategy_id,error_code,signature)
                    DO UPDATE SET occurrences=nexora_learning_errors.occurrences+1,last_seen=NOW()
                """,(strategy_id,str(error_code)[:80],sig))
            db.commit()
    try:
        await asyncio.to_thread(write)
        return True
    except Exception:
        return False
