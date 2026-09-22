"""Validated strategy knowledge bridge.

Only compact, already-validated strategy rows from the isolated demo-practice
database are exposed to the live strategy router. No raw practice order, raw
research page, error record or external AI output crosses this boundary.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time

log = logging.getLogger("candice.strategy_knowledge")

DB_URL = os.getenv("DATABASE_URL", "").strip()
REFRESH_SECONDS = max(30, int(os.getenv("STRATEGY_KNOWLEDGE_REFRESH_SECONDS", "60")))
_CACHE = {}
_META = {"updated_at": 0.0, "rows": 0, "db_ok": False}
_LOCK = asyncio.Lock()


def validated_strategy_priors():
    return dict(_CACHE)


def strategy_knowledge_status():
    return {"priors": dict(_CACHE), **_META}


async def refresh_validated_strategy_knowledge():
    if not DB_URL:
        _META["db_ok"] = False
        return False
    try:
        import psycopg
        def read():
            with psycopg.connect(DB_URL, connect_timeout=5) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT strategy,technique,samples,wins,losses,live_eligible,
                               updated_at
                        FROM candice_strategy_learning
                        WHERE live_eligible=TRUE
                          AND samples >= 30
                    """)
                    return cur.fetchall()
        rows = await asyncio.wait_for(asyncio.to_thread(read), timeout=1.0)
        fresh = {}
        for strategy, technique, samples, wins, losses, live_eligible, updated_at in rows:
            if not live_eligible:
                continue
            try:
                samples_i = int(samples or 0)
                wr = float(wins or 0) / max(1, int(wins or 0) + int(losses or 0))
            except Exception:
                continue
            if samples_i < 30 or wr < 0.60:
                continue
            # Small positive bounded strategy prior. This never supplies a
            # direction and never bypasses the technical Brain's hard gates.
            bonus = min(4.0, max(0.5, 0.5 + (wr - 0.60) * 8.0))
            key = str(strategy or "").upper()
            fresh[key] = max(fresh.get(key, 0.0), bonus)
        async with _LOCK:
            _CACHE.clear()
            _CACHE.update(fresh)
            _META["updated_at"] = time.time()
            _META["rows"] = len(rows)
            _META["db_ok"] = True
        log.info("STRATEGY_KNOWLEDGE_REFRESH rows=%s validated_strategies=%s priors=%s",
                 len(rows), len(fresh), fresh)
        return True
    except Exception as e:
        # Fail-stale: retain the last validated overlay. The signal path never
        # waits on this task and never loses a previous validated knowledge set
        # because the learning DB is temporarily unavailable.
        _META["db_ok"] = False
        log.warning("STRATEGY_KNOWLEDGE_REFRESH_FAILED type=%s message=%s", type(e).__name__, str(e)[:160])
        return False


async def strategy_knowledge_loop():
    await refresh_validated_strategy_knowledge()
    while True:
        try:
            await asyncio.sleep(REFRESH_SECONDS)
            await refresh_validated_strategy_knowledge()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("STRATEGY_KNOWLEDGE_LOOP_ERROR type=%s message=%s", type(e).__name__, str(e)[:160])
            await asyncio.sleep(REFRESH_SECONDS)
