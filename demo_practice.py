"""Isolated DEMO practice laboratory.

This module is not part of live signal scheduling. It observes the same closed 1m
market feed but owns its own experimental strategy detectors, approval queue and
learning database. A real broker order can only be sent after an explicit
Telegram human ACCEPT for that exact one-time token, and only to the authenticated
demo account.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import secrets
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque

log = logging.getLogger("candice.practice")

DB_URL = os.getenv("DATABASE_URL", "").strip()
ENABLED = os.getenv("DEMO_PRACTICE_ENABLED", "true").strip().lower() != "false"
PRACTICE_SECONDS = max(300, int(os.getenv("DEMO_PRACTICE_SECONDS", "7200")))
PRACTICE_POLL_SECONDS = max(5, int(os.getenv("DEMO_PRACTICE_POLL_SECONDS", "15")))
PENDING_TTL = max(10, min(60, int(os.getenv("DEMO_PRACTICE_PENDING_TTL", "25"))))
TRADE_AMOUNT = max(0.01, float(os.getenv("DEMO_PRACTICE_AMOUNT", "1")))
TRADE_DURATION = max(30, int(os.getenv("DEMO_PRACTICE_DURATION", "60")))
MIN_CONFIDENCE = max(55, min(95, int(os.getenv("DEMO_PRACTICE_MIN_CONFIDENCE", "70"))))
MIN_SAMPLES = max(10, int(os.getenv("DEMO_PRACTICE_MIN_SAMPLES", "30")))
MIN_WIN_RATE = max(0.50, min(0.95, float(os.getenv("DEMO_PRACTICE_MIN_WIN_RATE", "0.60"))))
MIN_RECENT_RATE = max(0.45, min(0.95, float(os.getenv("DEMO_PRACTICE_MIN_RECENT_RATE", "0.55"))))
MAX_LOSS_STREAK = max(2, int(os.getenv("DEMO_PRACTICE_MAX_LOSS_STREAK", "4")))
RECENT_WINDOW = max(10, int(os.getenv("DEMO_PRACTICE_RECENT_WINDOW", "20")))
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID", "").strip()

# Human approval is a hard-coded safety invariant. There is deliberately no
# environment flag that can silently bypass this requirement.
HUMAN_APPROVAL_REQUIRED = True

SNAPSHOTS: dict[str, dict] = {}
SNAPSHOT_LOCK = asyncio.Lock()
PRACTICE_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="demopractice")
LAST_STATUS = {
    "enabled": ENABLED,
    "practice_active": False,
    "seconds_remaining": 0,
    "pending": 0,
    "active": 0,
    "validated": 0,
    "last_candidate": None,
    "last_result": None,
}

_TABLES_READY = False
_TABLE_LOCK = asyncio.Lock()


def _now() -> float:
    return time.time()


def _db_required() -> bool:
    return bool(DB_URL)


def _normalize_candles(candles, timestamp=None):
    ref = float(timestamp or _now())
    out = []
    for c in candles or []:
        if not isinstance(c, dict):
            continue
        try:
            t = float(c.get("time", c.get("t")))
            o = float(c.get("open", c.get("o")))
            h = float(c.get("high", c.get("h")))
            lo = float(c.get("low", c.get("l")))
            cl = float(c.get("close", c.get("c")))
        except (TypeError, ValueError):
            continue
        if t > 20_000_000_000:
            t /= 1000.0
        # Only complete 1m candles are allowed into the practice detector.
        if t + 60 > ref - 1:
            continue
        out.append({
            "time": int(t // 60) * 60,
            "open": o,
            "high": h,
            "low": lo,
            "close": cl,
        })
    dedup = {x["time"]: x for x in out}
    return sorted(dedup.values(), key=lambda x: x["time"])[-90:]


def _ema(values, period):
    vals = [float(x) for x in values]
    if not vals:
        return 0.0
    k = 2.0 / (period + 1.0)
    e = vals[0]
    for v in vals[1:]:
        e = v * k + e * (1.0 - k)
    return e


def _rsi(values, period=14):
    vals = [float(x) for x in values]
    if len(vals) < period + 1:
        return 50.0
    gains, losses = [], []
    for a, b in zip(vals[-period-1:-1], vals[-period:]):
        d = b - a
        gains.append(max(0.0, d))
        losses.append(max(0.0, -d))
    ag = sum(gains) / period
    al = sum(losses) / period
    return 100.0 if al == 0 else 100.0 - 100.0 / (1.0 + ag / al)


def _atr(candles, period=14):
    if len(candles) < period + 1:
        return 0.0
    tr = []
    for i in range(len(candles) - period, len(candles)):
        cur = candles[i]
        prev = candles[i - 1]
        tr.append(max(
            cur["high"] - cur["low"],
            abs(cur["high"] - prev["close"]),
            abs(cur["low"] - prev["close"]),
        ))
    return sum(tr) / max(1, len(tr))


def _bar_features(c):
    rng = max(1e-12, c["high"] - c["low"])
    body = abs(c["close"] - c["open"]) / rng
    signed = 1 if c["close"] > c["open"] else -1 if c["close"] < c["open"] else 0
    upper = (c["high"] - max(c["open"], c["close"])) / rng
    lower = (min(c["open"], c["close"]) - c["low"]) / rng
    return rng, body, signed, upper, lower


def _candidate(pair, candles):
    if len(candles) < 30:
        return None
    closes = [c["close"] for c in candles]
    last = candles[-1]
    prev = candles[-2]
    rng, body, signed, upper, lower = _bar_features(last)
    _, pbody, psigned, _, _ = _bar_features(prev)
    px = max(abs(last["close"]), 1e-9)
    ema9 = _ema(closes[-35:], 9)
    ema21 = _ema(closes[-35:], 21)
    atr = max(_atr(candles), 1e-12)
    rsi = _rsi(closes)

    candidates = []

    # TREND_FOLLOWING / EMA continuation
    if signed and psigned == signed and body >= 0.55:
        aligned = (ema9 > ema21 and signed > 0) or (ema9 < ema21 and signed < 0)
        if aligned:
            candidates.append({
                "strategy": "TREND_FOLLOWING",
                "technique": "M1_EMA9_EMA21_TWO_BAR_CONTINUATION",
                "direction": "UP" if signed > 0 else "DOWN",
                "confidence": min(94, 72 + int(body * 15) + (8 if abs(ema9 - ema21) / px > 0.0003 else 0)),
                "context": {"body_ratio": round(body, 3), "ema_gap": round((ema9 - ema21) / px, 6),
                            "rsi": round(rsi, 2), "atr": atr},
            })

    # MOMENTUM / two closed bars with expanding range
    p_rng, _, _, _, _ = _bar_features(prev)
    if signed and psigned == signed and body >= 0.55 and rng >= p_rng * 0.95:
        candidates.append({
            "strategy": "MOMENTUM",
            "technique": "M1_TWO_BAR_DIRECTIONAL_CONTINUATION",
            "direction": "UP" if signed > 0 else "DOWN",
            "confidence": min(92, 70 + int(body * 18) + (4 if rng >= p_rng else 0)),
            "context": {"body_ratio": round(body, 3), "range_ratio": round(rng / max(p_rng, 1e-12), 3),
                        "rsi": round(rsi, 2)},
        })

    # BREAKOUT / closed candle over the previous five-bar range.
    lookback = candles[-7:-1]
    if len(lookback) >= 5:
        hi = max(c["high"] for c in lookback)
        lo = min(c["low"] for c in lookback)
        up_dist = (last["close"] - hi) / atr
        down_dist = (lo - last["close"]) / atr
        if up_dist >= 0.15 and body >= 0.55 and rng / atr >= 0.90:
            candidates.append({
                "strategy": "BREAKOUT",
                "technique": "M1_FIVE_BAR_RANGE_BREAKOUT",
                "direction": "UP",
                "confidence": min(95, 74 + int(body * 15) + int(min(6, up_dist * 3))),
                "context": {"breakout_distance": round(up_dist, 3), "body_ratio": round(body, 3),
                            "range_atr": round(rng / atr, 3), "rsi": round(rsi, 2)},
            })
        elif down_dist >= 0.15 and body >= 0.55 and rng / atr >= 0.90:
            candidates.append({
                "strategy": "BREAKOUT",
                "technique": "M1_FIVE_BAR_RANGE_BREAKOUT",
                "direction": "DOWN",
                "confidence": min(95, 74 + int(body * 15) + int(min(6, down_dist * 3))),
                "context": {"breakout_distance": round(down_dist, 3), "body_ratio": round(body, 3),
                            "range_atr": round(rng / atr, 3), "rsi": round(rsi, 2)},
            })

    # PRICE_ACTION / rejection near a recent extreme.
    recent_lows = min(c["low"] for c in candles[-12:-1])
    recent_highs = max(c["high"] for c in candles[-12:-1])
    near_low = (last["low"] - recent_lows) <= max(atr * 0.20, px * 0.00005)
    near_high = (recent_highs - last["high"]) <= max(atr * 0.20, px * 0.00005)
    if lower >= 0.45 and near_low and signed >= 0:
        candidates.append({
            "strategy": "PRICE_ACTION",
            "technique": "M1_LOWER_WICK_REJECTION_AT_RECENT_LOW",
            "direction": "UP",
            "confidence": min(90, 68 + int(lower * 20) + (4 if signed > 0 else 0)),
            "context": {"lower_wick": round(lower, 3), "body_ratio": round(body, 3), "rsi": round(rsi, 2)},
        })
    if upper >= 0.45 and near_high and signed <= 0:
        candidates.append({
            "strategy": "PRICE_ACTION",
            "technique": "M1_UPPER_WICK_REJECTION_AT_RECENT_HIGH",
            "direction": "DOWN",
            "confidence": min(90, 68 + int(upper * 20) + (4 if signed < 0 else 0)),
            "context": {"upper_wick": round(upper, 3), "body_ratio": round(body, 3), "rsi": round(rsi, 2)},
        })

    # MEAN_REVERSION / extreme RSI plus low directional efficiency.
    five = candles[-6:-1]
    displacement = abs(five[-1]["close"] - five[0]["open"]) / max(
        sum(c["high"] - c["low"] for c in five), 1e-12
    )
    if rsi <= 24 and displacement < 0.55 and near_low:
        candidates.append({
            "strategy": "MEAN_REVERSION",
            "technique": "M1_RSI_EXTREME_LOW_DISPLACEMENT_REBOUND",
            "direction": "UP",
            "confidence": min(88, 66 + int((24 - rsi) * 0.8)),
            "context": {"rsi": round(rsi, 2), "displacement": round(displacement, 3)},
        })
    elif rsi >= 76 and displacement < 0.55 and near_high:
        candidates.append({
            "strategy": "MEAN_REVERSION",
            "technique": "M1_RSI_EXTREME_LOW_DISPLACEMENT_REBOUND",
            "direction": "DOWN",
            "confidence": min(88, 66 + int((rsi - 76) * 0.8)),
            "context": {"rsi": round(rsi, 2), "displacement": round(displacement, 3)},
        })

    good = [x for x in candidates if int(x.get("confidence") or 0) >= MIN_CONFIDENCE]
    if not good:
        return None
    good.sort(key=lambda x: (int(x.get("confidence") or 0), x.get("strategy", ""), x.get("technique", "")), reverse=True)
    best = dict(good[0])
    best["pair"] = pair
    best["entry_price"] = float(last["close"])
    best["entry_candle_ts"] = int(last["time"])
    best["created_at"] = _now()
    return best


def record_practice_snapshot(pair, candles, price=None, timestamp=None):
    if not ENABLED:
        return
    cs = _normalize_candles(candles, timestamp)
    if len(cs) < 30:
        return
    SNAPSHOTS[str(pair)] = {
        "pair": str(pair),
        "candles": cs,
        "price": float(price) if price is not None else float(cs[-1]["close"]),
        "timestamp": float(timestamp or _now()),
    }
    # Hard memory bound; practice only needs recent M1 context.
    if len(SNAPSHOTS) > 120:
        oldest = sorted(SNAPSHOTS.items(), key=lambda kv: float(kv[1].get("timestamp", 0)))[:20]
        for p, _ in oldest:
            SNAPSHOTS.pop(p, None)


async def record_practice_snapshot_async(pair, candles, price=None, timestamp=None):
    if not ENABLED:
        return
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        PRACTICE_EXECUTOR, record_practice_snapshot, pair, candles, price, timestamp
    )


def _recent_rate(results):
    vals = list(results or [])[-RECENT_WINDOW:]
    if not vals:
        return 0.0
    wins = sum(1 for x in vals if str(x).upper() == "WIN")
    losses = sum(1 for x in vals if str(x).upper() == "LOSS")
    return wins / max(1, wins + losses)


def _qualification(samples, wins, losses, recent_results, max_loss_streak):
    wr = float(wins) / max(1, int(wins) + int(losses))
    rr = _recent_rate(recent_results)
    return (
        int(samples) >= MIN_SAMPLES
        and wr >= MIN_WIN_RATE
        and rr >= MIN_RECENT_RATE
        and int(max_loss_streak) <= MAX_LOSS_STREAK
    )


async def ensure_tables():
    global _TABLES_READY
    if _TABLES_READY:
        return True
    if not _db_required():
        log.warning("DEMO_PRACTICE_DB_NOT_CONFIGURED")
        return False
    async with _TABLE_LOCK:
        if _TABLES_READY:
            return True
        try:
            import psycopg
            def init():
                with psycopg.connect(DB_URL, connect_timeout=8) as db:
                    with db.cursor() as cur:
                        cur.execute("""
                            CREATE TABLE IF NOT EXISTS candice_practice_orders (
                                practice_id TEXT PRIMARY KEY,
                                approval_token TEXT UNIQUE NOT NULL,
                                pair TEXT NOT NULL,
                                direction TEXT NOT NULL,
                                strategy TEXT NOT NULL,
                                technique TEXT NOT NULL,
                                entry_price DOUBLE PRECISION,
                                entry_candle_ts BIGINT,
                                duration_seconds INTEGER NOT NULL,
                                amount DOUBLE PRECISION NOT NULL,
                                account_id BIGINT NOT NULL,
                                group_name TEXT NOT NULL DEFAULT 'demo',
                                status TEXT NOT NULL DEFAULT 'PENDING',
                                broker_trade_id TEXT UNIQUE,
                                context JSONB NOT NULL DEFAULT '{}'::jsonb,
                                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                approval_expires_at TIMESTAMPTZ NOT NULL,
                                accepted_at TIMESTAMPTZ,
                                executed_at TIMESTAMPTZ,
                                resolved_at TIMESTAMPTZ,
                                result TEXT,
                                pnl DOUBLE PRECISION,
                                error TEXT
                            )
                        """)
                        cur.execute("""
                            CREATE TABLE IF NOT EXISTS candice_strategy_learning (
                                strategy TEXT NOT NULL,
                                technique TEXT NOT NULL,
                                samples INTEGER NOT NULL DEFAULT 0,
                                wins INTEGER NOT NULL DEFAULT 0,
                                losses INTEGER NOT NULL DEFAULT 0,
                                ties INTEGER NOT NULL DEFAULT 0,
                                current_loss_streak INTEGER NOT NULL DEFAULT 0,
                                max_loss_streak INTEGER NOT NULL DEFAULT 0,
                                recent_results JSONB NOT NULL DEFAULT '[]'::jsonb,
                                live_eligible BOOLEAN NOT NULL DEFAULT FALSE,
                                validated_at TIMESTAMPTZ,
                                last_error TEXT,
                                last_lesson TEXT,
                                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                PRIMARY KEY(strategy, technique)
                            )
                        """)
                        cur.execute("""
                            CREATE TABLE IF NOT EXISTS candice_practice_lessons (
                                lesson_id TEXT PRIMARY KEY,
                                practice_id TEXT,
                                strategy TEXT,
                                technique TEXT,
                                result TEXT,
                                lesson TEXT,
                                reuse TEXT,
                                evidence TEXT,
                                confidence INTEGER NOT NULL DEFAULT 0,
                                provider TEXT,
                                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                            )
                        """)
                        cur.execute("""
                            CREATE TABLE IF NOT EXISTS candice_practice_meta (
                                key TEXT PRIMARY KEY,
                                value JSONB NOT NULL
                            )
                        """)
                        cur.execute("""
                            DELETE FROM candice_practice_orders
                            WHERE status IN ('REJECTED','EXPIRED','FAILED','RESOLVED')
                              AND COALESCE(resolved_at, created_at) < NOW() - INTERVAL '2 days'
                        """)
                    db.commit()
            await asyncio.to_thread(init)
            _TABLES_READY = True
            log.info("DEMO_PRACTICE_DB_READY")
            return True
        except Exception as e:
            log.error("DEMO_PRACTICE_DB_INIT_FAILED type=%s message=%s", type(e).__name__, str(e)[:180])
            return False


async def _meta_get(key, default=None):
    if not await ensure_tables():
        return default
    try:
        import psycopg
        def read():
            with psycopg.connect(DB_URL, connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("SELECT value FROM candice_practice_meta WHERE key=%s", (key,))
                    row = cur.fetchone()
                    return row[0] if row else None
        value = await asyncio.to_thread(read)
        return default if value is None else value
    except Exception:
        return default


async def _meta_set(key, value):
    if not await ensure_tables():
        return False
    try:
        import psycopg
        payload = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
        def write():
            with psycopg.connect(DB_URL, connect_timeout=8) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        INSERT INTO candice_practice_meta(key,value)
                        VALUES(%s,%s::jsonb)
                        ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value
                    """, (key, payload))
                db.commit()
        await asyncio.to_thread(write)
        return True
    except Exception:
        return False


async def _practice_anchor():
    val = await _meta_get("daily_anchor_ts", None)
    try:
        anchor = float(val)
    except (TypeError, ValueError):
        anchor = 0.0
    if anchor <= 0:
        anchor = _now()
        await _meta_set("daily_anchor_ts", anchor)
        log.info("DEMO_PRACTICE_ANCHOR_SET ts=%s", datetime.fromtimestamp(anchor, timezone.utc).isoformat())
    return anchor


def practice_window(now=None):
    now = _now() if now is None else float(now)
    # Anchor modulo 24h gives a stable two-hour window every day without making
    # the signal scheduler depend on a practice clock.
    anchor = _LAST_ANCHOR
    if anchor <= 0:
        return False, 0
    elapsed = (now - anchor) % 86400.0
    active = elapsed < PRACTICE_SECONDS
    remaining = int(PRACTICE_SECONDS - elapsed) if active else 0
    return active, max(0, remaining)


_LAST_ANCHOR = 0.0


async def _load_anchor_once():
    global _LAST_ANCHOR
    if _LAST_ANCHOR > 0:
        return _LAST_ANCHOR
    _LAST_ANCHOR = await _practice_anchor()
    return _LAST_ANCHOR


async def _pending_count(account_id):
    if not await ensure_tables():
        return 0
    try:
        import psycopg
        def read():
            with psycopg.connect(DB_URL, connect_timeout=5) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT COUNT(*) FROM candice_practice_orders
                        WHERE account_id=%s AND status IN ('PENDING','EXECUTING','ACTIVE')
                    """, (int(account_id),))
                    return int(cur.fetchone()[0] or 0)
        return await asyncio.to_thread(read)
    except Exception:
        return 0


async def _expire_pending():
    if not await ensure_tables():
        return
    try:
        import psycopg
        def expire():
            with psycopg.connect(DB_URL, connect_timeout=5) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        UPDATE candice_practice_orders
                        SET status='EXPIRED',resolved_at=NOW(),error='human_approval_timeout'
                        WHERE status='PENDING' AND approval_expires_at < NOW()
                    """)
                db.commit()
        await asyncio.to_thread(expire)
    except Exception:
        pass


async def _insert_candidate(candidate, account_id):
    if not await ensure_tables():
        return None
    practice_id = secrets.token_hex(12)
    token = secrets.token_urlsafe(12)
    expiry = datetime.now(timezone.utc) + timedelta(seconds=PENDING_TTL)
    payload = json.dumps(candidate.get("context") or {}, separators=(",", ":"), ensure_ascii=False)
    try:
        import psycopg
        def put():
            with psycopg.connect(DB_URL, connect_timeout=5) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        INSERT INTO candice_practice_orders(
                            practice_id,approval_token,pair,direction,strategy,technique,
                            entry_price,entry_candle_ts,duration_seconds,amount,account_id,
                            group_name,status,context,approval_expires_at
                        ) VALUES(
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'demo','PENDING',%s::jsonb,%s
                        )
                        RETURNING practice_id
                    """, (
                        practice_id, token, candidate["pair"], candidate["direction"],
                        candidate["strategy"], candidate["technique"],
                        candidate.get("entry_price"), candidate.get("entry_candle_ts"),
                        TRADE_DURATION, TRADE_AMOUNT, int(account_id), payload, expiry
                    ))
                db.commit()
                return practice_id
        pid = await asyncio.to_thread(put)
        candidate = dict(candidate)
        candidate["practice_id"] = pid
        candidate["approval_token"] = token
        candidate["approval_expires_at"] = expiry.timestamp()
        return candidate
    except Exception as e:
        log.warning("DEMO_PRACTICE_CANDIDATE_SAVE_FAILED type=%s message=%s", type(e).__name__, str(e)[:160])
        return None


async def _load_pending(token):
    if not await ensure_tables():
        return None
    try:
        import psycopg
        def read():
            with psycopg.connect(DB_URL, connect_timeout=5) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT practice_id,approval_token,pair,direction,strategy,technique,
                               entry_price,entry_candle_ts,duration_seconds,amount,account_id,
                               group_name,status,context,approval_expires_at
                        FROM candice_practice_orders
                        WHERE approval_token=%s
                        FOR UPDATE
                    """, (token,))
                    row = cur.fetchone()
                    if not row:
                        db.rollback()
                        return None
                    cols = ["practice_id","approval_token","pair","direction","strategy","technique",
                            "entry_price","entry_candle_ts","duration_seconds","amount","account_id",
                            "group_name","status","context","approval_expires_at"]
                    return dict(zip(cols, row))
        # A SELECT ... FOR UPDATE held in a short connection is used only to
        # avoid duplicate approvals; the state is revalidated before ordering.
        return await asyncio.to_thread(read)
    except Exception:
        return None


async def practice_accept(token, user_id, client, state):
    token = str(token or "").strip()
    uid = str(user_id or "").strip()
    if not token:
        return "❌ DEMO PRACTICE: approval token missing."
    if not ADMIN_TELEGRAM_ID or uid != ADMIN_TELEGRAM_ID:
        log.warning("DEMO_PRACTICE_UNAUTHORIZED_ACCEPT uid=%s", uid)
        return "❌ DEMO PRACTICE: Admin verification required."
    if not client or not getattr(getattr(client, "connection", None), "is_connected", False):
        return "❌ DEMO PRACTICE: demo broker connection is not ready."
    if str(state.get("account_group") or "").lower() != "demo":
        return "🛑 DEMO PRACTICE: non-demo account blocked."
    account_id = state.get("account_id")
    try:
        account_id = int(account_id)
    except (TypeError, ValueError):
        return "🛑 DEMO PRACTICE: authenticated demo account is unavailable."

    rec = await _load_pending(token)
    if not rec:
        return "⚠️ DEMO PRACTICE: token not found or already consumed."
    if str(rec.get("status")) != "PENDING":
        return f"⚠️ DEMO PRACTICE: token status is {rec.get('status')}."
    if int(rec.get("account_id") or 0) != account_id:
        return "🛑 DEMO PRACTICE: account mismatch blocked."
    if str(rec.get("group_name") or "").lower() != "demo":
        return "🛑 DEMO PRACTICE: broker group mismatch blocked."
    expires = rec.get("approval_expires_at")
    if isinstance(expires, str):
        try:
            expires = datetime.fromisoformat(expires.replace("Z", "+00:00")).timestamp()
        except Exception:
            expires = 0
    if not expires or float(expires) <= _now():
        return "⏰ DEMO PRACTICE: approval expired; no order sent."

    # Human approval is bound to one exact candidate. Mark it EXECUTING before
    # the broker request so a duplicate Telegram delivery cannot place twice.
    try:
        import psycopg
        def mark():
            with psycopg.connect(DB_URL, connect_timeout=5) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        UPDATE candice_practice_orders
                        SET status='EXECUTING',accepted_at=NOW()
                        WHERE practice_id=%s AND status='PENDING'
                    """, (rec["practice_id"],))
                    ok = cur.rowcount == 1
                db.commit()
                return ok
        if not await asyncio.to_thread(mark):
            return "⚠️ DEMO PRACTICE: approval was already consumed."
    except Exception as e:
        log.warning("DEMO_PRACTICE_APPROVAL_LOCK_FAILED type=%s message=%s", type(e).__name__, str(e)[:160])
        return "❌ DEMO PRACTICE: approval lock failed; no order sent."

    try:
        # Permanent execution boundary: only group=demo and the selected account
        # identity are accepted. There is intentionally no real-account fallback.
        result = await client.trade.place_order(
            pair=str(rec["pair"]),
            amount=float(rec["amount"]),
            direction=str(rec["direction"]).lower(),
            duration=int(rec["duration_seconds"]),
            account_id=account_id,
            group="demo",
        )
        trade_id = None
        if isinstance(result, dict):
            trade_id = result.get("id") or result.get("trade_id")
        if trade_id is None:
            raise RuntimeError("demo order returned no trade id")
        trade_id = str(trade_id)
        import psycopg
        def mark_active():
            with psycopg.connect(DB_URL, connect_timeout=5) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        UPDATE candice_practice_orders
                        SET status='ACTIVE',broker_trade_id=%s,executed_at=NOW()
                        WHERE practice_id=%s AND status='EXECUTING'
                    """, (trade_id, rec["practice_id"]))
                db.commit()
        await asyncio.to_thread(mark_active)
        log.info(
            "DEMO_PRACTICE_ORDER_SENT practice_id=%s pair=%s direction=%s duration=%s account_id=%s",
            rec["practice_id"], rec["pair"], rec["direction"], rec["duration_seconds"], account_id
        )
        return (
            "✅ <b>DEMO PRACTICE ACCEPTED</b>\n\n"
            f"📈 {rec['pair']}\n"
            f"➡️ {str(rec['direction']).upper()}\n"
            f"⏱️ {int(rec['duration_seconds'])} SEC\n"
            f"💰 Amount: {float(rec['amount']):g}\n"
            f"🆔 Practice: {rec['practice_id']}\n"
            "🔐 Human approval: VERIFIED\n"
            "🧪 Account: DEMO"
        )
    except Exception as e:
        try:
            import psycopg
            def fail():
                with psycopg.connect(DB_URL, connect_timeout=5) as db:
                    with db.cursor() as cur:
                        cur.execute("""
                            UPDATE candice_practice_orders
                            SET status='FAILED',resolved_at=NOW(),error=%s
                            WHERE practice_id=%s AND status='EXECUTING'
                        """, (str(e)[:500], rec["practice_id"]))
                    db.commit()
            await asyncio.to_thread(fail)
        except Exception:
            pass
        log.warning("DEMO_PRACTICE_ORDER_FAILED practice_id=%s type=%s message=%s",
                    rec["practice_id"], type(e).__name__, str(e)[:180])
        return "❌ DEMO PRACTICE: demo order failed; no retry is performed automatically."


async def practice_reject(token, user_id):
    token = str(token or "").strip()
    uid = str(user_id or "").strip()
    if not token:
        return "❌ DEMO PRACTICE: token missing."
    if not ADMIN_TELEGRAM_ID or uid != ADMIN_TELEGRAM_ID:
        return "❌ DEMO PRACTICE: Admin verification required."
    if not await ensure_tables():
        return "❌ DEMO PRACTICE: learning database unavailable."
    try:
        import psycopg
        def reject():
            with psycopg.connect(DB_URL, connect_timeout=5) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        UPDATE candice_practice_orders
                        SET status='REJECTED',resolved_at=NOW(),error='human_rejected'
                        WHERE approval_token=%s AND status='PENDING'
                    """, (token,))
                    ok = cur.rowcount == 1
                db.commit()
                return ok
        ok = await asyncio.to_thread(reject)
        return "❌ DEMO PRACTICE: candidate rejected; no order sent." if ok else "⚠️ DEMO PRACTICE: token not found/consumed."
    except Exception as e:
        log.warning("DEMO_PRACTICE_REJECT_FAILED type=%s message=%s", type(e).__name__, str(e)[:160])
        return "❌ DEMO PRACTICE: rejection failed."


def _result_from_trade(info):
    status = str((info or {}).get("status") or "").upper()
    if status in {"WIN","WON","PROFIT","SUCCESS"}:
        return "WIN"
    if status in {"LOSS","LOST","FAILED"}:
        return "LOSS"
    if status in {"TIE","DRAW","REFUND","BREAKEVEN"}:
        return "TIE"
    pnl = (info or {}).get("balance_change")
    try:
        pnl = float(pnl)
        if pnl > 0:
            return "WIN"
        if pnl < 0:
            return "LOSS"
    except (TypeError, ValueError):
        pass
    return None


async def _apply_result(rec, result, pnl=None):
    if not await ensure_tables():
        return
    try:
        import psycopg
        recent = []
        def update():
            with psycopg.connect(DB_URL, connect_timeout=5) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT samples,wins,losses,ties,current_loss_streak,max_loss_streak,recent_results
                        FROM candice_strategy_learning
                        WHERE strategy=%s AND technique=%s
                    """, (rec["strategy"], rec["technique"]))
                    row = cur.fetchone()
                    samples,wins,losses,ties,cur_streak,max_streak,raw_recent = row or (0,0,0,0,0,0,[])
                    try:
                        recent = list(raw_recent or [])
                    except Exception:
                        recent = []
                    samples = int(samples or 0) + 1
                    wins = int(wins or 0) + (1 if result == "WIN" else 0)
                    losses = int(losses or 0) + (1 if result == "LOSS" else 0)
                    ties = int(ties or 0) + (1 if result == "TIE" else 0)
                    if result == "LOSS":
                        cur_streak = int(cur_streak or 0) + 1
                        max_streak = max(int(max_streak or 0), cur_streak)
                    else:
                        cur_streak = 0
                    recent.append(result)
                    recent = recent[-RECENT_WINDOW:]
                    wr = wins / max(1, wins + losses)
                    rr = _recent_rate(recent)
                    live_ok = _qualification(samples,wins,losses,recent,max_streak)
                    validated_at = datetime.now(timezone.utc) if live_ok else None
                    cur.execute("""
                        INSERT INTO candice_strategy_learning(
                            strategy,technique,samples,wins,losses,ties,current_loss_streak,
                            max_loss_streak,recent_results,live_eligible,validated_at,last_error,updated_at
                        ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,NOW())
                        ON CONFLICT(strategy,technique) DO UPDATE SET
                            samples=EXCLUDED.samples,wins=EXCLUDED.wins,losses=EXCLUDED.losses,
                            ties=EXCLUDED.ties,current_loss_streak=EXCLUDED.current_loss_streak,
                            max_loss_streak=EXCLUDED.max_loss_streak,recent_results=EXCLUDED.recent_results,
                            live_eligible=EXCLUDED.live_eligible,
                            validated_at=CASE WHEN EXCLUDED.live_eligible THEN NOW() ELSE NULL END,
                            updated_at=NOW()
                    """, (
                        rec["strategy"], rec["technique"], samples, wins, losses, ties,
                        cur_streak, max_streak, json.dumps(recent), live_ok,
                        validated_at, None
                    ))
                    cur.execute("""
                        UPDATE candice_practice_orders
                        SET status='RESOLVED',resolved_at=NOW(),result=%s,pnl=%s
                        WHERE practice_id=%s AND status='ACTIVE'
                    """, (result, pnl, rec["practice_id"]))
                db.commit()
            return {"samples":samples,"wins":wins,"losses":losses,"ties":ties,
                    "win_rate":wr,"recent_rate":rr,"live_eligible":live_ok,
                    "max_loss_streak":max_streak}
        stats = await asyncio.to_thread(update)
        LAST_STATUS["last_result"] = {
            "practice_id": rec["practice_id"], "strategy": rec["strategy"],
            "technique": rec["technique"], "result": result, **stats
        }
        log.info(
            "DEMO_PRACTICE_RESULT practice_id=%s strategy=%s technique=%s result=%s "
            "samples=%s win_rate=%.3f recent_rate=%.3f live_eligible=%s max_loss_streak=%s",
            rec["practice_id"],rec["strategy"],rec["technique"],result,
            stats["samples"],stats["win_rate"],stats["recent_rate"],
            stats["live_eligible"],stats["max_loss_streak"]
        )
    except Exception as e:
        log.warning("DEMO_PRACTICE_RESULT_STORE_FAILED practice_id=%s type=%s message=%s",
                    rec.get("practice_id"), type(e).__name__, str(e)[:180])


async def handle_practice_trade_event(message):
    if not message or not await ensure_tables():
        return None
    event_code = message.get("e")
    if event_code not in {21,22,26}:
        return None
    data = message.get("d") or []
    if isinstance(data, list):
        info = data[0] if data else {}
    else:
        info = data if isinstance(data, dict) else {}
    trade_id = info.get("id") if isinstance(info, dict) else None
    if trade_id is None:
        return None
    trade_id = str(trade_id)
    try:
        import psycopg
        def read():
            with psycopg.connect(DB_URL, connect_timeout=5) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT practice_id,pair,direction,strategy,technique,amount,duration_seconds,
                               entry_price,account_id,status
                        FROM candice_practice_orders
                        WHERE broker_trade_id=%s AND status IN ('ACTIVE','EXECUTING')
                    """, (trade_id,))
                    row = cur.fetchone()
                    return row
        row = await asyncio.to_thread(read)
    except Exception:
        return None
    if not row:
        return None
    if event_code == 22:
        return (
            "🧪 <b>DEMO PRACTICE TRADE ACCEPTED BY BROKER</b>\n\n"
            f"📈 {row[1]}\n➡️ {str(row[2]).upper()}\n"
            f"🧠 {row[3]}\n🔧 {row[4]}\n"
            f"⏱️ {row[7] if row[7] is not None else '—'} → {row[6]} sec"
        )
    if event_code != 26:
        return None
    result = _result_from_trade(info)
    if result is None:
        log.warning("DEMO_PRACTICE_RESULT_UNKNOWN trade_id=%s status=%s", trade_id, info.get("status"))
        return None
    rec = {
        "practice_id": row[0], "pair": row[1], "direction": row[2],
        "strategy": row[3], "technique": row[4], "amount": row[5],
        "duration_seconds": row[6], "entry_price": row[7], "account_id": row[8]
    }
    pnl = info.get("balance_change")
    await _apply_result(rec, result, pnl)
    icon = "🟢" if result == "WIN" else "🔴" if result == "LOSS" else "🟡"
    return (
        "🧠 <b>DEMO PRACTICE RESULT</b>\n\n"
        f"📈 {rec['pair']}\n➡️ {str(rec['direction']).upper()}\n"
        f"🧠 Strategy: {rec['strategy']}\n"
        f"🔧 Technique: {rec['technique']}\n\n"
        f"{icon} <b>{result}</b>\n"
        "📚 Result stored in Practice Learning DB only."
    )


async def record_learning_lesson(rec, review):
    """Store post-result lessons in practice learning storage, never in live BRAIN."""
    if not await ensure_tables() or not isinstance(review, dict):
        return False
    try:
        import psycopg
        lesson_id = str(review.get("review_id") or secrets.token_hex(10))
        def write():
            with psycopg.connect(DB_URL, connect_timeout=5) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        INSERT INTO candice_practice_lessons(
                            lesson_id,practice_id,strategy,technique,result,lesson,reuse,evidence,confidence,provider
                        ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT(lesson_id) DO NOTHING
                    """, (
                        lesson_id,rec.get("practice_id") or rec.get("cycle_id"),
                        rec.get("strategy"),rec.get("technique") or rec.get("strategy"),
                        rec.get("result"),str(review.get("lesson") or "")[:1000],
                        str(review.get("reuse") or "")[:800],str(review.get("evidence") or "")[:1000],
                        max(0,min(100,int(review.get("confidence") or 0))),
                        str(review.get("provider") or "local")[:80],
                    ))
                db.commit()
        await asyncio.to_thread(write)
        return True
    except Exception:
        return False


async def _select_candidate(state):
    # Never invoke candice_brain/analyze_asset here. Practice is deliberately an
    # independent laboratory and cannot mutate the live signal decision path.
    items = []
    async with SNAPSHOT_LOCK:
        values = list(SNAPSHOTS.values())
    for snap in values:
        pair = str(snap.get("pair") or "")
        try:
            candidate = _candidate(pair, list(snap.get("candles") or []))
        except Exception as e:
            log.debug("DEMO_PRACTICE_DETECTOR_ERROR pair=%s type=%s", pair, type(e).__name__)
            continue
        if not candidate:
            continue
        # The practice account must already be authenticated as demo.
        items.append(candidate)
    if not items:
        return None
    items.sort(key=lambda x: (int(x.get("confidence") or 0), x["strategy"], x["technique"]), reverse=True)
    return items[0]


async def demo_practice_loop(state_getter, client_getter, notifier):
    global LAST_STATUS
    if not ENABLED:
        log.info("DEMO_PRACTICE_DISABLED")
        return
    if not await ensure_tables():
        return
    await _load_anchor_once()
    log.info(
        "DEMO_PRACTICE_READY window_seconds=%s human_approval=%s amount=%s duration=%s min_samples=%s",
        PRACTICE_SECONDS,HUMAN_APPROVAL_REQUIRED,TRADE_AMOUNT,TRADE_DURATION,MIN_SAMPLES
    )
    while True:
        try:
            await _expire_pending()
            anchor = await _load_anchor_once()
            active, remaining = practice_window()
            LAST_STATUS["practice_active"] = active
            LAST_STATUS["seconds_remaining"] = remaining
            state = state_getter() or {}
            client = client_getter()
            demo_ready = (
                bool(client)
                and str(state.get("account_group") or "").lower() == "demo"
                and state.get("account_id") is not None
            )
            if active and demo_ready:
                account_id = int(state["account_id"])
                pending = await _pending_count(account_id)
                LAST_STATUS["pending"] = pending
                if pending == 0:
                    candidate = await _select_candidate(state)
                    if candidate:
                        saved = await _insert_candidate(candidate, account_id)
                        if saved:
                            LAST_STATUS["last_candidate"] = saved
                            msg = (
                                "🧪 <b>CANDICE DEMO PRACTICE</b>\n\n"
                                f"📈 <b>{saved['pair']}</b>\n"
                                f"➡️ <b>{saved['direction']}</b>\n"
                                f"🧠 Strategy: {saved['strategy']}\n"
                                f"🔧 Technique: {saved['technique']}\n"
                                f"🎯 Confidence: {saved['confidence']}%\n"
                                f"💰 Entry: {saved['entry_price']}\n"
                                f"⏱️ Duration: {TRADE_DURATION} SEC\n\n"
                                "⚠️ <b>HUMAN VERIFICATION REQUIRED</b>\n"
                                f"✅ Accept: <code>/practice_accept {saved['approval_token']}</code>\n"
                                f"❌ Reject: <code>/practice_reject {saved['approval_token']}</code>\n"
                                f"⏳ Approval expires in {PENDING_TTL} sec\n"
                                "🧪 DEMO ACCOUNT ONLY • NO MARTINGALE"
                            )
                            await notifier(msg, "DEMO_PRACTICE_REQUEST")
            # Keep status counts cheap; DB is queried only at the next loop.
            LAST_STATUS["active"] = await _pending_count(int(state["account_id"])) if demo_ready else 0
            await asyncio.sleep(PRACTICE_POLL_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("DEMO_PRACTICE_LOOP_ERROR type=%s message=%s", type(e).__name__, str(e)[:180])
            await asyncio.sleep(PRACTICE_POLL_SECONDS)


def practice_status():
    return dict(LAST_STATUS)
