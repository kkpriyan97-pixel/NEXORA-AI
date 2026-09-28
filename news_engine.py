"""Non-blocking macro-news context and short-horizon forecast helpers for NEXORA/Candice.

News is context only. It never rewrites the deterministic Candice Brain direction.
External calendar access is optional and disabled unless configured.
"""

from __future__ import annotations

import asyncio
import math
import os
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import Any

import httpx


@dataclass(frozen=True)
class NewsEvent:
    event_id: str
    timestamp_utc: float
    country: str
    currency: str
    event: str
    impact: int
    actual: float | None
    forecast: float | None
    previous: float | None
    source: str = ""


def _clean_num(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text or text.lower() in {"n/a", "na", "null", "-", "--"}:
        return None
    text = text.replace("%", "").replace("$", "").replace("€", "").replace("£", "")
    try:
        return float(text)
    except (TypeError, ValueError):
        m = re.search(r"[-+]?\d+(?:\.\d+)?", text)
        try:
            return float(m.group(0)) if m else None
        except (TypeError, ValueError):
            return None


def _parse_ts(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).timestamp()
    except ValueError:
        try:
            return float(text)
        except (TypeError, ValueError):
            return None


def _currency_from_row(row: dict[str, Any]) -> str:
    for key in ("Currency", "currency", "CountryCode", "country_code"):
        value = str(row.get(key) or "").strip().upper()
        if value and len(value) == 3:
            return value
    country = str(row.get("Country") or row.get("country") or "").strip().lower()
    mapping = {
        "united states": "USD",
        "euro area": "EUR",
        "germany": "EUR",
        "france": "EUR",
        "italy": "EUR",
        "spain": "EUR",
        "united kingdom": "GBP",
        "japan": "JPY",
        "australia": "AUD",
        "new zealand": "NZD",
        "canada": "CAD",
        "switzerland": "CHF",
        "china": "CNY",
    }
    return mapping.get(country, "")


def normalize_event(row: dict[str, Any], source: str = "") -> NewsEvent | None:
    ts = _parse_ts(row.get("Date", row.get("date", row.get("timestamp"))))
    if ts is None:
        return None
    impact_raw = row.get("Importance", row.get("importance", row.get("impact", 0)))
    try:
        impact = int(float(impact_raw or 0))
    except (TypeError, ValueError):
        impact = 0
    if impact >= 3:
        impact = 3
    elif impact == 2:
        impact = 2
    else:
        impact = 1 if impact else 0

    event_name = str(
        row.get("Event")
        or row.get("event")
        or row.get("Category")
        or row.get("category")
        or "Economic event"
    ).strip()
    country = str(row.get("Country") or row.get("country") or "").strip()
    event_id = str(
        row.get("CalendarID")
        or row.get("CalendarId")
        or row.get("calendar_id")
        or f"{country}:{event_name}:{int(ts)}"
    )
    return NewsEvent(
        event_id=event_id,
        timestamp_utc=float(ts),
        country=country,
        currency=_currency_from_row(row),
        event=event_name,
        impact=impact,
        actual=_clean_num(row.get("Actual", row.get("actual"))),
        forecast=_clean_num(row.get("Forecast", row.get("forecast"))),
        previous=_clean_num(row.get("Previous", row.get("previous"))),
        source=source,
    )


def parse_calendar_payload(payload: Any, source: str = "") -> list[NewsEvent]:
    if isinstance(payload, dict):
        for key in ("data", "results", "calendar", "events"):
            if isinstance(payload.get(key), list):
                payload = payload[key]
                break
        else:
            payload = [payload]
    if not isinstance(payload, list):
        return []
    result: list[NewsEvent] = []
    seen: set[str] = set()
    for row in payload:
        if not isinstance(row, dict):
            continue
        event = normalize_event(row, source=source)
        if event and event.event_id not in seen:
            seen.add(event.event_id)
            result.append(event)
    return sorted(result, key=lambda x: x.timestamp_utc)


def event_surprise(event: NewsEvent) -> float:
    if event.actual is None or event.forecast is None:
        return 0.0
    scale = max(
        abs(event.forecast),
        abs(event.previous or 0.0),
        1.0,
    )
    return max(-100.0, min(100.0, (event.actual - event.forecast) / scale * 100.0))


def pair_currencies(pair: str) -> set[str]:
    p = str(pair or "").upper().replace("_OTC", "")
    found = set()
    if len(p) >= 6:
        found.add(p[:3])
        found.add(p[3:6])
    return {x for x in found if len(x) == 3}


def news_context(pair: str, now_ts: float, events: list[NewsEvent]) -> dict[str, Any]:
    currencies = pair_currencies(pair)
    relevant = [e for e in events if e.currency and e.currency in currencies and e.impact >= 2]
    active: list[dict[str, Any]] = []
    risk = 0.0
    phase = "CLEAR"

    for event in relevant:
        delta = float(now_ts) - event.timestamp_utc
        if -900.0 <= delta <= 900.0:
            if delta < -30.0:
                local_phase = "PRE"
                local_risk = 60.0 if event.impact == 2 else 80.0
            elif -30.0 <= delta <= 90.0:
                local_phase = "RELEASE"
                local_risk = 92.0 if event.impact == 3 else 82.0
            else:
                local_phase = "POST"
                local_risk = 70.0 if event.impact == 3 else 55.0
            phase_rank = {"CLEAR": 0, "PRE": 1, "POST": 2, "RELEASE": 3}
            if phase_rank[local_phase] > phase_rank[phase]:
                phase = local_phase
            risk = max(risk, local_risk)
            active.append({
                "event": event.event,
                "currency": event.currency,
                "impact": event.impact,
                "seconds_from_release": round(delta, 1),
                "surprise_pct": round(event_surprise(event), 3),
                "actual": event.actual,
                "forecast": event.forecast,
                "previous": event.previous,
                "source": event.source,
            })

    return {
        "phase": phase,
        "risk": round(min(100.0, risk), 1),
        "relevant_events": active,
        "has_high_impact": any(int(x["impact"]) >= 3 for x in active),
        "max_abs_surprise_pct": round(
            max((abs(float(x.get("surprise_pct") or 0.0)) for x in active), default=0.0),
            3,
        ),
    }


class NewsCalendarClient:
    def __init__(self) -> None:
        self.api_key = os.getenv("TRADING_ECONOMICS_API_KEY", "").strip()
        self.custom_url = os.getenv("NEXORA_NEWS_CALENDAR_URL", "").strip()
        self.timeout = max(2.0, min(8.0, float(os.getenv("NEXORA_NEWS_TIMEOUT", "5") or 5)))
        self.lookahead_hours = max(2, min(48, int(os.getenv("NEXORA_NEWS_LOOKAHEAD_HOURS", "24") or 24)))

    @property
    def enabled(self) -> bool:
        return bool(self.api_key or self.custom_url)

    async def fetch(self) -> tuple[list[NewsEvent], str]:
        if not self.enabled:
            return [], "disabled"
        url = self.custom_url
        headers: dict[str, str] = {}
        if self.api_key and not url:
            # Trading Economics documents the /calendar endpoint and API-key
            # authentication. We request high-impact events only to keep the
            # refresh payload small and the 5M scheduler isolated from network cost.
            url = (
                "https://api.tradingeconomics.com/calendar"
                f"?c={self.api_key}&importance=3&f=json"
            )
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(connect=2.5, read=self.timeout, write=2.5, pool=1.5),
                headers=headers,
            ) as client:
                response = await client.get(url)
                response.raise_for_status()
                payload = response.json()
            events = parse_calendar_payload(payload, source="tradingeconomics" if self.api_key and not self.custom_url else "custom")
            now = datetime.now(timezone.utc).timestamp()
            horizon = float(self.lookahead_hours * 3600)
            events = [e for e in events if -900.0 <= e.timestamp_utc - now <= horizon]
            return events, ("tradingeconomics" if self.api_key and not self.custom_url else "custom")
        except Exception as exc:
            return [], f"error:{type(exc).__name__}"


def compact_event_line(context: dict[str, Any]) -> str:
    events = context.get("relevant_events") or []
    if not events:
        return "CLEAR"
    e = events[0]
    delta = float(e.get("seconds_from_release") or 0.0)
    sign = "+" if delta >= 0 else ""
    return f'{e.get("currency","")} {e.get("event","")} | {sign}{delta:.0f}s | surprise={float(e.get("surprise_pct") or 0.0):.2f}%'


__all__ = [
    "NewsEvent",
    "NewsCalendarClient",
    "compact_event_line",
    "event_surprise",
    "news_context",
    "parse_calendar_payload",
]
