"""Live AI provider adapter.

No provider, model, or secret is hard-coded. Configure them with:
AI_PROVIDER=openai-compatible
AI_BASE_URL=<provider chat-completions URL>
AI_MODEL=<model>
AI_API_KEY=<secret>

The adapter is read-only: it analyzes supplied market data and never places
trades or accesses broker credentials.
"""

from __future__ import annotations

import json
import os
from typing import Any

import httpx

from ai_engine import MarketSnapshot, build_ai_request, parse_ai_decision


class AIProviderError(RuntimeError):
    pass


def _config() -> tuple[str, str, str]:
    base_url = os.getenv("AI_BASE_URL", "").strip().rstrip("/")
    model = os.getenv("AI_MODEL", "").strip()
    api_key = (
        os.getenv("AI_API_KEY", "").strip()
        or os.getenv("OPENAI_API_KEY", "").strip()
        or os.getenv("OPENROUTER_API_KEY", "").strip()
    )
    if not base_url or not model or not api_key:
        raise AIProviderError("AI environment is not configured")
    return base_url, model, api_key


def _prompt(snapshot: MarketSnapshot) -> str:
    request = build_ai_request(snapshot)
    return (
        "You are the market-analysis brain for a DEMO trading-signal system. "
        "Analyze only the supplied live snapshot. Do not invent prices, candles, "
        "indicators, news, or market conditions. Return JSON only. "
        "Return a direction only when the evidence supports UP or DOWN; otherwise "
        "return an empty direction so the caller silently skips the analysis. "
        "Do not provide financial guarantees.\n\n"
        + json.dumps(request, ensure_ascii=False, separators=(",", ":"))
    )


async def analyze(snapshot: MarketSnapshot) -> dict[str, Any] | None:
    """Ask the configured AI provider for a signal; return None when unqualified."""
    base_url, model, api_key = _config()
    url = base_url if base_url.endswith("/chat/completions") else f"{base_url}/chat/completions"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": (
                    "Return JSON with exactly: direction (UP/DOWN or empty), "
                    "confidence (0-100), reason (short string)."
                ),
            },
            {"role": "user", "content": _prompt(snapshot)},
        ],
    }

    timeout = httpx.Timeout(20.0, connect=8.0)
    async with httpx.AsyncClient(timeout=timeout) as http:
        response = await http.post(url, headers=headers, json=payload)

    if response.status_code >= 400:
        raise AIProviderError(f"AI provider HTTP {response.status_code}")

    body = response.json()
    try:
        content = body["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise AIProviderError("AI provider returned an invalid response") from exc

    decision = parse_ai_decision(content, snapshot)
    if decision is None:
        return None

    return {
        "decision": decision.decision,
        "direction": decision.direction,
        "confidence": decision.confidence,
        "reason": decision.reason,
        "display_name": decision.display_name,
        "pair": decision.pair,
        "signal_label": f"{decision.display_name} ({decision.pair})",
    }
