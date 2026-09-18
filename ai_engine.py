"""Read-only AI decision layer foundation for the NEXORA/Candice market brain.

The model is asked only for qualified signal data. Non-qualified analyses are
silently skipped by the integration layer; NO_SIGNAL is not a user-facing
Telegram state.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class MarketSnapshot:
    display_name: str
    pair: str
    mode: str
    profitability: int
    price: float | None
    timestamp: Any
    candles: list[dict[str, Any]]


@dataclass(frozen=True)
class AIDecision:
    decision: str
    direction: str | None
    confidence: int
    reason: str
    display_name: str
    pair: str


def snapshot_from_asset(
    asset: dict[str, Any],
    candles: list[dict[str, Any]],
    price: float | None,
    timestamp: Any,
) -> MarketSnapshot:
    return MarketSnapshot(
        display_name=str(asset.get("display_name") or asset.get("title") or "").strip(),
        pair=str(asset.get("pair") or "").strip(),
        mode=str(asset.get("mode") or ""),
        profitability=int(asset.get("profitability") or 0),
        price=price,
        timestamp=timestamp,
        candles=candles or [],
    )


def signal_label(snapshot: MarketSnapshot) -> str:
    """Account name first; API pair is shown only in parentheses."""
    if not snapshot.display_name or not snapshot.pair:
        raise ValueError("Missing live account display name or API pair")
    return f"{snapshot.display_name} ({snapshot.pair})"


def build_ai_request(snapshot: MarketSnapshot) -> dict[str, Any]:
    """Build provider-neutral input for the AI model."""
    return {
        "task": "Analyze this live market snapshot for a DEMO trading signal.",
        "constraints": {
            "read_only": True,
            "no_auto_trade": True,
            "no_login": True,
            "use_live_snapshot_only": True,
            "emit_signal_only_when_direction_is_supported": True,
        },
        "asset": {
            "name": snapshot.display_name,
            "pair": snapshot.pair,
            "mode": snapshot.mode,
            "profitability": snapshot.profitability,
        },
        "market": {
            "price": snapshot.price,
            "timestamp": snapshot.timestamp,
            "candles": snapshot.candles,
        },
        "required_output": {
            "direction": "UP or DOWN",
            "confidence": "integer 0-100",
            "reason": "short factual explanation",
        },
    }


def ai_environment_status() -> dict[str, Any]:
    """Report configuration presence without exposing secret values."""
    provider = os.getenv("AI_PROVIDER", "").strip()
    model = os.getenv("AI_MODEL", "").strip()
    api_key_present = bool(
        os.getenv("AI_API_KEY", "").strip()
        or os.getenv("OPENAI_API_KEY", "").strip()
        or os.getenv("OPENROUTER_API_KEY", "").strip()
    )
    return {
        "configured": bool(provider and model and api_key_present),
        "provider": provider or None,
        "model": model or None,
        "api_key_present": api_key_present,
    }


def parse_ai_decision(
    payload: str | dict[str, Any],
    snapshot: MarketSnapshot,
) -> AIDecision | None:
    """Validate a model response; return None when it is not a signal."""
    data = json.loads(payload) if isinstance(payload, str) else payload
    direction = str(data.get("direction") or "").upper()
    if direction not in {"UP", "DOWN"}:
        return None

    confidence = max(0, min(100, int(data.get("confidence") or 0)))
    reason = str(data.get("reason") or "").strip()

    return AIDecision(
        decision="SIGNAL",
        direction=direction,
        confidence=confidence,
        reason=reason,
        display_name=snapshot.display_name,
        pair=snapshot.pair,
    )


def decision_dict(decision: AIDecision) -> dict[str, Any]:
    return asdict(decision)
