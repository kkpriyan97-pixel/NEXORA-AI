from __future__ import annotations

from brain_rules import BrainState, rank_signal_candidates
from candice_brain import analyze_asset


def _up_candles(n=120, start=1_000_000):
    out=[]
    for i in range(n):
        t=start+i*60
        base=100.0 + max(0,i-89)*0.35
        o=base
        c=o+0.08
        out.append({"time":t,"open":o,"high":c+0.03,"low":o-0.03,"close":c,"volume":100})
    return out


def main():
    now=1_000_000+121*60
    candles=_up_candles(start=1_000_000)
    candidate=analyze_asset(
        {"pair":"TEST_META","display_name":"TEST_META"},
        candles,
        price=candles[-1]["close"],
    )
    assert candidate is not None
    ind=candidate["indicators"]
    assert ind["m1_sequence_signature"]
    assert ind["market_regime"]
    assert ind["decision_time_bucket"]

    brain=BrainState()
    brain.learning_account_id=1
    for i in range(12):
        brain.learn({
            "account_id":1,
            "pair":"TEST_META",
            "strategy":"AVWAP_VOLUME_PROFILE",
            "direction":"UP",
            "expiry_minutes":1,
            "pattern":"AVWAP_VOLUME_PROFILE_ALIGNMENT",
            "trend_15m":"AVWAP_BULLISH",
            "structure_1m":"ABOVE_AVWAP_POC",
            "indicator_context":ind,
            "result":"WIN" if i != 3 else "LOSS",
        })

    adapted=brain.adaptive_candidate(candidate)
    assert 0.35 <= adapted["meta_calibrated_probability"] <= 0.70
    assert "meta_rank_bonus" in adapted
    assert "meta_rank_score" in adapted
    assert adapted["confidence"] >= 0
    assert adapted["expiry_minutes"] == 1

    # Ranking uses the meta score but still retains technical confidence as a tie-breaker.
    a={"pair":"A","strategy":"AVWAP_VOLUME_PROFILE","direction":"UP",
       "confidence":95,"market_quality":95,"meta_rank_score":97}
    b={"pair":"B","strategy":"AVWAP_VOLUME_PROFILE","direction":"UP",
       "confidence":99,"market_quality":99,"meta_rank_score":96}
    ranked=rank_signal_candidates([a,b])
    assert ranked[0]["pair"]=="A"

    print("NEXORA_META_PREDICTION_SMOKE_OK")


if __name__=="__main__":
    main()
