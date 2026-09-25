from __future__ import annotations

import time
import candice_brain as cb
from candice_brain import ALLOWED_STRATEGY, OTC_STRATEGY, analyze_asset


def up_candles(n=120, start=1_000_000):
    out=[]
    for i in range(n):
        t=start+i*60
        base=100+(i-99)*0.45 if i>=100 else 100+(i%5)*0.01
        o=base
        c=o+0.08
        out.append({"time":t,"open":o,"high":c+0.03,"low":o-0.03,"close":c,"volume":100})
    return out


def down_candles(n=120, start=1_000_000):
    out=[]
    for i in range(n):
        t=start+i*60
        base=110-(i-99)*0.45 if i>=100 else 110+(i%5)*0.01
        o=base
        c=o-0.08
        out.append({"time":t,"open":o,"high":o+0.03,"low":c-0.03,"close":c,"volume":100})
    return out




def otc_structure_candles(n=120,start=1_000_000):
    out=[]
    for i in range(n):
        t=start+i*60
        base=100.0+(i*0.025)
        o=base
        c=o+0.04
        out.append({
            "time":t,"open":o,"high":c+0.02,"low":o-0.02,
            "close":c,"volume":0
        })

    # Final closed-M1 sequence: BOS -> displacement -> retest/hold -> confirmation.
    for i in range(112,117):
        t=start+i*60
        o=103.85+(i-112)*0.03
        c=o+0.02
        out[i]={"time":t,"open":o,"high":104.50,"low":o-0.02,"close":c,"volume":0}

    t=start+117*60
    out[117]={
        "time":t,"open":104.00,"high":106.20,"low":103.80,
        "close":105.70,"volume":0
    }
    t=start+118*60
    out[118]={
        "time":t,"open":104.90,"high":105.90,"low":104.30,
        "close":105.30,"volume":0
    }
    t=start+119*60
    out[119]={
        "time":t,"open":105.30,"high":106.00,"low":105.20,
        "close":105.85,"volume":0
    }
    return out


def main():
    start=int(time.time()//60)*60-121*60
    now=time.time()

    up=analyze_asset({"pair":"TEST_UP","display_name":"TEST_UP"},up_candles(start=start))
    assert up and up["direction"]=="UP"
    assert up["strategy"]==ALLOWED_STRATEGY
    assert up["expiry_minutes"]==1
    assert up["decision_candle_closed"] is True
    assert set(("anchored_vwap","volume_profile_poc","volume_profile_vah","volume_profile_val")) <= set(up["indicators"])
    assert up["indicators"]["alligator_confirmed"] is True
    assert up["indicators"]["alligator_periods"] == "13/8,8/5,5/3"
    assert up["indicators"]["alligator_confirmation"] == "CONFIRMED"

    down=analyze_asset({"pair":"TEST_DOWN","display_name":"TEST_DOWN"},down_candles(start=start))
    assert down and down["direction"]=="DOWN"
    assert down["strategy"]==ALLOWED_STRATEGY
    assert down["expiry_minutes"]==1
    assert down["indicators"]["alligator_confirmed"] is True
    assert down["indicators"]["alligator_periods"] == "13/8,8/5,5/3"
    assert down["indicators"]["alligator_confirmation"] == "CONFIRMED"

    flat=[{"time":start+i*60,"open":100,"high":100.01,"low":99.99,"close":100,"volume":100} for i in range(120)]
    assert analyze_asset({"pair":"TEST_FLAT","display_name":"TEST_FLAT"},flat) is None

    otc_start=(int(time.time())//900)*900-8*900
    otc=analyze_asset(
        {"pair":"EURUSD_OTC","display_name":"EURUSD OTC","mode":"OTC"},
        otc_structure_candles(start=otc_start),
    )
    if not otc:
        _closed=cb._closed_1m(otc_structure_candles(start=otc_start),time.time())
        _blocks=cb._complete_15m_blocks(_closed,time.time())
        _bias=cb._otc_15m_bias(_blocks)
        _setup=cb._otc_structure_setup(_closed,"UP")
        raise AssertionError(
            f"OTC debug closed={len(_closed)} blocks={len(_blocks)} bias={_bias} "
            f"setup={_setup} last={_closed[-3:] if _closed else []}"
        )
    assert otc["direction"]=="UP"
    assert otc["strategy"]==OTC_STRATEGY
    assert otc["expiry_minutes"]==1
    assert otc["self_strategy_version"]=="OTC_STRUCTURE_V1"
    assert otc["indicators"]["otc_strategy"] is True
    assert otc["indicators"]["otc_market_bias"]=="BULLISH"
    assert otc["indicators"]["otc_bos_confirmed"] is True
    assert otc["indicators"]["otc_displacement_confirmed"] is True
    assert otc["indicators"]["otc_retest_confirmed"] is True
    assert otc["indicators"]["otc_hold_confirmed"] is True
    assert otc["indicators"]["otc_confirmation_candle_confirmed"] is True
    assert otc["indicators"]["exact_live_setup"] is True

    # A structurally valid setup with no broker-reported volume must be rejected
    # by the strict HIGH-volume live gate rather than being labelled HIGH.
    no_volume=[]
    for i in range(120):
        t=start+i*60
        base=100+(i-99)*0.45 if i>=100 else 100+(i%5)*0.01
        o=base
        close=o+0.08
        no_volume.append({"time":t,"open":o,"high":close+0.03,"low":o-0.03,"close":close})
    assert analyze_asset({"pair":"TEST_NO_VOLUME","display_name":"TEST_NO_VOLUME"},no_volume) is None
    discovery=analyze_asset(
        {"pair":"TEST_NO_VOLUME_DISCOVERY","display_name":"TEST_NO_VOLUME_DISCOVERY"},
        no_volume,
        require_high_volume=False,
    )
    assert discovery and discovery["indicators"]["high_volume_confirmed"] is False
    assert discovery["indicators"]["high_volume_gate_pending"] is True

    print("NEXORA_AVWAP_VP_SMOKE_OK")


if __name__=="__main__":
    main()
