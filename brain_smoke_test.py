from __future__ import annotations

from candice_brain import ALLOWED_STRATEGY, analyze_asset


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


def main():
    start=1_000_000
    now=start+121*60

    up=analyze_asset({"pair":"TEST_UP","display_name":"TEST_UP"},up_candles(start=start),now=now)
    assert up and up["direction"]=="UP"
    assert up["strategy"]==ALLOWED_STRATEGY
    assert up["expiry_minutes"]==1
    assert up["decision_candle_closed"] is True
    assert set(("anchored_vwap","volume_profile_poc","volume_profile_vah","volume_profile_val")) <= set(up["indicators"])

    down=analyze_asset({"pair":"TEST_DOWN","display_name":"TEST_DOWN"},down_candles(start=start),now=now)
    assert down and down["direction"]=="DOWN"
    assert down["strategy"]==ALLOWED_STRATEGY
    assert down["expiry_minutes"]==1

    flat=[{"time":start+i*60,"open":100,"high":100.01,"low":99.99,"close":100,"volume":100} for i in range(120)]
    assert analyze_asset({"pair":"TEST_FLAT","display_name":"TEST_FLAT"},flat,now=now) is None
    print("NEXORA_AVWAP_VP_SMOKE_OK")


if __name__=="__main__":
    main()
