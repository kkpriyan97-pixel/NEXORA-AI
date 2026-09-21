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

        strategy_name=str(x.get("strategy") or "").upper()
        trend_name=str(x.get("trend_15m") or "").upper()
        evidence=x.get("evidence") or {}

        # Sideways-regime momentum is the second failure pattern found in the
        # recent consecutive losses (ETHUSD and PEPEUSD). Directional candle
        # evidence alone was enough to reach 90+, even while the 15m regime was
        # SIDEWAYS. Make this a local, zero-latency veto: momentum in a sideways
        # market must show materially stronger displacement, body, efficiency,
        # repeated alignment and a directional candle. This does not change the
        # 5-minute scheduler or spend any AI/network time.
        if (
            strategy_name=="MOMENTUM"
            and trend_name=="SIDEWAYS"
            and (
                float(x.get("momentum_norm") or 0.0) < 0.50
                or float(x.get("body_ratio") or 0.0) < 0.60
                or float(x.get("efficiency") or 0.0) < 0.45
                or int(evidence.get("recent_aligned_candles") or 0) < 2
                or str(x.get("pattern") or "").upper() not in (
                    "BULLISH_CANDLE","BEARISH_CANDLE",
                    "BULLISH_REJECTION","BEARISH_REJECTION"
                )
            )
        ):
            log.info(
                "SIDEWAYS_MOMENTUM_GATE_REJECTED pair=%s direction=%s momentum=%.2f body=%.2f efficiency=%.2f aligned=%s pattern=%s",
                x.get("pair"),x.get("direction"),float(x.get("momentum_norm") or 0.0),
                float(x.get("body_ratio") or 0.0),float(x.get("efficiency") or 0.0),
                evidence.get("recent_aligned_candles"),x.get("pattern")
            )
            return None

        # A 1-2 minute expiry is particularly sensitive to late/weak moves.
        # When the 15m regime is SIDEWAYS, do not allow a weak momentum setup
        # to survive only because its numeric score crossed 90.
        if (
            strategy_name=="MOMENTUM"
            and trend_name=="SIDEWAYS"
            and int(x.get("confidence") or 0)>=90
            and float(x.get("momentum_norm") or 0.0)<0.55
        ):
            log.info(
                "SIDEWAYS_MOMENTUM_CONFIDENCE_GUARD pair=%s confidence=%s momentum=%.2f",
                x.get("pair"),x.get("confidence"),float(x.get("momentum_norm") or 0.0)
            )
            return None

        # Final lightweight setup-strength guard. This is intentionally scoped
        # to BREAKOUT candidates only and runs before the external verifier,
        # so it cannot consume additional cycle time. It vetoes only weak
        # breakouts; other strategies and the 30-second deadline are untouched.
        if (
            log.info(
                "BREAKOUT_STRENGTH_GATE_REJECTED pair=%s direction=%s distance=%.3f body=%.2f momentum=%.2f efficiency=%.2f",
                x.get("pair"),x.get("direction"),
                float(x.get("breakout_distance_up") if str(x.get("direction") or "").upper()=="UP"
                      else x.get("breakout_distance_down") or 0.0),
                float(x.get("body_ratio") or 0.0),float(x.get("momentum_norm") or 0.0),
                float(x.get("efficiency") or 0.0)
            )
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