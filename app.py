                # deadline.  Run an overdue pass immediately, then preserve the
                # original wall-clock timestamp for each future pass.  This keeps
                # pass 5 at target-150s instead of accidentally moving it to the
                # last few seconds before Telegram delivery.
                await asyncio.sleep(max(0.0,scan_at-time.time()))

            remaining=max(0,signal_at-time.time())
            if remaining<=3.0:
                log.info(
                    "FIVE_SCAN_WINDOW_CLOSED cycle=%s scan=SCAN_%s remaining=%.2f",
                    cycle_id,pass_no,remaining
                )
                break

            scan_budget=max(1.0,remaining-4.0)
            try:
                # Pass 5 is the delivery-critical pass. Earlier passes already
                # refreshed the 60-second candle dataset, so forcing another full
                # 52-asset broker fetch here can consume the exact 30s lead window.
                # Reuse healthy recent candles on pass 5; stale/failed assets still
                # refresh through refresh_candles()'s normal due/stale logic.
                force_refresh=(pass_no!=5)
                await asyncio.wait_for(
                    refresh_candles(force=force_refresh),
                    timeout=scan_budget
                )
                log.info(
                    "ACCOUNT_FULL_SCAN cycle=%s scan=SCAN_%s pass=%s assets=%s analyzed=%s "
                    "seconds_to_signal=%.2f deep=%s catchup=%s force_refresh=%s",
                    cycle_id,pass_no,pass_no,len(STATE["assets"]),
                    len(STATE["analyses"]),max(0,signal_at-time.time()),
                    pass_no==5,catchup_mode,force_refresh
                )
            except asyncio.TimeoutError:
                log.warning(
                    "ACCOUNT_FULL_SCAN_TIMEOUT cycle=%s scan=SCAN_%s pass=%s budget=%.2f "
                    "seconds_to_signal=%.2f",
                    cycle_id,pass_no,pass_no,scan_budget,
                    max(0,signal_at-time.time())
                )
            except Exception as e:
                log.warning(
                    "ACCOUNT_FULL_SCAN_FAILED cycle=%s scan=SCAN_%s pass=%s type=%s message=%s",
                    cycle_id,pass_no,pass_no,type(e).__name__,str(e)[:160]
                )

            remaining=max(0,signal_at-time.time())
            if remaining<=3.0:
                break

            try:
                candidate=await asyncio.wait_for(
                    final_candidate(
                        # Pass 4 has enough time to perform external AI verification
                        # and warm its cache. Pass 5 is the exact delivery pass, so
                        # it must be cache-only; no provider timeout may consume the
                        # final signal window.
                        require_live_price=False,
                        deep_analysis=(pass_no in (4,5)),
                        use_cached_only=(pass_no==5),
                        # Rank several candidates in the deep passes. Pass 4 uses
                        # that ranked set to prepare authenticated tick coverage.
                        return_ranked=(pass_no in (4,5))
                    ),
                    timeout=max(1.0,remaining-0.50)
                )
                if isinstance(candidate,list):
                    selected=candidate[:20]
                    if not selected:
                        log.info(
                            "SCAN_COMPLETE cycle=%s scan=SCAN_%s candidate=none analyzed=%d",
                            cycle_id,pass_no,len(STATE["analyses"])
                        )
                    else:
                        for raw_candidate in selected:
                            if not isinstance(raw_candidate,dict) or not raw_candidate.get("pair"):
                                continue
                            item=raw_candidate.copy()
                            item["expiry_minutes"]=1
                            item["qualified_pass"]=pass_no
                            key=(
                                item.get("pair"),
                                str(item.get("entry_candle_ts")),
                                str(item.get("direction") or "").upper()
                            )
                            candidate_pool[key]=item

                        if pass_no==4:
                            # Prepare the authenticated tick slots well before the
                            # exact 30-second signal boundary. Any broker
                            # subscribe/unsubscribe latency is absorbed here,
                            # never at signal send time.
                            prep_items=sorted(
                                [x for x in candidate_pool.values()
                                 if int(x.get("qualified_pass") or 0)>=3],
                                key=lambda x:(
                                    int(x.get("confidence") or 0),
                                    float(x.get("strategy_margin") or 0),
                                    float(x.get("direction_agreement") or 0),
                                    float(x.get("market_quality") or 0)
                                ),
                                reverse=True
                            )[:ACCOUNT_TICK_PIN_SLOTS]
                            if prep_items:
                                pin_ttl=max(45.0,target-time.time()+20.0)
                                try:
                                    active_prep=await pin_account_tick_pairs(
                                        [x.get("pair") for x in prep_items],
                                        ttl=pin_ttl
                                    )
                                    log.info(
                                        "FINAL_CANDIDATE_TICKS_PREPARED cycle=%s pairs=%s pass=%s active=%s ttl=%.1f target_utc=%s",
                                        cycle_id,[x.get("pair") for x in prep_items],
                                        pass_no,active_prep,pin_ttl,
                                        datetime.fromtimestamp(
                                            target,tz=timezone.utc
                                        ).strftime("%H:%M:%S")
                                    )
                                except Exception as e:
                                    log.warning(
                                        "FINAL_CANDIDATE_TICKS_PREPARE_FAILED cycle=%s pairs=%s pass=%s message=%s",
                                        cycle_id,[x.get("pair") for x in prep_items],
                                        pass_no,type(e).__name__,str(e)[:120]
                                    )
                        if pass_no==5 and selected:
                            # Pass 5 can discover a different top pair from pass 4.
                            # Rebind the two authenticated tick slots immediately
                            # after pass-5 ranking, while there is still time before
                            # the exact signal boundary. The signal second itself
                            # remains strictly network-free.
                            live_ranked=sorted(
                                [x for x in selected if isinstance(x,dict) and x.get("pair")],
                                key=lambda x:(
                                    0 if live_price_age(x.get("pair"),time.time()) is None else 1,
                                    -min(60.0,float(live_price_age(x.get("pair"),time.time()) or 60.0)),
                                    int(x.get("confidence") or 0),
                                    float(x.get("strategy_margin") or 0),
                                    float(x.get("direction_agreement") or 0),
                                    float(x.get("market_quality") or 0)
                                ),
                                reverse=True
                            )[:ACCOUNT_TICK_PIN_SLOTS]
                            remaining_before_pin=max(0.0,signal_at-time.time())
                            # Never start broker subscription work too close to the
                            # signal boundary. The existing pass-4 preparation remains
                            # the fallback when less than 12 seconds are available.
                            if live_ranked and remaining_before_pin>=12.0:
                                pin_ttl=max(20.0,remaining_before_pin+10.0)
                                try:
                                    active_final=await pin_account_tick_pairs(
                                        [x.get("pair") for x in live_ranked],
                                        ttl=pin_ttl
                                    )
                                    log.info(
                                        "FINAL_CANDIDATE_TICKS_FINAL_PREPARED cycle=%s pairs=%s pass=%s active=%s ttl=%.1f seconds_to_signal=%.2f",
                                        cycle_id,[x.get("pair") for x in live_ranked],
                                        pass_no,active_final,pin_ttl,
                                        max(0.0,signal_at-time.time())
                                    )
                                except Exception as e:
                                    log.warning(
                                        "FINAL_CANDIDATE_TICKS_FINAL_PREPARE_FAILED cycle=%s pairs=%s pass=%s type=%s message=%s",
                                        cycle_id,[x.get("pair") for x in live_ranked],
                                        pass_no,type(e).__name__,str(e)[:120]
                                    )
                            elif live_ranked:
                                log.warning(
                                    "FINAL_CANDIDATE_TICKS_FINAL_PREPARE_SKIPPED cycle=%s pairs=%s seconds_to_signal=%.2f reason=too_close_to_boundary",
                                    cycle_id,[x.get("pair") for x in live_ranked],
                                    remaining_before_pin
                                )
                        for item in selected:
                            log.info(
                                "SCAN_CANDIDATE_SELECTED cycle=%s scan=SCAN_%s pass=%s pair=%s "
                                "confidence=%s strategy=%s expiry=%s pool=%s deep=%s",
                                cycle_id,pass_no,pass_no,item.get("pair"),
                                item.get("confidence"),item.get("strategy"),item.get("expiry_minutes"),
                                len(candidate_pool),pass_no==5
                            )
                elif candidate is None:
                    log.info(
                        "SCAN_COMPLETE cycle=%s scan=SCAN_%s candidate=none analyzed=%d",
                        cycle_id,pass_no,len(STATE["analyses"])
                    )
                else:
                    candidate=candidate.copy()
                    candidate["expiry_minutes"]=1
                    candidate["qualified_pass"]=pass_no
                    key=(
                        candidate.get("pair"),
                        str(candidate.get("entry_candle_ts")),
                        str(candidate.get("direction") or "").upper()
                    )
                    candidate_pool[key]=candidate
                    log.info(
                        "SCAN_CANDIDATE_SELECTED cycle=%s scan=SCAN_%s pass=%s pair=%s "
                        "confidence=%s strategy=%s expiry=%s pool=%s deep=%s",
                        cycle_id,pass_no,pass_no,candidate.get("pair"),