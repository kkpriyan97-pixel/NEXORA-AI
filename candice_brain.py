            rejection = bullish_rejection if direction == "UP" else bearish_rejection
            stoch_mean = (direction == "UP" and stoch_oversold) or (direction == "DOWN" and stoch_overbought)
            if not extreme or not location or not weak_momentum or not stoch_mean:
                return -1.0
            s = 64.0
            s += 10 if trend == "SIDEWAYS" else 0
            s += 8 if rejection else 0
            s += 8 if structure_quality[direction] >= 0.4 else 0
            s += 6 if body_ratio >= 0.25 else 0
            return min(94.0, s)

        if strategy == "PRICE_ACTION":
            directional_pattern = (
                direction == "UP" and pattern in {"BULLISH_CANDLE", "BULLISH_REJECTION"}
            ) or (
                direction == "DOWN" and pattern in {"BEARISH_CANDLE", "BEARISH_REJECTION"}
            )
            if not directional_pattern:
                return -1.0
            location = near_support if direction == "UP" else near_resistance
            s = 58.0
            s += 12 if location else 5
            s += 8 if structure_ok else 0
            s += 7 if trend_ok else 0
            s += 7 if body_ratio >= 0.60 else 2
            s += 5 if slope_ok else 0
            return min(94.0, s)

        if strategy == "MOMENTUM":
            # Momentum must represent continuation, not merely one fast candle.
            # In a SIDEWAYS regime we only allow it when a real level breakout
            # plus strong directional efficiency confirms that continuation.
            momentum_efficiency = _efficiency(v, 8)
            directional_structure = structure_quality[direction]
            sideways_confirmed = (
                trend == "SIDEWAYS"
                and (
                    (direction == "UP" and breakout_up) or
                    (direction == "DOWN" and breakout_down)
                )
                and breakout_distance_up if direction == "UP" else breakout_distance_down
            )
            if momentum_norm < 0.30 or body_ratio < 0.50:
                return -1.0
            if (direction == "UP" and momentum <= 0) or (direction == "DOWN" and momentum >= 0):
                return -1.0
            if (direction == "UP" and rr >= 73) or (direction == "DOWN" and rr <= 27):
                return -1.0
            if (direction == "UP" and not stoch_bull) or (direction == "DOWN" and not stoch_bear):
                return -1.0
            if directional_structure < 0.50 or aligned_recent(direction) < 2 or momentum_efficiency < 0.40:
                return -1.0
            if trend == "SIDEWAYS":
                side_break = (
                    (direction == "UP" and breakout_up and breakout_distance_up >= 0.15) or
                    (direction == "DOWN" and breakout_down and breakout_distance_down >= 0.15)
                )
                if not side_break or momentum_efficiency < 0.50:
                    return -1.0
            elif not trend_ok:
                return -1.0
            s = 60.0
            s += 10 if momentum_norm >= 0.50 else 5
            s += 8 if body_ratio >= 0.60 else 3
            s += 7 if aligned_recent(direction) >= 2 else 0
            s += 6 if trend_ok else 0
            s += 5 if structure_ok else 0
            s += 4 if slope_ok else 0
            s += 5 if ((direction == "UP" and stoch_bull) or (direction == "DOWN" and stoch_bear)) else 0
            s += 5 if momentum_efficiency >= 0.50 else 0
            return min(95.0, s)

        if strategy == "VOLATILITY":
            if volatility_ratio < 1.15 or momentum_norm < 0.30 or body_ratio < 0.45:
                return -1.0
            if trend in {"UP", "DOWN"} and not trend_ok:
                return -1.0
            s = 61.0
            s += 10 if volatility_ratio >= 1.30 else 4
            s += 8 if momentum_norm >= 0.50 else 3
            s += 7 if body_ratio >= 0.60 else 3
            s += 5 if trend_ok else 0
            s += 5 if structure_ok else 0
            s += 4 if dc["expansion"] else 0
            s += 4 if ((direction == "UP" and stoch_bull) or (direction == "DOWN" and stoch_bear)) else 0
            return min(94.0, s)

        if strategy == "TREND_FOLLOWING":
            if not trend_ok or not structure_ok or not slope_ok:
                return -1.0
            if momentum_norm < 0.20:
                return -1.0
            if structure_quality[direction] < 0.40:
                return -1.0
            if trend_persistence < 2:
                return -1.0
            if (direction == "UP" and near_resistance and not breakout_up) or (
                direction == "DOWN" and near_support and not breakout_down
            ):
                return -1.0
            if (direction == "UP" and rr >= 74) or (direction == "DOWN" and rr <= 26):