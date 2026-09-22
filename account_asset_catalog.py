# Canonical fixed asset universe for NEXORA-AI.
# Source: user-provided "available_assets_list(1).pdf" on 2026-09-22.
# Runtime MUST NOT call any broker asset/profitability-list endpoint to build
# this universe. Live market data may still be requested for these exact pairs.

def _a(display_name, pair, market_group, profitability):
    return {
        "display_name": display_name,
        "pair": pair,
        "market_group": market_group,
        "profitability": int(profitability),
    }

ACCOUNT_ASSET_CATALOG = [
    # Flex / Commodity
    _a("WTI Crude Oil", "WTI", "Flex - Commodity", 10),
    _a("BRENT", "_BRN", "Flex - Commodity", 10),
    _a("Natural Gas", "NG", "Flex - Commodity", 10),

    # Flex / Index
    _a("Asia Composite Index", "ASIA_X", "Flex - Index", 85),
    _a("Europe Composite Index", "EUROPE_X", "Flex - Index", 85),
    _a("Crypto Composite Index", "CRYPTO_X", "Flex - Index", 85),
    _a("Compound Index", "MCI_X", "Flex - Index", 85),
    _a("Football Champions 2026 Index", "GOAL_X", "Flex - Index", 90),
    _a("Halal Market Axis", "HMA_X", "Flex - Index", 85),
    _a("Quickler", "ULTRA_X", "Flex - Index", 85),
    _a("Stable Tick Index", "STABLE_X", "Flex - Index", 85),
    _a("Arabian General Index", "ARAB_X", "Flex - Index", 80),
    _a("Oasis Index", "OASIS_X", "Flex - Index", 80),
    _a("Qahwa Index", "QAHWA_X", "Flex - Index", 80),
    _a("Basic Altcoin Index", "ALTCOIN", "Flex - Index", 80),
    _a("Basic Dollar Index", "BDIX", "Flex - Index", 10),
    _a("CAC 40", "FCE", "Flex - Index", 10),
    _a("NASDAQ", "NQ", "Flex - Index", 10),
    _a("Dow Jones", "YM", "Flex - Index", 10),
    _a("Nikkei 225", "NKD", "Flex - Index", 10),
    _a("RUSSELL 2000", "TF", "Flex - Index", 10),
    _a("EURO STOXX 50", "FESX", "Flex - Index", 10),
    _a("FTSE 100", "Z", "Flex - Index", 10),
    _a("Hang Seng Index", "HSI", "Flex - Index", 10),
    _a("DAX", "FDAX", "Flex - Index", 10),
    _a("S&P 500", "ES", "Flex - Index", 10),

    # Flex / Crypto
    _a("Bitcoin", "Bitcoin", "Flex - Crypto", 80),
    _a("Ethereum", "ETHUSD", "Flex - Crypto", 80),
    _a("Litecoin", "LTCUSD", "Flex - Crypto", 10),

    # Flex / Forex
    _a("EUR/USD", "EURUSD", "Flex - Forex", 10),
    _a("USD/JPY", "USDJPY", "Flex - Forex", 10),
    _a("AUD/CAD", "AUDCAD", "Flex - Forex", 80),
    _a("GBP/USD", "GBPUSD", "Flex - Forex", 80),
    _a("AUD/USD", "AUDUSD", "Flex - Forex", 80),
    _a("CHF/JPY", "CHFJPY", "Flex - Forex", 10),
    _a("AUD/CHF", "AUDCHF", "Flex - Forex", 30),
    _a("EUR/AUD", "EURAUD", "Flex - Forex", 10),
    _a("EUR/CAD", "EURCAD", "Flex - Forex", 80),
    _a("AUD/NZD", "AUDNZD", "Flex - Forex", 80),
    _a("USD/CAD", "USDCAD", "Flex - Forex", 80),
    _a("GBP/CAD", "GBPCAD", "Flex - Forex", 10),
    _a("CAD/JPY", "CADJPY", "Flex - Forex", 10),
    _a("EUR/JPY", "EURJPY", "Flex - Forex", 10),
    _a("EUR/NZD", "EURNZD", "Flex - Forex", 30),
    _a("EUR/GBP", "EURGBP", "Flex - Forex", 30),
    _a("USD/CHF", "USDCHF", "Flex - Forex", 10),
    _a("GBP/NZD", "GBPNZD", "Flex - Forex", 10),
    _a("GBP/AUD", "GBPAUD", "Flex - Forex", 80),
    _a("AUD/JPY", "AUDJPY", "Flex - Forex", 10),
    _a("EUR/CHF", "EURCHF", "Flex - Forex", 30),
    _a("NZD/USD", "NZDUSD", "Flex - Forex", 30),
    _a("GBP/CHF", "GBPCHF", "Flex - Forex", 30),
    _a("NZD/JPY", "NZDJPY", "Flex - Forex", 80),
    _a("NZD/CAD", "NZDCAD", "Flex - Forex", 10),
    _a("CAD/CHF", "CADCHF", "Flex - Forex", 80),
    _a("GBP/JPY", "GBPJPY", "Flex - Forex", 10),
    _a("USD/NOK", "USDNOK", "Flex - Forex", 10),
    _a("USD/MXN", "USDMXN", "Flex - Forex", 10),
    _a("USD/SGD", "USDSGD", "Flex - Forex", 10),
    _a("NZD/CHF", "NZDCHF", "Flex - Forex", 10),

    # Flex / Metal
    _a("Gold", "XAUUSD", "Flex - Metal", 13),
    _a("Silver", "XAGUSD", "Flex - Metal", 10),
    _a("Copper", "HG", "Flex - Metal", 10),
    _a("Platinum", "PL", "Flex - Metal", 10),

    # OTC / Crypto
    _a("BNB OTC", "BNBUSD_OTC", "OTC - Crypto", 90),
    _a("Bitcoin OTC", "BTCUSD_OTC", "OTC - Crypto", 90),
    _a("PEPE OTC", "PEPEUSD_OTC", "OTC - Crypto", 90),
    _a("Ethereum OTC", "ETHUSD_OTC", "OTC - Crypto", 90),
    _a("SHIB OTC", "SHIBUSD_OTC", "OTC - Crypto", 10),
    _a("Dogecoin OTC", "DOGUSD_OTC", "OTC - Crypto", 90),
    _a("Litecoin OTC", "LTCUSD_OTC", "OTC - Crypto", 90),
    _a("Solana OTC", "SOLUSD_OTC", "OTC - Crypto", 90),
    _a("Ripple OTC", "XRPUSD_OTC", "OTC - Crypto", 90),

    # OTC / Forex
    _a("EUR/USD OTC", "EURUSD_OTC", "OTC - Forex", 85),
    _a("AUD/USD OTC", "AUDUSD_OTC", "OTC - Forex", 80),
    _a("USD/CHF OTC", "USDCHF_OTC", "OTC - Forex", 85),
    _a("GBP/USD OTC", "GBPUSD_OTC", "OTC - Forex", 85),
    _a("USD/JPY OTC", "USDJPY_OTC", "OTC - Forex", 80),
    _a("AUD/CAD OTC", "AUDCAD_OTC", "OTC - Forex", 85),
    _a("NZD/USD OTC", "NZDUSD_OTC", "OTC - Forex", 85),
    _a("USD/CAD OTC", "USDCAD_OTC", "OTC - Forex", 85),
    _a("GBP/CAD OTC", "GBPCAD_OTC", "OTC - Forex", 85),
    _a("EUR/GBP OTC", "EURGBP_OTC", "OTC - Forex", 85),
    _a("GBP/JPY OTC", "GBPJPY_OTC", "OTC - Forex", 85),
    _a("EUR/CAD OTC", "EURCAD_OTC", "OTC - Forex", 85),
    _a("AUD/CHF OTC", "AUDCHF_OTC", "OTC - Forex", 85),
    _a("CAD/JPY OTC", "CADJPY_OTC", "OTC - Forex", 85),
    _a("AUD/NZD OTC", "AUDNZD_OTC", "OTC - Forex", 85),
    _a("AUD/JPY OTC", "AUDJPY_OTC", "OTC - Forex", 85),
    _a("GBP/AUD OTC", "GBPAUD_OTC", "OTC - Forex", 85),
    _a("CAD/CHF OTC", "CADCHF_OTC", "OTC - Forex", 85),
    _a("CHF/JPY OTC", "CHFJPY_OTC", "OTC - Forex", 85),
    _a("EUR/AUD OTC", "EURAUD_OTC", "OTC - Forex", 85),
    _a("EUR/JPY OTC", "EURJPY_OTC", "OTC - Forex", 85),
    _a("EUR/NZD OTC", "EURNZD_OTC", "OTC - Forex", 85),
    _a("EUR/CHF OTC", "EURCHF_OTC", "OTC - Forex", 85),
    _a("GBP/NZD OTC", "GBPNZD_OTC", "OTC - Forex", 85),
    _a("NZD/CAD OTC", "NZDCAD_OTC", "OTC - Forex", 85),
    _a("NZD/CHF OTC", "NZDCHF_OTC", "OTC - Forex", 85),
    _a("NZD/JPY OTC", "NZDJPY_OTC", "OTC - Forex", 85),
    _a("GBP/CHF OTC", "GBPCHF_OTC", "OTC - Forex", 85),

    # OTC / Metal
    _a("Gold OTC", "XAUUSD_OTC", "OTC - Metal", 85),
    _a("Silver OTC", "XAGUSD_OTC", "OTC - Metal", 85),

    # Stocks (the supplied screenshot showed BMW without a closed notice)
    _a("BMW", "BMW", "Stocks", 10),
]

if len(ACCOUNT_ASSET_CATALOG) != 104:
    raise RuntimeError(
        f"Canonical account asset catalog must contain exactly 104 assets; "
        f"found {len(ACCOUNT_ASSET_CATALOG)}"
    )

if len({x["display_name"] for x in ACCOUNT_ASSET_CATALOG}) != 104:
    raise RuntimeError("Canonical account asset catalog contains duplicate display names")

if len({x["pair"] for x in ACCOUNT_ASSET_CATALOG}) != 104:
    raise RuntimeError("Canonical account asset catalog contains duplicate internal pairs")


def build_account_asset_snapshot():
    """Build runtime assets from the fixed user-supplied catalog only."""
    out = []
    for item in ACCOUNT_ASSET_CATALOG:
        pair = item["pair"]
        out.append({
            "pair": pair,
            "display_name": item["display_name"],
            "title": item["display_name"],
            "signal_asset_label": item["display_name"],
            "profitability": item["profitability"],
            "locked": False,
            "locked_trading": False,
            "disabled": False,
            "api_blocked": False,
            "mode": "OTC" if "_OTC" in pair.upper() else "REAL",
            "trading_mode": "FLEX_TIME",
            "market_group": item["market_group"],
            # Quickler remains visible in the scan universe but is not sent
            # through the normal signal gate used by the existing Brain.
            "signal_eligible": pair.upper() != "ULTRA_X",
        })
    return out
