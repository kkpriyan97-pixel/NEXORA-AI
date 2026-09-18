import asyncio
import os
import logging

from olymptrade_ws import OlympTradeClient
from olymptrade_ws.olympconfig import parameters

ASSET = os.getenv("OLYMPTRADE_ASSET", "BNBUSD_OTC")
TOKEN_ENV = "OLYMPTRADE_ACCESS_TOKEN"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("nexora_asset_probe")


async def on_tick(message: dict) -> None:
    for tick in message.get("d", []) or []:
        if isinstance(tick, dict) and tick.get("p") == ASSET:
            log.info("ASSET_TICK asset=%s price=%s ts=%s", ASSET, tick.get("q"), tick.get("t"))


async def main() -> None:
    token = os.getenv(TOKEN_ENV)
    if not token:
        raise RuntimeError(
            f"{TOKEN_ENV} is not set. Put the token in the runtime secret store; "
            "never commit it to Git."
        )

    client = OlympTradeClient(access_token=token, log_raw_messages=False)
    client.register_callback(parameters.E_TICK_UPDATE, on_tick)

    try:
        await client.start()
        log.info("OLYMP_CONNECTED read_only=true asset=%s", ASSET)

        # Read-only market access: no order/trade method is called.
        candles = await client.market.get_candles(ASSET, size=60, count=5)
        if candles:
            log.info("ASSET_HISTORY_OK asset=%s candles=%d", ASSET, len(candles))
            for candle in candles[-2:]:
                log.info("CANDLE asset=%s data=%s", ASSET, candle)
        else:
            log.warning("ASSET_HISTORY_EMPTY asset=%s", ASSET)

        await client.market.subscribe_ticks(ASSET)
        log.info("ASSET_TICK_SUBSCRIBED asset=%s", ASSET)

        await asyncio.sleep(15)
    finally:
        await client.stop()
        log.info("OLYMP_DISCONNECTED")


if __name__ == "__main__":
    asyncio.run(main())
