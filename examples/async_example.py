import asyncio
import time

from fsticker import AccessType, AsyncMergedFeed, Credentials


# Merged/deduplicated callback. Fires once per new piece of information, regardless of which broker(s) reported
async def on_tick(tick: dict):
    print(f"tick  :: {tick}")


# Everything below is per-broker, all of these always receives the broker's name as its first argument +>


async def on_order(broker: str, msg: str):
    print(f"order :: [{broker}] {msg}")


def on_candle(payload):
    print(f"Candle :: {payload}")


def on_error(broker: str, msg: str):  # sync callbacks works too
    print(f"error :: [{broker}] {msg}")


def on_open(broker: str, msg: str):
    print(f"open  :: [{broker}] connected -- {time.asctime()} -- {msg}")


def on_close(broker: str):
    print(f"close :: [{broker}] disconnected -- {time.asctime()}")


async def on_stalled(broker: str, consecutive_failures: int):
    print(
        f"stall :: [{broker}] failed to reconnect {consecutive_failures} times in a row"
    )


async def main():
    brokers = [
        Credentials(
            name="broker-1",
            ws_endpoint="websocket url",
            user_id="Your USER ID",
            token="Your access token",
            access_type=AccessType.API,
            verify_ssl=True,
            enable_ip_pinning=True,
        ),
        Credentials(
            name="broker2-web",
            ws_endpoint="websocket url",
            token="Your access token",
            access_type=AccessType.WEB,
            verify_ssl=True,
            enable_ip_pinning=True,
        ),
    ]

    tokens_list = ["BSE|1", "BSE|12", "NSE|26000", "NSE|26009", "NSE|2885"]

    feed = AsyncMergedFeed(
        brokers,
        candle_timeframes=[
            (
                60,
                True,
                True,
                3,
            ),  # 1-min, live: partials + completed, auto finalizing after 3 sec of timeframe completion ( default 4)
            (180, False, True),  # 5-min, closed-only, auto finalizing
        ],
    )

    await feed.start(
        on_tick=on_tick,
        on_candle=on_candle,
        on_order=on_order,
        on_error=on_error,
        on_open=on_open,
        on_close=on_close,
        on_stalled=on_stalled,
    )

    # Safe to call before, during, or after connecting.
    await feed.subscribe(tokens_list)

    if await feed.wait_connected(timeout=10):
        print("All brokers connected.")
    else:
        print("Timed out waiting for one or more brokers.")

    # Example of unsubscribing::
    # await feed.unsubscribe(["BSE|1"])

    # Example of restricting subscription to a single broker:
    # await feed.subscribe(["NSE|26000"], target="shoonya")

    print("Running -- press Ctrl+C to stop.")
    try:
        # Blocks until Ctrl+C / SIGTERM, feed.stop(), or every broker has closed.
        await feed.wait_closed()
    finally:
        # Always runs, including on platforms without loop signal handlers (Windows),
        # where Ctrl+C reaches us as KeyboardInterrupt / task cancellation instead.
        await feed.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted.")
