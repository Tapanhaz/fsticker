import fsticker
from fsticker import (  # noqa: F401, RUF100
    AccessType,
    FeedType,
    TableMode,
    TimescaleConfig,
)

# Merged/deduplicated feed along with a field carrying the originating broker name
# Fires once per new piece of information, regardless of which broker(s) reported


def on_tick(tick: dict):
    print("tick  ::", tick)


def on_candle(payload):
    print("Candle :: ", payload)


def on_candle_gap(payload):
    print("Outage started ::", payload)


def on_candle_gap_report(payload):
    print("Outage resolved, affected candles ::", payload)


# Everything below is per-broker, all of these always receives the broker's name as its first argument +>


def on_order(broker: str, msg: str):
    print(f"order :: [{broker}] {msg}")
    print("Type :: ", type(msg))


def on_error(broker: str, msg: str):
    print(f"error :: [{broker}] {msg}")


def on_open(broker: str, msg: str):
    print(f"open  :: [{broker}] connected -- {msg}")


def on_close(broker: str):
    print(f"close :: [{broker}] disconnected")


def on_stalled(broker: str, consecutive_failures: int):
    print(
        f"stall :: [{broker}] failed to reconnect {consecutive_failures} times in a row"
    )


def on_log(broker: str, level: int, msg: str):
    print(f"[{broker}][{fsticker.LogLevel(level).name}]{msg}")


# Fires after both broker has finished closing.


def on_shutdown():
    print("shutdown :: all brokers closed")


# The name should be unique in Credentials. Will get the same later via callbacks `broker` field


def main():
    brokers = [
        fsticker.Credentials(
            name="broker-1",
            ws_endpoint="websocket url",
            user_id="Your USER ID",
            token="Your access token",
            access_type=AccessType.API,
            verify_ssl=True,
            enable_ip_pinning=True,
            # min_log_level=fsticker.LogLevel.DEBUG,
        ),
        fsticker.Credentials(
            name="broker2-web",
            ws_endpoint="websocket url",
            token="Your access token",
            access_type=AccessType.WEB,
            verify_ssl=True,
            # min_log_level=fsticker.LogLevel.DEBUG,
        ),
    ]

    feed = fsticker.MergedFeed(
        brokers,
        candle_timeframes=[
            (
                60,
                False,
                True,
                4,
                True,
            ),  # 1-min, closed only, auto finalize grace period, omit partial ( mid session outage)
            (
                300,
                False,
                True,
                4,
                True,
            ),  # 5-min, closed-only, auto finalize grace period, omit partial ( mid session outage)
        ],
        timescale=TimescaleConfig(
            host="localhost",
            dbname="fsticker_db",
            user="postgres",
            password="password",
            table="ohlc",
            table_mode=TableMode.TIMEFRAME,  # In TIMEFRAME mode it will generate two tables ohlc_60 & ohlc_180
        ),
    )

    feed.start(
        on_tick=on_tick,
        on_candle=on_candle,
        on_order=on_order,
        on_error=on_error,
        on_open=on_open,
        on_close=on_close,
        on_stalled=on_stalled,
        on_shutdown=on_shutdown,
        on_candle_gap=on_candle_gap,
        on_candle_gap_report=on_candle_gap_report,
        # on_log=on_log
    )

    # Safe to call before, during, or after connecting : from any thread--> It is  queued and flushed automatically once each broker authenticates
    feed.subscribe(["NSE|26000", "NSE|26009", "BSE|1"])  # default FeedType.Touchline

    # Optional: block here until every broker has authenticated, with a timeout. Not required
    if feed.wait_connected(timeout=5):
        print("All brokers connected.")
    else:
        print("Not all brokers connected (timed out).")

    # Example unsubscribe :: also safe to run from another thread
    # feed.unsubscribe(["BSE|1"])

    # Example restricting subscription to a single broker instead of both
    # feed.subscribe(["NSE|26000"], target="shoonya-api")

    print("Running -- press Ctrl+C to stop.")
    # blocks until stop() (via Ctrl+C // feed.stop() from another thread)
    try:
        feed.run()
    except KeyboardInterrupt:
        print("\nInterrupted")


if __name__ == "__main__":
    main()
