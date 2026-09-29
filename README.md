# fsticker

[![PyPI](https://img.shields.io/pypi/v/fsticker.svg?color=blue)](https://pypi.org/project/fsticker/)
[![Downloads](https://static.pepy.tech/badge/fsticker)](https://pepy.tech/project/fsticker)
[![Python versions](https://img.shields.io/pypi/pyversions/fsticker.svg)](https://pypi.org/project/fsticker/)
[![Build](https://github.com/Tapanhaz/fsticker/actions/workflows/build.yml/badge.svg)](https://github.com/Tapanhaz/fsticker/actions/workflows/build.yml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Multi-broker, deduplicated market-data feed for Shoonya/NorenWS-style WebSocket protocol 
(Shoonya, Flattrade, and compatible brokers). Connects to several broker accounts at once, merges their ticks into
a single deduplicated stream, and optionally builds OHLC candles from that
stream.

> **Disclaimer:** `fsticker` is an independent, unofficial project built
> for personal use. It is not affiliated with, endorsed by, or connected
> to Shoonya, Flattrade, NorenWS/Noren, Kambala, or any broker referenced
> in this repo. All trademarks belong to their respective owners.

## Features

- **Multi-broker merge** — subscribe the same instruments across several
  broker sessions; receive one deduplicated tick stream.
- **Auto-reconnect & auto-resubscribe** — exponential backoff with jitter,
  per broker, independent of the others. Once a broker reconnects, every
  instrument you'd previously subscribed on it is automatically
  re-subscribed — no manual resubscribe logic needed on your end.
- **Optional IP pinning** — resolves a broker's hostname to its candidate IPs
  and fails over between them.
- **Optional candle engine** — turn the merged tick stream into OHLC candles
  at any number of timeframes, live (partial + complete) or closed-only,
  with optional anchored to each exchange's real session open.
- **Optional TimescaleDB sink** — persist completed candles straight into a
  Postgres/TimescaleDB hypertable, with zero effect on the rest of the feed
  if it's not configured or the database isn't reachable.
- **Sync and async APIs** — `fsticker.MergedFeed` (blocking `run()`) and
  `fsticker.AsyncMergedFeed` (`asyncio`-native).

## Install

`fsticker` is available on pypi. For Installation, run : 

```bash
pip install fsticker
```

Prebuilt self-contained wheels are provided for Linux, macOS and Windows.

### Platform support

| OS      | Minimum version | Notes                                              |
|---------|------------------|-----------------------------------------------------|
| Linux   | glibc-based, manylinux_2_28+ (x86_64, aarch64) | musl (Alpine) not supported |
| Windows | Windows 10+ (amd64, arm64) | |
| macOS   | **15.4 (Sequoia) or newer**, both Intel and Apple Silicon | see below |

**Why macOS needs 15.4+:** the optional TimescaleDB candle sink statically
links PostgreSQL's `libpq`, which — with the toolchain used to build it —
cannot be built targeting macOS versions older than 15.4. Since this
feature is on by default, the published macOS wheels target 15.4+
across the board.

If you're on an older macOS and don't need TimescaleDB persistence, build
from source with the sink disabled:

```bash
pip install fsticker --no-binary fsticker \
    --config-settings=cmake.define.FSTICKER_ENABLE_TIMESCALE=OFF
```

### Requirements

- **Python 3.11 – 3.14** (CPython only)


## Quickstart (sync)

```python
import fsticker

brokers = [
    fsticker.Credentials(
        name="shoonya",
        ws_endpoint="wss://api.shoonya.com/NorenWSAPI/",
        user_id="YOUR_USER_ID",
        token="YOUR_SESSION_TOKEN",
        access_type=fsticker.AccessType.API,
    ),
]

feed = fsticker.MergedFeed(brokers)
feed.start(on_tick=lambda tick: print(tick))
feed.subscribe(["NSE|26000", "NSE|26009"])
feed.run()  # blocks until Ctrl+C
```

## Quickstart (async)

```python
import asyncio
from fsticker import AsyncMergedFeed, Credentials

async def main():
    feed = AsyncMergedFeed([
        Credentials(
            name="shoonya",
            ws_endpoint="wss://api.shoonya.com/NorenWSAPI/",
            user_id="YOUR_USER_ID",
            token="YOUR_SESSION_TOKEN",
        ),
    ])

    await feed.start(on_tick=lambda tick: print(tick))
    await feed.subscribe(["NSE|26000", "NSE|26009"])

    try:
        await feed.wait_closed()  # blocks until stop()/Ctrl+C/SIGTERM, or every broker closes
    finally:
        await feed.aclose()

asyncio.run(main())
```

For fuller, runnable examples — including order/error/stall callbacks,
multi-broker setup, and the candle engine — see
[`examples/sync_example.py`](https://github.com/Tapanhaz/fsticker/blob/main/examples/sync_example.py)
and
[`examples/async_example.py`](https://github.com/Tapanhaz/fsticker/blob/main/examples/async_example.py).

## Configuration

`MergedFeed` and `AsyncMergedFeed` take the same constructor parameters:

```python
Feed(
    brokers: list[Credentials],
    candle_timeframes: list[tuple] | None = None,
    exchange_anchors: dict[str, int] = {  # see defaults below
        "NSE": 13500, "BSE": 13500, "NFO": 13500, "BFO": 13500,
        "MCX": 12600, "CDS": 12600, "BCD": 12600,
    },
    timescale: TimescaleConfig | None = None,
    tick_queue_capacity: int = 100_000,
    tick_queue_overwrite: bool = True,
    candle_queue_capacity: int = 100_000,
    candle_queue_overwrite: bool = True,
)
```

| Param                    | Type                       | Default      | Meaning |
|----------------------------|------------------------------|---------------|---------|
| `brokers`                  | `list[Credentials]`          | required       | See below. |
| `candle_timeframes`         | `list[tuple] \| None`        | `None`         | See below. |
| `exchange_anchors`          | `dict[str, int]`             | shown above    | See below. |
| `timescale`                 | `TimescaleConfig \| None`    | `None`         | See below. |
| `tick_queue_capacity`       | `int`                        | `100_000`      | Max buffered raw ticks before the overwrite policy below kicks in. |
| `tick_queue_overwrite`      | `bool`                       | `True`         | When the tick queue is full: drop the oldest queued tick to make room for the new one, instead of blocking/erroring. |
| `candle_queue_capacity`     | `int`                        | `100_000`      | Same as `tick_queue_capacity`, for the candle queue. |
| `candle_queue_overwrite`    | `bool`                       | `True`         | Same drop-oldest behavior, for candles. |



### `brokers` — `list[Credentials]` (required)

One `Credentials` entry per broker session you want merged into the feed.
You can pass a single broker, or several — including two sessions on the
*same* broker (e.g. one `AccessType.API` and one `AccessType.WEB`), which
is exactly the "if one disconnects the other keeps feeding" merged-feed
use case.

| Field                | Type   | Default            | Meaning |
|-----------------------|--------|---------------------|---------|
| `name`                | `str`  | required             | Unique per-broker label. Used in every per-broker callback (`on_order`, `on_error`, `on_open`, `on_close`, `on_stalled`) and as the `target=` argument to `subscribe()`/`unsubscribe()` when you want to address one broker instead of all of them. |
| `ws_endpoint`         | `str`  | required             | The NorenWS-compatible websocket URL for *this specific session*. A broker's plain API endpoint and its "Web" endpoint are different URLs with different `access_type`s — see the two Shoonya endpoints in the examples below. |
| `user_id`             | `str`  | required             | The broker account's client/user ID used to authenticate this session. |
| `token`               | `str`  | required             | The session/auth token from that broker's login flow — **not** your account password, and a live secret (see the warning below if you're publishing example code). |
| `access_type`         | `AccessType` | `AccessType.API` | `AccessType.API`, `AccessType.WEB`, or `AccessType.MOB`. Must match whichever flavor of `ws_endpoint`/`token` you're using — mixing them (e.g. a Web token against an API endpoint) will fail to authenticate. |
| `verify_ssl`          | `bool` | `False`              | Verify the websocket's TLS certificate. |
| `enable_ip_pinning`   | `bool` | `False`              | Resolve `ws_endpoint`'s hostname to its candidate IPs up front and fail over between them on reconnect, instead of re-resolving DNS each time (see "Optional IP pinning" above). |



### `candle_timeframes` — `list[tuple] | None`

Each tuple is `(period: int, live: bool, auto_finalize: bool)`, or with
an optional 4th element, `(period, live, auto_finalize, auto_finalize_grace_seconds: float)`.

- `period` — the candle's timeframe in seconds; any positive integer.
- `live` — `True` emits both a partial (`current`) and completed
  (`previous`) candle per tick, in the `dict[dict]` shape described
  below. `False` emits once, only on candle completion, as a plain dict.
- `auto_finalize` — normally a candle is only marked complete once the
  *next* timeframe's first tick arrives for that token. Set `True` to
  instead force-finalize it a fixed grace period after the timeframe
  boundary — useful for illiquid instruments that might not tick again
  for a while.
- `auto_finalize_grace_seconds` (optional, default `4.0`) — how long
  after the timeframe boundary `auto_finalize=True` waits before
  force-emitting the candle. Only relevant when `auto_finalize=True`.

eg. `[(60, True, True)]` — 1-minute, live partials + completed,
auto-finalized after the default 4s grace.
eg. `[(60, True, True, 10.0)]` — same, but with a 10s grace period
instead of the default.

When provided, `candle_timeframes` activates the candle engine, which is
inert by default. Candles are constructed from the same deduplicated
feed.

Each candle carries fields: `e, tk, ts, period, time, o, h, l, c, v, oi, oi_delta`.

On live mode (`live=True`), each tick emits a `dict[dict]` with keys
`'previous'` (complete) and `'current'` (partial):

|                          | previous       | current                                          |
|--------------------------|----------------|---------------------------------------------------|
| Day first tick           | `None`         | `is_new: true` (a field in candle tick `current`) |
| interval first tick      | emit candle    | `is_new: true`                                     |
| mid interval             | `None`         | `is_new: false`                                    |
| trigger `auto_finalize`  | emit candle    | `None`                                             |

### `exchange_anchors` — `dict[str, int] | None`

It takes eg. `{"NSE": 13500}`

Per-exchange session-open offset in seconds since UTC midnight. Keys are
exchange codes ("NSE", "MCX", ...). The default covers the Indian
exchanges this library targets (NSE/BSE/NFO/BFO 09:15 IST = 13500;
MCX/CDS/BCD 09:00 IST = 12600).

The dict is not required to be exhaustive: any exchange not present is
treated as UTC-midnight-aligned (offset 0) -- its candles still form,
just bucketed on UTC boundaries instead of a session anchor. So you can
pass a partial dict to override only the exchanges you care about.

Pass `None` (or `{}`) to disable anchoring entirely, making every exchange
UTC-aligned.

### `timescale` — `TimescaleConfig | None`

If not passed, the TimescaleDB sink is completely inert — no connection
is attempted and the rest of the feed is unaffected.

`TimescaleConfig` fields:

| Field                  | Type        | Default                  | Meaning |
|-------------------------|-------------|----------------------------|---------|
| `host`                  | `str`       | required                   | Postgres/TimescaleDB host. |
| `dbname`                | `str`       | required                   | Database name (e.g. `fsticker_db`). |
| `user`                  | `str`       | required                   | Postgres role to connect as. |
| `password`              | `str`       | required                   | Password for that role. |
| `port`                  | `int`       | `5432`                      | Postgres port. |
| `table`                 | `str`       | `"candles"`                 | Base table name completed candles are written to. |
| `table_mode`            | `TableMode` | `TableMode.TIMEFRAME`       | `TableMode.SINGLE` ("single") writes every candle period into one shared table; `TableMode.TIMEFRAME` ("per_timeframe") writes one table per period (`<table>_60`, `<table>_300`, ...). |
| `connect_timeout`       | `float`     | `5.0`                       | Seconds to wait for the initial Postgres connection before giving up. |
| `auto_bootstrap_schema` | `bool`      | `True`                      | Create the table(s)/hypertable(s) automatically on first use if they don't exist. See below. |

### Schema

With `auto_bootstrap_schema=True` (the default), nothing *inside the
database* to set up by hand: on first successful connection the sink
creates the table(s) it needs (`CREATE TABLE IF NOT EXISTS`) and,
best-effort, upgrades them to TimescaleDB hypertables. If the
`timescaledb` extension isn't installed, or your
database user lacks privilege to create it, the sink logs that once
and quietly continues as plain Postgres tables —
persistence still works, you just don't get hypertable
chunking/compression. With `auto_bootstrap_schema=False`, the sink
issues no DDL at all — you are responsible for creating matching
table(s) yourself before starting the feed.

**We'd recommend leaving `auto_bootstrap_schema=True`** and letting the
library manage the schema. The definition below is shown so you know
exactly what gets created — for verification, writing your own read
queries, migrations, etc. — not as something you're expected to run by
hand; `auto_bootstrap_schema=False` exists for locked-down environments
where the app's database role genuinely can't be granted DDL privileges.

Expected schema per table (`table_mode=TIMEFRAME` produces one of these
per configured period, named `<table>_<period>`). The steps below are
what `auto_bootstrap_schema=True` already does for you automatically;
they're spelled out here as the exact DDL to run yourself if you're on
`auto_bootstrap_schema=False`, or just want to see precisely what's
being created.

```sql
CREATE TABLE candles_60 (
    exchange TEXT NOT NULL,
    token TEXT NOT NULL,
    trading_symbol TEXT,
    period BIGINT NOT NULL, 
    "time" BIGINT NOT NULL,             
    ts TIMESTAMP GENERATED ALWAYS AS
        (to_timestamp("time") AT TIME ZONE 'Asia/Kolkata') STORED,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    open_interest BIGINT,
    oi_delta BIGINT,
    PRIMARY KEY (exchange, token, period, "time")
);
```

-- Step 2: the integer-now function. Create this ONCE per database --
-- it's shared by every hypertable you convert, not redeclared per table.
-- Skip this step entirely if you already ran it for an earlier table.

```sql
CREATE OR REPLACE FUNCTION fsticker_sec_now() RETURNS BIGINT
LANGUAGE SQL STABLE AS $$ SELECT EXTRACT(EPOCH FROM now())::BIGINT $$;
```
-- Step 3: convert to a hypertable. . chunk_interval (86400 = 1 day, in seconds) is yours to choose --
-- fsticker itself scales this with the candle period when it bootstraps a
-- table automatically; pick something similar for your own timeframe.

```sql
SELECT create_hypertable('candles_60', by_range('time', 86400::BIGINT),
                          if_not_exists => TRUE);
```

-- Step 4: register the function against THIS table specifically. Repeat
-- this one line (only this one) for every additional hypertable you create --
-- 'fsticker_sec_now' already exists from step 2, don't recreate it.

```sql
SELECT set_integer_now_func('candles_60', 'fsticker_sec_now');
```

See [`docs/timescaledb-setup.md`](https://github.com/Tapanhaz/fsticker/blob/main/docs/timescaledb-setup.md) for a full
guide to getting a Postgres/TimescaleDB server running (Docker,
dedicated role setup, native install, and troubleshooting) — this
section only covers the table `fsticker` itself creates/expects.

## Methods

Sync (`MergedFeed`) and async (`AsyncMergedFeed`) expose the same
methods; the async versions are coroutines (`await feed.subscribe(...)`,
etc.).

### Callbacks — via `start()`, or as direct attributes

`MergedFeed` exposes every callback as a property (`feed.on_tick = ...`),
and `AsyncMergedFeed` exposes them as plain settable attributes — so you
can assign callbacks either before or after `start()`, as an alternative
to passing them all as `start()` kwargs.

### `start(...)` (async: `await start(...)`)

Registers callbacks and begins connecting every configured broker:

| Callback      | Fires with                          |
|---------------|--------------------------------------|
| `on_tick`     | one merged, deduplicated tick (dict) |
| `on_candle`   | one completed/partial candle payload |
| `on_order`    | `(broker, payload)` order updates    |
| `on_error`    | `(broker, payload)` broker errors    |
| `on_open`     | `(broker, payload)` on connect       |
| `on_close`    | `(broker,)` on disconnect            |
| `on_stalled`  | `(broker, consecutive_failures)`     |
| `on_candle_gap` | one dict, see below                |

**`on_candle_gap`** fires when a broker disconnect leaves one or more
tokens with *no* connected broker, so the candles currently forming for
them are missing ticks. A token still carried by another live broker
never appears. Requires `candle_timeframes`.

```python
{
    "broker": "shoonya",  # ================>  the disconnect of the broker that took 
    "tokens": ["NSE|26000", "NSE|26009"],   # the last carrier tokens that went dark
    "periods": [                            # candle buckets open at that moment
        {"period": 60,  "time": 1790238960},
        {"period": 300, "time": 1790238900},
    ],
}
```

`period`/`time` match the fields in `on_candle`, so you can key straight
into your stored candles, e.g. to schedule a historical fetch after the
bucket closes and replace that row. It runs inline on the broker's
thread: hand real work off to your own thread. Tokens with different
exchange session anchors (e.g. NSE and MCX) produce one event per anchor
group.

Every callback is optional and independent — a sync `def` or `async def`
callback both work in async implementation.

### `subscribe(instruments, feed_type=FeedType.Touchline, target="")`

Subscribes to a list of instrument tokens (e.g. `["NSE|26000", "BSE|1"]`).
Safe to call before, during, or after connecting, from any thread (sync)
or task (async), at any point in your code — calls made before
authentication are queued and flushed automatically once each broker's
session is ready, so you don't need to wait for `on_open`/
`wait_connected` before subscribing.

- `feed_type` — `FeedType.Touchline` (default) or `FeedType.SnapQuote` for depth feed.

- `target` — defaults to `""`, meaning "every configured broker". Pass
  a broker's `name` (matching `Credentials.name`) to restrict the call
  to that one session instead — useful when you only want a given
  instrument carried by a single broker.


### `unsubscribe(instruments, feed_type=FeedType.Touchline, target="")`

Same signature and thread/task-safety guarantees as `subscribe()`, in
reverse — stops delivering ticks for the given instruments.

### `wait_connected(timeout=None)`

Blocks (sync) or awaits (async) until every configured broker has
finished authenticating, or `timeout` seconds have elapsed — `None`
means wait indefinitely. Returns `True` if all brokers connected in
time, `False` on timeout. Entirely optional: you can subscribe and
start receiving ticks without ever calling this, since
`start()`/`subscribe()` already handle the "not connected yet" case by
queuing.

### `stop()` (async: `await stop()`)

Requests a graceful shutdown from outside the feed — e.g. from another
thread (sync) or task (async), or from a signal handler of your own.
This is what unblocks `run()` (sync) or completes `wait_closed()`
(async) on demand, as opposed to shutdown happening because every
broker closed on its own (e.g. all sessions hit invalid credentials).

### `run(handle_signals=True, grace_seconds=2.0)` — sync only

Blocks the calling thread, running the feed until `stop()` is called or
the process receives `Ctrl+C`/`SIGTERM`. This is the sync feed's main
entry point — call it last, after `start()` and your initial
`subscribe()` calls.

- `handle_signals` — when `True` (default), `run()` installs its own
  `SIGINT`/`SIGTERM` handling for the duration of the call.
- `grace_seconds` — how long `run()` waits for the websocket connections
  to close gracefully once a shutdown is triggered. If a broker hasn't
  finished closing within that window, it's force-closed rather than
  waited on indefinitely.

On `Ctrl+C`/`SIGTERM`, `run()` closes gracefully (within
`grace_seconds`) and then delivers the interruption to your code as a
plain `KeyboardInterrupt` .

### `wait_closed()` — async only

Awaits until the feed closes — via `stop()`, `Ctrl+C`/`SIGTERM`, or
every broker closing on its own. This is the async equivalent of
`run()`'s blocking behavior, but as a plain awaitable rather than
something that owns the event loop.

### `aclose()` — async only

Performs a clean async shutdown of the feed (closing broker connections,
stopping worker tasks). Always call this in a `finally:` block after
`wait_closed()` — it runs on every exit path, including platforms
without loop signal handlers (Windows).


## Contributing

Issues, bug reports, and pull requests are welcome — whether that's a
fix, a doc improvement, a new broker adapter, or just a question. If
you're planning something larger, opening an issue first to discuss the
approach is appreciated but not required.

## License

Apache License 2.0 — see [`LICENSE`](https://github.com/Tapanhaz/fsticker/blob/main/LICENSE).

The creator puts real effort into keeping this efficient and bug-free,
but software has edges — especially around live market data and money.
This project is provided "as is," without warranty (Apache 2.0, Sections
7–8 cover this formally): the creator and contributors aren't liable for
losses, missed trades, bad data, or anything else that comes from using
it. Use your own judgment, especially before relying on it for real
trading decisions.