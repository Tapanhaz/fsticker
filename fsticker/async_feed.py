# Copyright (c) 2026 Tapanhaz
# Part of fsticker :: https://github.com/Tapanhaz/fsticker
# Licensed under the Apache License, Version 2.0
# Created: 2026-09-25 10:41 IST

from __future__ import annotations

import asyncio
import inspect
import signal
import sys
import threading
from collections.abc import Awaitable, Callable
from typing import Union

from fsticker.feed_merge import (
    AccessType,
    Credentials,
    FeedType,
    MergedFeed,
)  # type: ignore

__all__ = ["AccessType", "AsyncMergedFeed", "Credentials", "FeedType"]

Callback = Union[Callable[..., None], Callable[..., Awaitable[None]]]


def _noop(*_args: object) -> None:
    return None


class AsyncMergedFeed:
    def __init__(
        self,
        brokers: list[Credentials],
        candle_timeframes: list[tuple[int, bool, bool]] | None = None,
        exchange_anchors: dict = {  # noqa: B006
            "NSE": 13500,
            "BSE": 13500,
            "NFO": 13500,
            "BFO": 13500,
            "MCX": 12600,
            "CDS": 12600,
            "BCD": 12600,
        },
        timescale: object = None,
        tick_queue_capacity: int = 100_000,
        tick_queue_overwrite: bool = True,
        candle_queue_capacity: int = 100_000,
        candle_queue_overwrite: bool = True,
    ):
        self._feed = MergedFeed(
            brokers,
            candle_timeframes=candle_timeframes,
            exchange_anchors=exchange_anchors,
            timescale=timescale,
            tick_queue_capacity=tick_queue_capacity,
            tick_queue_overwrite=tick_queue_overwrite,
            candle_queue_capacity=candle_queue_capacity,
            candle_queue_overwrite=candle_queue_overwrite,
        )
        self._broker_names = [b.name for b in brokers]
        self._loop: asyncio.AbstractEventLoop | None = None
        self._notify_sock = None
        self._tick_task: asyncio.Task | None = None
        self._notify_candle_sock = None
        self._candle_task: asyncio.Task | None = None
        self._closed: asyncio.Event | None = None
        self._connected_events: dict[str, asyncio.Event] = {}
        self._running = False
        self._stopped = False
        self._signal: int | None = None
        self._prev_handlers: dict[int, object] = {}

        self._on_tick: Callback | None = None
        self._process_tick = _noop
        self._on_candle: Callback | None = None
        self._process_candle = _noop
        self._on_order: Callback | None = None
        self._dispatch_order = _noop
        self._on_error: Callback | None = None
        self._dispatch_error = _noop
        self._on_open_cb: Callback | None = None
        self._dispatch_open = _noop
        self._on_close_cb: Callback | None = None
        self._dispatch_close = _noop
        self._on_stalled: Callback | None = None
        self._dispatch_stalled = _noop
        self._on_log: Callback | None = None
        self._dispatch_log = _noop
        self._on_candle_gap: Callback | None = None
        self._dispatch_candle_gap = _noop

    async def _process_tick_sync(self, tick: object) -> None:
        self._on_tick(tick)

    async def _process_tick_async(self, tick: object) -> None:
        await self._on_tick(tick)

    @property
    def on_tick(self) -> Callback | None:
        return self._on_tick

    @on_tick.setter
    def on_tick(self, cb: Callback | None) -> None:
        self._on_tick = cb
        if cb is None:
            self._process_tick = _noop
        elif inspect.iscoroutinefunction(cb):
            self._process_tick = self._process_tick_async
        else:
            self._process_tick = self._process_tick_sync

    async def _process_candle_sync(self, payload: object) -> None:
        self._on_candle(payload)

    async def _process_candle_async(self, payload: object) -> None:
        await self._on_candle(payload)

    @property
    def on_candle(self) -> Callback | None:
        return self._on_candle

    @on_candle.setter
    def on_candle(self, cb: Callback | None) -> None:
        self._on_candle = cb
        if cb is None:
            self._process_candle = _noop
        elif inspect.iscoroutinefunction(cb):
            self._process_candle = self._process_candle_async
        else:
            self._process_candle = self._process_candle_sync

    def _dispatch_sync_order(self, *args: object) -> None:
        self._on_order(*args)

    def _dispatch_async_order(self, *args: object) -> None:
        self._loop.create_task(self._on_order(*args))

    @property
    def on_order(self) -> Callback | None:
        return self._on_order

    @on_order.setter
    def on_order(self, cb: Callback | None) -> None:
        self._on_order = cb
        if cb is None:
            self._dispatch_order = _noop
        elif inspect.iscoroutinefunction(cb):
            self._dispatch_order = self._dispatch_async_order
        else:
            self._dispatch_order = self._dispatch_sync_order

    def _dispatch_sync_error(self, *args: object) -> None:
        self._on_error(*args)

    def _dispatch_async_error(self, *args: object) -> None:
        self._loop.create_task(self._on_error(*args))

    @property
    def on_error(self) -> Callback | None:
        return self._on_error

    @on_error.setter
    def on_error(self, cb: Callback | None) -> None:
        self._on_error = cb
        if cb is None:
            self._dispatch_error = _noop
        elif inspect.iscoroutinefunction(cb):
            self._dispatch_error = self._dispatch_async_error
        else:
            self._dispatch_error = self._dispatch_sync_error

    def _dispatch_sync_stalled(self, *args: object) -> None:
        self._on_stalled(*args)

    def _dispatch_async_stalled(self, *args: object) -> None:
        self._loop.create_task(self._on_stalled(*args))

    @property
    def on_stalled(self) -> Callback | None:
        return self._on_stalled

    @on_stalled.setter
    def on_stalled(self, cb: Callback | None) -> None:
        self._on_stalled = cb
        if cb is None:
            self._dispatch_stalled = _noop
        elif inspect.iscoroutinefunction(cb):
            self._dispatch_stalled = self._dispatch_async_stalled
        else:
            self._dispatch_stalled = self._dispatch_sync_stalled

    def _dispatch_sync_log(self, *args: object) -> None:
        self._on_log(*args)

    def _dispatch_async_log(self, *args: object) -> None:
        self._loop.create_task(self._on_log(*args))

    @property
    def on_log(self) -> Callback | None:
        return self._on_log

    @on_log.setter
    def on_log(self, cb: Callback | None) -> None:
        self._on_log = cb
        if cb is None:
            self._dispatch_log = _noop
        elif inspect.iscoroutinefunction(cb):
            self._dispatch_log = self._dispatch_async_log
        else:
            self._dispatch_log = self._dispatch_sync_log

    def _dispatch_sync_candle_gap(self, *args: object) -> None:
        self._on_candle_gap(*args)

    def _dispatch_async_candle_gap(self, *args: object) -> None:
        self._loop.create_task(self._on_candle_gap(*args))

    @property
    def on_candle_gap(self) -> Callback | None:
        return self._on_candle_gap

    @on_candle_gap.setter
    def on_candle_gap(self, cb: Callback | None) -> None:
        self._on_candle_gap = cb
        if cb is None:
            self._dispatch_candle_gap = _noop
        elif inspect.iscoroutinefunction(cb):
            self._dispatch_candle_gap = self._dispatch_async_candle_gap
        else:
            self._dispatch_candle_gap = self._dispatch_sync_candle_gap

    def _dispatch_sync_open(self, *args: object) -> None:
        self._on_open_cb(*args)

    def _dispatch_async_open(self, *args: object) -> None:
        self._loop.create_task(self._on_open_cb(*args))

    @property
    def on_open(self) -> Callback | None:
        return self._on_open_cb

    @on_open.setter
    def on_open(self, cb: Callback | None) -> None:
        self._on_open_cb = cb
        if cb is None:
            self._dispatch_open = _noop
        elif inspect.iscoroutinefunction(cb):
            self._dispatch_open = self._dispatch_async_open
        else:
            self._dispatch_open = self._dispatch_sync_open

    def _dispatch_sync_close(self, *args: object) -> None:
        self._on_close_cb(*args)

    def _dispatch_async_close(self, *args: object) -> None:
        self._loop.create_task(self._on_close_cb(*args))

    @property
    def on_close(self) -> Callback | None:
        return self._on_close_cb

    @on_close.setter
    def on_close(self, cb: Callback | None) -> None:
        self._on_close_cb = cb
        if cb is None:
            self._dispatch_close = _noop
        elif inspect.iscoroutinefunction(cb):
            self._dispatch_close = self._dispatch_async_close
        else:
            self._dispatch_close = self._dispatch_sync_close

    async def start(
        self,
        on_tick: Callback | None = None,
        on_candle: Callback | None = None,
        on_order: Callback | None = None,
        on_error: Callback | None = None,
        on_open: Callback | None = None,
        on_close: Callback | None = None,
        on_stalled: Callback | None = None,
        on_log: Callback | None = None,
        on_candle_gap: Callback | None = None,
    ) -> AsyncMergedFeed:
        if on_tick is not None:
            self.on_tick = on_tick
        if on_candle is not None:
            self.on_candle = on_candle
        if on_order is not None:
            self.on_order = on_order
        if on_error is not None:
            self.on_error = on_error
        if on_open is not None:
            self.on_open = on_open
        if on_close is not None:
            self.on_close = on_close
        if on_stalled is not None:
            self.on_stalled = on_stalled
        if on_log is not None:
            self.on_log = on_log
        if on_candle_gap is not None:
            self.on_candle_gap = on_candle_gap

        self._loop = asyncio.get_running_loop()
        self._closed = asyncio.Event()
        self._connected_events = {name: asyncio.Event() for name in self._broker_names}
        self._running = True

        self._notify_sock = self._feed._enable_async_ticks()
        self._tick_task = self._loop.create_task(self._read_ticks())

        self._notify_candle_sock = self._feed._enable_async_candles()
        self._candle_task = self._loop.create_task(self._read_candles())

        loop = self._loop
        self._feed.on_order = lambda b, m: loop.call_soon_threadsafe(
            self._dispatch_order, b, m
        )
        self._feed.on_error = lambda b, m: loop.call_soon_threadsafe(
            self._dispatch_error, b, m
        )
        self._feed.on_open = lambda b, m: loop.call_soon_threadsafe(self._on_open, b, m)
        self._feed.on_close = lambda b: loop.call_soon_threadsafe(self._on_close, b)
        self._feed.on_stalled = lambda b, n: loop.call_soon_threadsafe(
            self._dispatch_stalled, b, n
        )
        self._feed.on_log = lambda b, lvl, m: loop.call_soon_threadsafe(
            self._dispatch_log, b, lvl, m
        )
        self._feed.on_candle_gap = lambda p: loop.call_soon_threadsafe(
            self._dispatch_candle_gap, p
        )
        self._feed.on_shutdown = lambda: loop.call_soon_threadsafe(self._closed.set)

        self._install_signal_handlers()

        self._feed.start()
        return self

    async def __aenter__(self) -> AsyncMergedFeed:  # noqa: PYI034
        return await self.start()

    def _on_open(self, broker: str, msg: str) -> None:
        ev = self._connected_events.get(broker)
        if ev is not None:
            ev.set()
        self._dispatch_open(broker, msg)

    def _on_close(self, broker: str) -> None:
        ev = self._connected_events.get(broker)
        if ev is not None:
            ev.clear()
        self._dispatch_close(broker)

    def _install_signal_handlers(self) -> None:
        if sys.platform == "win32":
            return
        if threading.current_thread() is not threading.main_thread():
            return
        for sig in (signal.SIGINT, signal.SIGTERM):
            prev = signal.getsignal(sig)
            if prev == signal.SIG_IGN:
                continue
            self._loop.add_signal_handler(sig, self._on_signal, sig)
            self._prev_handlers[sig] = prev

    def _on_signal(self, sig: int) -> None:
        if self._signal is None:
            self._signal = sig
        self._sync_stop()

    def _restore_signal_handlers(self) -> None:
        while self._prev_handlers:
            sig, prev = self._prev_handlers.popitem()
            try:
                self._loop.remove_signal_handler(sig)
                signal.signal(sig, prev if prev is not None else signal.SIG_DFL)
            except (ValueError, RuntimeError, OSError):
                pass

    def _sync_stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        self._feed.stop()

    async def _read_ticks(self) -> None:
        counter = 0
        while self._running:
            self._feed._begin_ticks_wait()
            tick = self._feed._pop_tick()
            if tick is not None:
                self._feed._end_ticks_wait()
                try:
                    await self._process_tick(tick)
                except Exception as exc:  # noqa: BLE001
                    self._loop.call_exception_handler(
                        {
                            "message": "exception in AsyncMergedFeed.on_tick",
                            "exception": exc,
                        }
                    )
                counter = (counter + 1) & 127
                if counter == 0:
                    await asyncio.sleep(0)  # starve prevention !!
            else:
                try:
                    await self._loop.sock_recv(self._notify_sock, 64)
                except asyncio.CancelledError:
                    self._feed._end_ticks_wait()
                    return
                except OSError:
                    self._feed._end_ticks_wait()
                    if not self._running:
                        return
                    raise
                self._feed._end_ticks_wait()

    async def _read_candles(self) -> None:
        counter = 0
        while self._running:
            self._feed._begin_candles_wait()
            msg = self._feed._pop_candle()
            if msg is not None:
                self._feed._end_candles_wait()
                try:
                    await self._process_candle(msg)
                except Exception as exc:  # noqa: BLE001
                    self._loop.call_exception_handler(
                        {
                            "message": "exception in AsyncMergedFeed.on_candle",
                            "exception": exc,
                        }
                    )
                counter = (counter + 1) & 127
                if counter == 0:
                    await asyncio.sleep(0)
            else:
                try:
                    await self._loop.sock_recv(self._notify_candle_sock, 64)
                except asyncio.CancelledError:
                    self._feed._end_candles_wait()
                    return
                except OSError:
                    self._feed._end_candles_wait()
                    if not self._running:
                        return
                    raise
                self._feed._end_candles_wait()

    async def subscribe(
        self,
        instruments: list[str],
        feed_type: FeedType = FeedType.Touchline,
        target: str = "",
    ) -> None:
        self._feed.subscribe(instruments, feed_type, target)

    async def unsubscribe(
        self,
        instruments: list[str],
        feed_type: FeedType = FeedType.Touchline,
        target: str = "",
    ) -> None:
        self._feed.unsubscribe(instruments, feed_type, target)

    async def wait_connected(self, timeout: float | None = None) -> bool:
        connect_task = asyncio.ensure_future(
            asyncio.gather(*(ev.wait() for ev in self._connected_events.values()))
        )
        closed_task = asyncio.ensure_future(self._closed.wait())
        done, pending = await asyncio.wait(
            {connect_task, closed_task},
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for p in pending:
            p.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        return connect_task in done

    async def wait_closed(self) -> None:
        await self._closed.wait()

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        self._feed.stop()

    async def aclose(self) -> None:
        self._restore_signal_handlers()

        if not self._stopped:
            self._stopped = True
            self._feed.stop()
        await self._closed.wait()

        self._running = False
        if self._tick_task is not None:
            self._tick_task.cancel()
            try:
                await self._tick_task
            except asyncio.CancelledError:
                pass
        if self._candle_task is not None:
            self._candle_task.cancel()
            try:
                await self._candle_task
            except asyncio.CancelledError:
                pass

        if self._notify_sock is not None:
            self._notify_sock.close()
            self._notify_sock = None

        if self._notify_candle_sock is not None:
            self._notify_candle_sock.close()
            self._notify_candle_sock = None

        sig, self._signal = self._signal, None
        if sig is not None:
            signal.raise_signal(sig)

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        await self.aclose()
        return False

    @property
    def broker_count(self) -> int:
        return self._feed.broker_count
