# Copyright (c) 2026 Tapanhaz
# Part of fsticker :: https://github.com/Tapanhaz/fsticker
# Licensed under the Apache License, Version 2.0
# Created: 2026-09-25 10:41 IST

from libcpp.vector cimport vector
from libcpp.string cimport string
from cpython.object cimport PyObject
from cpython.exc cimport PyErr_CheckSignals
from cpython.ref cimport Py_DECREF
import threading
from enum import StrEnum, IntEnum
import msgspec
from libc.stdio cimport fprintf, stderr, fflush

from fsticker.feed_bridge cimport (
    PyBrokerSpec,
    PyTimeframeSpec, 
    PyExchangeAnchor,
    PyTimescaleParams,
    PyMergedFeedBridge, 
    FeedType as CFeedType
)

import time


cpdef enum class FeedType:
    Touchline = <int>CFeedType.Touchline
    SnapQuote = <int>CFeedType.SnapQuote

class AccessType(StrEnum):
    API = "API"
    WEB = "WEB"
    MOB = "MOB"

class TableMode(StrEnum):
    SINGLE = "single"
    TIMEFRAME = "per_timeframe"

class LogLevel(IntEnum):
    DEBUG = 0
    INFO = 1
    WARN = 2
    ERROR = 3

class DispatchMode(StrEnum):
    QUEUED = "queued"
    INLINE = "inline"


cdef bint _on_main_thread() except -1:
    return threading.current_thread() is threading.main_thread()

cdef inline object _steal(PyObject* p):
    
    cdef object o = <object>p
    Py_DECREF(o)
    return o

cdef class Credentials:
    cdef:
        public str name
        public str ws_endpoint
        public str user_id
        public str token
        public object access_type
        public bint verify_ssl
        public bint enable_ip_pinning
        public double idle_ping_timeout
        public double ping_reply_timeout
        public str ping_payload
        public object min_log_level

    def __init__(
            self, 
            str name, 
            str ws_endpoint, 
            str user_id, 
            str token, 
            object access_type=AccessType.API,
            bool verify_ssl=False, 
            bool enable_ip_pinning=False,
            double idle_ping_timeout=3.0,
            double ping_reply_timeout=2.0,
            str ping_payload='{"t":"h"}',
            object min_log_level=LogLevel.INFO,
            ):
        self.name = name
        self.ws_endpoint = ws_endpoint
        self.user_id = user_id
        self.token = token
        self.access_type = access_type
        self.verify_ssl = verify_ssl
        self.enable_ip_pinning = enable_ip_pinning
        self.idle_ping_timeout = idle_ping_timeout
        self.ping_reply_timeout = ping_reply_timeout
        self.ping_payload = ping_payload
        self.min_log_level = min_log_level

cdef class TimescaleConfig:
    cdef:
        public str host
        public int port
        public str dbname
        public str user
        public str password
        public str table
        public str table_mode
        public double connect_timeout
        public bint auto_bootstrap_schema

    def __init__(
            self, 
            str host, 
            str dbname, 
            str user, 
            str password,
            int port=5432, 
            str table="candles",
            object table_mode=TableMode.TIMEFRAME, 
            double connect_timeout=5.0, 
            bint auto_bootstrap_schema=True
            ):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password        
        self.table = table
        self.table_mode = table_mode.value
        self.connect_timeout = connect_timeout
        self.auto_bootstrap_schema = auto_bootstrap_schema



cdef class MergedFeed:
    cdef:
        PyMergedFeedBridge* _bridge
        object _on_tick
        object _on_candle
        object _on_order
        object _on_error
        object _on_open
        object _on_close
        object _on_stalled
        object _on_shutdown
        object _on_log
        object _on_candle_gap
        object _dispatch_mode
        object _tick_thread
        object _candle_thread
        object _dispatch_stop
        object _notify_sock
        object _notify_candle_sock
        object _json_decoder
        bint _started

        int _tick_queue_capacity
        bint _tick_queue_overwrite
        int _candle_queue_capacity
        bint _candle_queue_overwrite


    def __init__(
            self, 
            list brokers, 
            list candle_timeframes=None,
            dict exchange_anchors={"NSE": 13500, "BSE": 13500, "NFO": 13500, "BFO": 13500, "MCX": 12600, "CDS": 12600, "BCD": 12600},
            object timescale=None,
            object dispatch_mode=DispatchMode.QUEUED,
            int tick_queue_capacity=100_000, 
            bint tick_queue_overwrite=True, 
            int candle_queue_capacity=100_000, 
            bint candle_queue_overwrite=True
            ):
        cdef:
            vector[PyBrokerSpec] specs
            PyBrokerSpec spec
            Credentials b
            vector[PyTimeframeSpec] tf_specs
            PyTimeframeSpec tf
            vector[PyExchangeAnchor] anchors
            PyExchangeAnchor anchor

        if dispatch_mode not in (DispatchMode.QUEUED, DispatchMode.INLINE):
            raise ValueError(
                "MergedFeed.dispatch_mode must be DispatchMode.QUEUED or "
                f"DispatchMode.INLINE, got {dispatch_mode!r}"
            )
        self._dispatch_mode = dispatch_mode
        self._dispatch_stop = threading.Event()
        self._tick_thread = None
        self._candle_thread = None


        for b in brokers:
            spec = PyBrokerSpec()
            spec.name = b.name.encode("utf-8")
            spec.ws_endpoint = b.ws_endpoint.encode("utf-8")
            spec.user_id = b.user_id.encode("utf-8")
            spec.token = b.token.encode("utf-8")
            spec.access_type = b.access_type.value.encode("utf-8")
            spec.verify_ssl = b.verify_ssl
            spec.enable_ip_pinning = b.enable_ip_pinning
            spec.idle_ping_timeout_ms = <long long>(b.idle_ping_timeout * 1000)
            spec.ping_reply_timeout_ms = <long long>(b.ping_reply_timeout * 1000)
            spec.ping_payload = b.ping_payload.encode("utf-8")
            spec.min_log_level = <int>b.min_log_level
            specs.push_back(spec)

        self._tick_queue_capacity = tick_queue_capacity
        self._tick_queue_overwrite = tick_queue_overwrite
        self._candle_queue_capacity = candle_queue_capacity
        self._candle_queue_overwrite = candle_queue_overwrite

        self._bridge = new PyMergedFeedBridge(specs)


        if candle_timeframes:
            for tf_spec in candle_timeframes:
                if len(tf_spec) == 4:
                    seconds, live, auto_finalize, grace = tf_spec
                elif len(tf_spec) == 3:
                    seconds, live, auto_finalize = tf_spec
                    grace = 4.0
                else:
                    raise ValueError(
                        "each candle_timeframes entry must be a 3-tuple "
                        "(seconds, live, auto_finalize) or a 4-tuple adding "
                        f"auto_finalize_grace_seconds, got {tf_spec!r}"
                    )
                tf = PyTimeframeSpec()
                tf.seconds = <long long>seconds
                tf.live = <bint>live
                tf.auto_finalize = <bint>auto_finalize
                tf.auto_finalize_grace_seconds = <long long>grace
                tf_specs.push_back(tf)
                
            if exchange_anchors:
                for exchange, secs in exchange_anchors.items():
                    anchor = PyExchangeAnchor()
                    anchor.exchange = exchange.encode("utf-8")
                    anchor.anchor_seconds = <long long>secs
                    anchors.push_back(anchor)
            self._bridge.configure_candles(tf_specs, anchors)

        cdef PyTimescaleParams tsp 

        if timescale is not None:  
            tsp = PyTimescaleParams()          
            tsp.host = timescale.host.encode("utf-8")
            tsp.port = <unsigned short>timescale.port
            tsp.dbname = timescale.dbname.encode("utf-8")
            tsp.user = timescale.user.encode("utf-8")
            tsp.password = timescale.password.encode("utf-8")
            tsp.table = timescale.table.encode("utf-8")
            tsp.table_mode = 1 if timescale.table_mode == "per_timeframe" else 0
            tsp.connect_timeout_ms = <long long>(timescale.connect_timeout * 1000)
            tsp.auto_bootstrap_schema = timescale.auto_bootstrap_schema
            self._bridge.configure_timescale(tsp)

        self._on_tick = None
        self._on_order = None
        self._on_error = None
        self._on_open = None
        self._on_close = None
        self._on_stalled = None
        self._on_shutdown = None
        self._on_log = None
        self._on_candle_gap = None
        self._notify_sock = None
        self._notify_candle_sock = None
        self._on_candle = None
        self._json_decoder = msgspec.json.Decoder()
        self._started = False

    def __dealloc__(self):
        cdef bint joined
        self._stop_dispatch_threads()
        if self._bridge is not NULL:
            with nogil:
                joined = self._bridge.close()
            if joined:
                del self._bridge
            self._bridge = NULL

    cdef object _wrap_json(self, object cb, str which):
        if cb is None:
            return None
            
        cdef bytes which_b = which.encode("utf-8")
        def _decoded(broker, msg):
            cdef bytes broker_b, err_b
            decoder = self._json_decoder
            try:
                payload = decoder.decode(msg)
            except msgspec.DecodeError as exc:
                broker_b = broker.encode("utf-8")
                err_b    = str(exc).encode("utf-8")
                fprintf(stderr, b"[fsticker][WARN] malformed JSON from [%s] on %s, skipped: %s\n",
                      <char*>broker_b, <char*>which_b, <char*>err_b)
                fflush(stderr)
                return
            return cb(broker, payload)
        return _decoded

    property on_tick:
        def __set__(self, cb):
            self._on_tick = cb 
            #self._bridge.set_tick_callback(<PyObject*>cb)
            if self._dispatch_mode == DispatchMode.INLINE:
                self._bridge.set_tick_callback(<PyObject*>cb)

    property on_candle:
        def __set__(self, cb):
            self._on_candle = cb
            #self._bridge.set_candle_callback(<PyObject*>cb)
            if self._dispatch_mode == DispatchMode.INLINE:
                self._bridge.set_candle_callback(<PyObject*>cb)


    property on_order:
        def __set__(self, cb):
            self._on_order = cb
            wrapped = self._wrap_json(cb, "on_order")
            self._on_order = wrapped  
            self._bridge.set_order_callback(<PyObject*>wrapped)

    property on_error:
        def __set__(self, cb):
            self._on_error = cb
            wrapped = self._wrap_json(cb, "on_error")
            self._on_error = wrapped
            self._bridge.set_error_callback(<PyObject*>wrapped)

    property on_open:
        def __set__(self, cb):
            self._on_open = cb
            wrapped = self._wrap_json(cb, "on_open")
            self._on_open = wrapped
            self._bridge.set_open_callback(<PyObject*>wrapped)

    property on_close:
        def __set__(self, cb):
            self._on_close = cb
            self._bridge.set_close_callback(<PyObject*>cb)

    property on_stalled:
        def __set__(self, cb):
            self._on_stalled = cb
            self._bridge.set_stalled_callback(<PyObject*>cb)

    property on_shutdown:
        def __set__(self, cb):
            self._on_shutdown = cb
            self._bridge.set_shutdown_callback(<PyObject*>cb)

    property on_log:
        def __set__(self, cb):
            self._on_log = cb
            self._bridge.set_log_callback(<PyObject*>cb)
    
    property on_candle_gap:
        def __set__(self, cb):
            self._on_candle_gap = cb
            self._bridge.set_candle_gap_callback(<PyObject*>cb)

    cpdef object _enable_async_ticks(self):
        self._bridge.set_tick_queue_limits(<size_t>self._tick_queue_capacity, self._tick_queue_overwrite)
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("127.0.0.1", 0))
        sock.connect(sock.getsockname())
        sock.setblocking(False)
        self._notify_sock = sock 
        self._bridge.enable_async_ticks(sock.fileno())
        return sock

    @property
    def dropped_ticks(self):
        return self._bridge.dropped_ticks()

    cpdef object _pop_tick(self):
        return _steal(self._bridge.pop_tick_as_pydict())

    cpdef void _begin_ticks_wait(self):
        self._bridge.begin_ticks_wait()

    cpdef void _end_ticks_wait(self):
        self._bridge.end_ticks_wait()

    cpdef object _enable_async_candles(self):
        self._bridge.set_candle_queue_limits(<size_t>self._candle_queue_capacity, self._candle_queue_overwrite)
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("127.0.0.1", 0))
        sock.connect(sock.getsockname())
        sock.setblocking(False)
        self._notify_candle_sock = sock
        self._bridge.enable_async_candles(sock.fileno())
        return sock

    @property
    def dropped_candles(self):
        return self._bridge.dropped_candles()

    cpdef object _pop_candle(self):
        return _steal(self._bridge.pop_candle_as_py())

    cpdef void _begin_candles_wait(self):
        self._bridge.begin_candles_wait()

    cpdef void _end_candles_wait(self):
        self._bridge.end_candles_wait()

    def _tick_worker(self):
        sock = self._notify_sock
        stop = self._dispatch_stop
        while not stop.is_set():
            self._begin_ticks_wait()
            tick = self._pop_tick()
            if tick is not None:
                self._end_ticks_wait()
                cb = self._on_tick
                if cb is not None:
                    try:
                        cb(tick)
                    except Exception:  # noqa: BLE001
                        import traceback
                        traceback.print_exc()
            else:
                try:
                    sock.recv(64)
                except OSError:
                    self._end_ticks_wait()
                    return
                self._end_ticks_wait()

    def _candle_worker(self):
        sock = self._notify_candle_sock
        stop = self._dispatch_stop
        while not stop.is_set():
            self._begin_candles_wait()
            payload = self._pop_candle()
            if payload is not None:
                self._end_candles_wait()
                cb = self._on_candle
                if cb is not None:
                    try:
                        cb(payload)
                    except Exception:  # noqa: BLE001
                        import traceback
                        traceback.print_exc()
            else:
                try:
                    sock.recv(64)
                except OSError:
                    self._end_candles_wait()
                    return
                self._end_candles_wait()

    def _start_dispatch_threads(self):
        if self._dispatch_mode != DispatchMode.QUEUED:
            return
        self._dispatch_stop.clear()
        
        if self._on_tick is not None:
            self._notify_sock = self._enable_async_ticks()
            self._notify_sock.setblocking(True)
            self._tick_thread = threading.Thread(
                target=self._tick_worker, name="fsticker-tick-dispatch", daemon=True
            )
            self._tick_thread.start()
        if self._on_candle is not None:
            self._notify_candle_sock = self._enable_async_candles()
            self._notify_candle_sock.setblocking(True)
            self._candle_thread = threading.Thread(
                target=self._candle_worker, name="fsticker-candle-dispatch", daemon=True
            )
            self._candle_thread.start()

    def _stop_dispatch_threads(self):
        self._dispatch_stop.set()
        if self._tick_thread is not None and self._notify_sock is not None:
            try:
                self._notify_sock.send(b"\0")
            except OSError:
                pass
        if self._candle_thread is not None and self._notify_candle_sock is not None:
            try:
                self._notify_candle_sock.send(b"\0")
            except OSError:
                pass
        if self._tick_thread is not None:
            self._tick_thread.join(timeout=5.0)
            self._tick_thread = None
        if self._candle_thread is not None:
            self._candle_thread.join(timeout=5.0)
            self._candle_thread = None


    cpdef void start(
        self, 
        object on_tick=None, 
        object on_candle=None,
        object on_order=None, 
        object on_error=None,
        object on_open=None, 
        object on_close=None, 
        object on_stalled=None, 
        object on_shutdown=None,
        object on_log=None,
        object on_candle_gap=None
    ):
        if on_tick     is not None: 
            self.on_tick     = on_tick
        if on_candle   is not None: 
            self.on_candle   = on_candle
        if on_order    is not None: 
            self.on_order    = on_order
        if on_error    is not None: 
            self.on_error    = on_error
        if on_open     is not None: 
            self.on_open     = on_open
        if on_close    is not None: 
            self.on_close    = on_close
        if on_stalled  is not None: 
            self.on_stalled  = on_stalled
        if on_shutdown is not None: 
            self.on_shutdown = on_shutdown
        if on_log      is not None:
            self.on_log      = on_log
        if on_candle_gap is not None:
            self.on_candle_gap = on_candle_gap

        self._start_dispatch_threads()

        self._bridge.start()
        self._started = True

    cpdef void stop(self):
        self._bridge.stop()
   
    cpdef void subscribe(self, list instruments, FeedType feed_type= FeedType.Touchline, str target=""):
        cdef:
            vector[string] cinstruments
            CFeedType feedtype =  <CFeedType>feed_type
        
        for i in instruments:
            cinstruments.push_back(i.encode("utf-8"))
        
        self._bridge.subscribe(cinstruments, feedtype, target.encode("utf-8"))

    cpdef void unsubscribe(self, list instruments, FeedType feed_type= FeedType.Touchline, str target=""):
        cdef:
            vector[string] cinstruments
            CFeedType feedtype =  <CFeedType>feed_type
            string ctarget = target.encode("utf-8")

        for i in instruments:
            cinstruments.push_back(i.encode("utf-8"))
        with nogil:
            self._bridge.unsubscribe(cinstruments, feedtype, ctarget)
            
    cdef int _check_signals(self) except -1:
        try:
            PyErr_CheckSignals()
        except KeyboardInterrupt:
            self.stop()
            raise
        return 0

    cpdef wait_connected(self, timeout=None):
        cdef:
            long long ms = -1
            double t
            
        if not self._started:
            raise RuntimeError("MergedFeed.wait_connected() called before start()")
        if timeout is not None:
            t = <double>timeout
            ms = 0 if t <= 0.0 else (<long long>(t * 1000.0) if t < 1.0e9 else -1)
        try:
            return self._bridge.wait_connected(ms, _on_main_thread()) > 0
        except KeyboardInterrupt:
            self.stop()
            raise

    cpdef run(self, bint handle_signals=True, double grace_seconds=2.0):
        cdef:
            int rc = 0
            bint closed = False
            bint main_thread = _on_main_thread()
            double grace_ms = grace_seconds * 1000.0

        if not self._started:
            raise RuntimeError("MergedFeed.run() called before start()")
        grace_ms = 0.0 if grace_ms < 0.0 else (2.0e9 if grace_ms > 2.0e9 else grace_ms)

        try:
            if handle_signals:
                if not main_thread:
                    raise RuntimeError("run(handle_signals=True) needs the main thread; "
                                       "pass handle_signals=False")
                with nogil:
                    rc = self._bridge.run(True, <int>grace_ms)
                if rc == -3:
                    raise RuntimeError("another MergedFeed.run() already handles SIGINT/SIGTERM; "
                                       "pass handle_signals=False")
                if rc == -4:
                    raise RuntimeError("fsticker: internal error while waiting for shutdown")
                if rc > 0:
                    self._bridge.reraise(rc)
                    PyErr_CheckSignals()  
            else:
                try:
                    self._bridge.run_passive(main_thread)
                    closed = True
                finally:
                    if not closed:
                        self.stop()
                    with nogil:
                        self._bridge.join_all()
        finally:
            self._stop_dispatch_threads()

    @property
    def broker_count(self):
        return self._bridge.broker_count()
