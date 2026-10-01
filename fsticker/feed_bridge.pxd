# Copyright (c) 2026 Tapanhaz
# Part of fsticker :: https://github.com/Tapanhaz/fsticker
# Licensed under the Apache License, Version 2.0
# Created: 2026-09-25 10:41 IST

from libcpp.string cimport string
from libcpp.vector cimport vector
from cpython.object cimport PyObject

cdef extern from "types.hpp" namespace "fsticker":
    cdef enum class FeedType:
        Touchline
        SnapQuote

cdef extern from "bridge.hpp" namespace "fsticker::pybridge":
    cdef cppclass PyBrokerSpec:
        PyBrokerSpec()
        string name
        string ws_endpoint
        string user_id
        string token
        string access_type
        bint verify_ssl
        bint enable_ip_pinning
        long long idle_ping_timeout_ms
        long long ping_reply_timeout_ms
        string ping_payload
        int min_log_level

    cdef cppclass PyTimeframeSpec:
        PyTimeframeSpec()
        long long seconds
        bint live
        bint auto_finalize
        long long auto_finalize_grace_seconds
        bint omit_possible_partial

    cdef cppclass PyExchangeAnchor:
        PyExchangeAnchor()
        string exchange
        long long anchor_seconds

    cdef cppclass PyTimescaleParams:
        PyTimescaleParams()
        string host
        unsigned short port
        string dbname
        string user
        string password
        string table
        int table_mode
        long long connect_timeout_ms
        size_t queue_capacity
        bint queue_overwrite
        bint auto_bootstrap_schema


    cdef cppclass PyMergedFeedBridge:
        PyMergedFeedBridge(vector[PyBrokerSpec] specs) except +

        void set_tick_callback(PyObject* callable)
        void set_order_callback(PyObject* callable)
        void set_error_callback(PyObject* callable)
        void set_open_callback(PyObject* callable)
        void set_close_callback(PyObject* callable)
        void set_stalled_callback(PyObject* callable)
        void set_shutdown_callback(PyObject* callable)
        void set_log_callback(PyObject* callable)
        void set_candle_callback(PyObject* callable)
        void set_candle_gap_callback(PyObject* callable)
        void set_candle_gap_report_callback(PyObject* callable)
        void configure_candles(vector[PyTimeframeSpec] specs, vector[PyExchangeAnchor] anchors)
        void configure_timescale(PyTimescaleParams params)

        void enable_async_ticks(int notify_fd)        
        PyObject* pop_tick_as_pydict() except NULL
        void begin_ticks_wait()
        void end_ticks_wait()
        void set_tick_queue_limits(size_t capacity, bint overwrite)
        size_t dropped_ticks()        

        void enable_async_candles(int notify_fd)
        PyObject* pop_candle_as_py() except NULL
        void begin_candles_wait()
        void end_candles_wait()
        void set_candle_queue_limits(size_t capacity, bint overwrite)
        size_t dropped_candles()

        void enable_async_gap_reports(int notify_fd)
        PyObject* pop_gap_report_as_pydict() except NULL
        void begin_gap_reports_wait()
        void end_gap_reports_wait()
        void set_gap_report_queue_limits(size_t capacity, bint overwrite)
        size_t dropped_gap_reports()

        void start()
        void stop()

        int run(bint handle_signals, int grace_ms) nogil
        int run_passive(bint main_thread) except -2

        void reraise(int sig)
        bint close() nogil
        bint join_all() nogil

        void subscribe(vector[string] instruments, FeedType feed_type, string target)
        void unsubscribe(vector[string] instruments, FeedType feed_type, string target) nogil

        int wait_connected(long long timeout_ms, bint main_thread) except -2

        size_t broker_count()
