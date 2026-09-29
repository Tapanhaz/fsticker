// Copyright (c) 2026 Tapanhaz
// Part of fsticker :: https://github.com/Tapanhaz/fsticker
// Licensed under the Apache License, Version 2.0
// Created: 2026-09-25 10:41 IST

#pragma once

#define PY_SSIZE_T_CLEAN
#include "async_candle_bridge.hpp"
#include "async_tick_bridge.hpp"
#include "merge/merger_all.hpp"
#include "signal_guard.hpp"

#include <Python.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <boost/asio/io_context.hpp>
#include <boost/container/small_vector.hpp>
#include <chrono>
#include <climits>
#include <cstdio>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#if defined(FSTICKER_HAVE_DICT_SETITEM_KNOWNHASH)
// Private CPython symbol, no public header declares it as of 3.13
extern "C" int _PyDict_SetItem_KnownHash(PyObject *mp, PyObject *key, PyObject *item, Py_hash_t hash);
#endif


namespace fsticker::pybridge {

    namespace {

        void invoke(PyObject *callable, PyObject *args) {
            if (!args) {
                PyErr_Print();
                return;
            }
            if (!callable) {
                Py_DECREF(args);
                return;
            }
            PyObject *result = PyObject_CallObject(callable, args);
            if (result) {
                Py_DECREF(result);
            } else {
                PyErr_Print();
            }
            Py_DECREF(args);
        }

        constexpr int kRankCount = 50;

        [[nodiscard]] inline int rank_of(const std::string &k) noexcept {
            switch (k.size()) {
                case 1:
                    switch (k[0]) {
                        case 't':
                            return 0;
                        case 'e':
                            return 1;
                        case 'v':
                            return 8;
                        case 'o':
                            return 13;
                        case 'h':
                            return 14;
                        case 'l':
                            return 15;
                        case 'c':
                            return 16;
                        default:
                            return -1;
                    }
                case 2:
                    switch (k[0]) {
                        case 't':
                            switch (k[1]) {
                                case 'k':
                                    return 2;
                                case 's':
                                    return 3;
                                case 'i':
                                    return 18;
                                default:
                                    return -1;
                            }
                        case 'f':
                            return k[1] == 't' ? 4 : -1;
                        case 'l':
                            return k[1] == 'p' ? 6 : k[1] == 's' ? 19 : -1;
                        case 'p':
                            return k[1] == 'c' ? 7 : k[1] == 'p' ? 17 : -1;
                        case 'a':
                            return k[1] == 'p' ? 9 : -1;
                        case 'o':
                            return k[1] == 'i' ? 10 : -1;
                        default:
                            return -1;
                    }
                case 3: {
                    if (k == "poi")
                        return 11;
                    if (k == "toi")
                        return 12;

                    const char lvlc = k[2];
                    if (lvlc < '1' || lvlc > '5')
                        return -1;
                    int field_idx;
                    switch (k[1]) {
                        case 'p':
                            field_idx = 0;
                            break;
                        case 'q':
                            field_idx = 1;
                            break;
                        case 'o':
                            field_idx = 2;
                            break;
                        default:
                            return -1;
                    }
                    int side_offset;
                    if (k[0] == 'b')
                        side_offset = 0;
                    else if (k[0] == 's')
                        side_offset = 3;
                    else
                        return -1;
                    return 20 + (lvlc - '1') * 6 + side_offset + field_idx;
                }
                case 6:
                    return k == "broker" ? 5 : -1;
                default:
                    return -1;
            }
        }

        struct RankedKey {
            PyObject *py   = nullptr;
            Py_hash_t hash = -1;
        };

        const std::array<RankedKey, kRankCount> &keys_by_rank() {
            static const std::array<RankedKey, kRankCount> table = [] {
                static constexpr const char *kNames[kRankCount] = {
                    "t",   "e",   "tk",  "ts",  "ft",  "broker", "lp",  "pc",  "v",   "ap",
                    "oi",  "poi", "toi", "o",   "h",   "l",      "c",   "pp",  "ti",  "ls",
                    "bp1", "bq1", "bo1", "sp1", "sq1", "so1",    "bp2", "bq2", "bo2", "sp2",
                    "sq2", "so2", "bp3", "bq3", "bo3", "sp3",    "sq3", "so3", "bp4", "bq4",
                    "bo4", "sp4", "sq4", "so4", "bp5", "bq5",    "bo5", "sp5", "sq5", "so5"};
                std::array<RankedKey, kRankCount> t {};
                for (int i = 0; i < kRankCount; ++i) {
                    PyObject *py                   = PyUnicode_InternFromString(kNames[i]);
                    t[static_cast<std::size_t>(i)] = {py, PyObject_Hash(py)};
                }
                return t;
            }();
            return table;
        }

        PyObject *field_to_py(const fsticker::merge::FieldValue &v) {
            if (const auto *s = std::get_if<std::string>(&v))
                return PyUnicode_FromStringAndSize(s->data(), static_cast<Py_ssize_t>(s->size()));
            if (const auto *i = std::get_if<std::int64_t>(&v))
                return PyLong_FromLongLong(*i);
            if (const auto *db = std::get_if<double>(&v))
                return PyFloat_FromDouble(*db);
            Py_RETURN_NONE;
        }

        bool put_known_hash(PyObject *d, const RankedKey &k, const fsticker::merge::FieldValue &v) {
            PyObject *val = field_to_py(v);
            if (!val)
                return false;
#if defined(FSTICKER_HAVE_DICT_SETITEM_KNOWNHASH)
            const int rc = _PyDict_SetItem_KnownHash(d, k.py, val, k.hash);
#else
            const int rc = PyDict_SetItem(d, k.py, val);
#endif
            Py_DECREF(val);
            return rc == 0;
        }

        bool put(PyObject *d, PyObject *key, const fsticker::merge::FieldValue &v) {
            PyObject *val = field_to_py(v);
            if (!val)
                return false;
            const int rc = PyDict_SetItem(d, key, val);
            Py_DECREF(val);
            return rc == 0;
        }

    } // namespace

    inline PyObject *tick_to_pydict(const fsticker::merge::Tick &tick) {
        using Entry       = fsticker::merge::Tick::value_type;
        const auto &ranks = keys_by_rank();

        boost::container::small_vector<const Entry *, kRankCount> ranked(kRankCount, nullptr);
        boost::container::small_vector<const Entry *, 16>         rest;

        for (const auto &kv : tick) {
            const int r = rank_of(kv.first);
            if (r >= 0)
                ranked[static_cast<std::size_t>(r)] = &kv;
            else
                rest.push_back(&kv);
        }

        PyObject *d = _PyDict_NewPresized(kRankCount);
        if (!d)
            return nullptr;

        for (int r = 0; r < kRankCount; ++r) {
            const Entry *e = ranked[static_cast<std::size_t>(r)];
            if (e && !put_known_hash(d, ranks[static_cast<std::size_t>(r)], e->second)) {
                Py_DECREF(d);
                return nullptr;
            }
        }
        for (const Entry *e : rest) {
            PyObject  *key = PyUnicode_FromStringAndSize(e->first.data(),
                                                         static_cast<Py_ssize_t>(e->first.size()));
            const bool ok  = key && put(d, key, e->second);
            Py_XDECREF(key);
            if (!ok) {
                Py_DECREF(d);
                return nullptr;
            }
        }
        return d;
    }

    inline namespace candle_keys_impl {
        struct CandleKeys {
            PyObject *e, *tk, *ts, *period, *time, *o, *h, *l, *c, *v, *oi, *oi_delta, *state,
                *is_new, *current, *previous;
        };

        inline const CandleKeys &candle_keys() {
            static const CandleKeys k = [] {
                CandleKeys c;
                c.e        = PyUnicode_InternFromString("e");
                c.tk       = PyUnicode_InternFromString("tk");
                c.ts       = PyUnicode_InternFromString("ts");
                c.period   = PyUnicode_InternFromString("period");
                c.time     = PyUnicode_InternFromString("time");
                c.o        = PyUnicode_InternFromString("o");
                c.h        = PyUnicode_InternFromString("h");
                c.l        = PyUnicode_InternFromString("l");
                c.c        = PyUnicode_InternFromString("c");
                c.v        = PyUnicode_InternFromString("v");
                c.oi       = PyUnicode_InternFromString("oi");
                c.oi_delta = PyUnicode_InternFromString("oi_delta");
                c.is_new   = PyUnicode_InternFromString("is_new");
                c.current  = PyUnicode_InternFromString("current");
                c.previous = PyUnicode_InternFromString("previous");
                return c;
            }();
            return k;
        }
    } // namespace candle_keys_impl

    inline PyObject *candle_to_pydict(const fsticker::merge::Candle &cd) {
        const auto &k = candle_keys();
        PyObject   *d = PyDict_New();
        if (!d)
            return nullptr;

        auto set = [&](PyObject *key, PyObject *val) {
            if (!val)
                return false;
            const int rc = PyDict_SetItem(d, key, val);
            Py_DECREF(val);
            return rc == 0;
        };

        bool ok =
            set(k.e, PyUnicode_FromStringAndSize(cd.exchange.data(),
                                                 static_cast<Py_ssize_t>(cd.exchange.size()))) &&
            set(k.tk, PyUnicode_FromStringAndSize(cd.token.data(),
                                                  static_cast<Py_ssize_t>(cd.token.size()))) &&
            set(k.ts, PyUnicode_FromStringAndSize(cd.trading_symbol.data(),
                                                  static_cast<Py_ssize_t>(cd.trading_symbol.size()))) &&
            set(k.period, PyLong_FromLongLong(cd.period_seconds)) &&
            set(k.time, PyLong_FromLongLong(cd.period_start)) &&
            set(k.o, PyFloat_FromDouble(cd.open)) && set(k.h, PyFloat_FromDouble(cd.high)) &&
            set(k.l, PyFloat_FromDouble(cd.low)) && set(k.c, PyFloat_FromDouble(cd.close)) &&
            set(k.v, PyLong_FromLongLong(cd.volume)) &&
            set(k.oi, PyLong_FromLongLong(cd.open_interest)) &&
            set(k.oi_delta, PyLong_FromLongLong(cd.oi_delta));

        if (!ok) {
            Py_DECREF(d);
            return nullptr;
        }
        return d;
    }

    inline PyObject *candle_live_event_to_pydict(const std::vector<fsticker::merge::Candle> &msg) {
        const auto &k = candle_keys();
        PyObject   *d = PyDict_New();
        if (!d)
            return nullptr;

        auto set_none = [&](PyObject *key) {
            const int rc = PyDict_SetItem(d, key, Py_None);
            return rc == 0;
        };

        auto set_candle = [&](PyObject *key, const fsticker::merge::Candle &c, bool with_is_new) {
            PyObject *nested = candle_to_pydict(c);
            if (!nested)
                return false;
            if (with_is_new) {
                PyObject *is_new_val = PyBool_FromLong(c.is_new);
                if (!is_new_val || PyDict_SetItem(nested, k.is_new, is_new_val) != 0) {
                    Py_XDECREF(is_new_val);
                    Py_DECREF(nested);
                    return false;
                }
                Py_DECREF(is_new_val);
            }
            const int rc = PyDict_SetItem(d, key, nested);
            Py_DECREF(nested);
            return rc == 0;
        };

        bool ok;
        if (msg.size() == 2) {
            ok = set_candle(k.previous, msg[0], false) && set_candle(k.current, msg[1], true);
        } else if (msg[0].state == fsticker::merge::CandleState::Complete) {

            ok = set_candle(k.previous, msg[0], false) && set_none(k.current);
        } else {
            ok = set_none(k.previous) && set_candle(k.current, msg[0], true);
        }

        if (!ok) {
            Py_DECREF(d);
            return nullptr;
        }
        return d;
    }

    inline PyObject *gap_to_pydict(const fsticker::merge::CandleGap &g) {
        PyObject *d = PyDict_New();
        if (!d)
            return nullptr;

        PyObject *broker =
            PyUnicode_FromStringAndSize(g.broker.data(), static_cast<Py_ssize_t>(g.broker.size()));
        PyObject *tokens  = PyList_New(static_cast<Py_ssize_t>(g.tokens.size()));
        PyObject *periods = PyList_New(static_cast<Py_ssize_t>(g.periods.size()));
        if (!broker || !tokens || !periods)
            goto fail;

        for (std::size_t i = 0; i < g.tokens.size(); ++i) {
            PyObject *s = PyUnicode_FromStringAndSize(g.tokens[i].data(),
                                                      static_cast<Py_ssize_t>(g.tokens[i].size()));
            if (!s)
                goto fail;
            PyList_SET_ITEM(tokens, static_cast<Py_ssize_t>(i), s);
        }
        for (std::size_t i = 0; i < g.periods.size(); ++i) {
            PyObject *e =
                Py_BuildValue("{s:L,s:L}", "period", static_cast<long long>(g.periods[i].first),
                              "time", static_cast<long long>(g.periods[i].second));
            if (!e)
                goto fail;
            PyList_SET_ITEM(periods, static_cast<Py_ssize_t>(i), e);
        }
        if (PyDict_SetItemString(d, "broker", broker) != 0 ||
            PyDict_SetItemString(d, "tokens", tokens) != 0 ||
            PyDict_SetItemString(d, "periods", periods) != 0)
            goto fail;

        Py_DECREF(broker);
        Py_DECREF(tokens);
        Py_DECREF(periods);
        return d;

    fail:
        Py_XDECREF(broker);
        Py_XDECREF(tokens);
        Py_XDECREF(periods);
        Py_DECREF(d);
        return nullptr;
    }

    struct PyBrokerSpec {
        std::string name;
        std::string ws_endpoint;
        std::string user_id;
        std::string token;
        std::string access_type;
        bool        verify_ssl            = false;
        bool        enable_ip_pinning     = false;
        long long   idle_ping_timeout_ms  = 3000;
        long long   ping_reply_timeout_ms = 2000;
        std::string ping_payload          = R"({"t":"h"})";
        int         min_log_level         = 1;
    };

    struct PyExchangeAnchor {
        std::string exchange;
        long long   anchor_seconds = 0;
    };

    struct PyTimeframeSpec {
        long long seconds                     = 60;
        bool      live                        = false;
        bool      auto_finalize               = false;
        long long auto_finalize_grace_seconds = 4;
    };

    struct PyTimescaleParams {
        std::string    host;
        unsigned short port = 5432;
        std::string    dbname;
        std::string    user;
        std::string    password;
        std::string    table                 = "candles";
        int            table_mode            = 0;
        long long      connect_timeout_ms    = 5000;
        std::size_t    queue_capacity        = 50000;
        bool           queue_overwrite       = true;
        bool           auto_bootstrap_schema = true;
    };

    struct PyCallableSlot {
        PyObject *callable = nullptr;

        void set(PyObject *obj) {
            PyGILState_STATE gstate = PyGILState_Ensure();
            Py_XINCREF(obj);
            PyObject *old = callable;
            callable      = obj;
            Py_XDECREF(old);
            PyGILState_Release(gstate);
        }

        ~PyCallableSlot() {
            if (!callable)
                return;
            PyGILState_STATE gstate = PyGILState_Ensure();
            Py_XDECREF(callable);
            PyGILState_Release(gstate);
        }
    };

    class PyMergedFeedBridge {
    public:
        explicit PyMergedFeedBridge(std::vector<PyBrokerSpec> specs) {
            std::vector<fsticker::merge::BrokerSpec> cpp_specs;
            cpp_specs.reserve(specs.size());
            for (const auto &s : specs) {
                fsticker::Credentials creds;
                creds.ws_endpoint       = s.ws_endpoint;
                creds.user_id           = s.user_id;
                creds.token             = s.token;
                creds.access_type       = fsticker::access_type_from_string(s.access_type);
                creds.verify_ssl        = s.verify_ssl;
                creds.enable_ip_pinning = s.enable_ip_pinning;
                fsticker::Ticker::Params tp;
                tp.idle_ping_timeout  = std::chrono::milliseconds(s.idle_ping_timeout_ms);
                tp.ping_reply_timeout = std::chrono::milliseconds(s.ping_reply_timeout_ms);
                if (!s.ping_payload.empty())
                    tp.ping_payload = s.ping_payload;

                tp.min_log_level = static_cast<fsticker::LogLevel>(s.min_log_level);
                cpp_specs.push_back(fsticker::merge::BrokerSpec {s.name, creds, tp});
            }
            feed_ = std::make_shared<fsticker::merge::MergedFeed>(control_ioc_, std::move(cpp_specs));
            feed_->set_wake_callback([this] { notify_waiters(); });

            feed_->set_tick_callback([this](const fsticker::merge::Tick &tick) {
                if (async_ticks_) {
                    async_ticks_->push(tick);
                    return;
                }
                PyGILState_STATE g = PyGILState_Ensure();
                if (tick_cb_.callable) {
                    PyObject *args = PyTuple_New(1);
                    if (args) {
                        PyObject *d = tick_to_pydict(tick);
                        if (d) {
                            PyTuple_SET_ITEM(args, 0, d);
                            invoke(tick_cb_.callable, args);
                        } else {
                            Py_DECREF(args);
                            PyErr_Print();
                        }
                    } else {
                        PyErr_Print();
                    }
                }
                PyGILState_Release(g);
            });

            feed_->set_order_callback([this](const std::string &broker, const char *data,
                                             std::size_t size) {
                PyGILState_STATE g = PyGILState_Ensure();
                if (order_cb_.callable) {
                    invoke(order_cb_.callable, Py_BuildValue("(s#s#)", broker.c_str(),
                                                             static_cast<Py_ssize_t>(broker.size()),
                                                             data, static_cast<Py_ssize_t>(size)));
                }
                PyGILState_Release(g);
            });

            feed_->set_error_callback([this](const std::string &broker, const char *data,
                                             std::size_t size) {
                PyGILState_STATE g = PyGILState_Ensure();
                if (error_cb_.callable) {
                    invoke(error_cb_.callable, Py_BuildValue("(s#s#)", broker.c_str(),
                                                             static_cast<Py_ssize_t>(broker.size()),
                                                             data, static_cast<Py_ssize_t>(size)));
                }
                PyGILState_Release(g);
            });

            feed_->set_open_callback([this](const std::string &broker, const char *data,
                                            std::size_t size) {
                PyGILState_STATE g = PyGILState_Ensure();
                if (open_cb_.callable) {
                    invoke(open_cb_.callable, Py_BuildValue("(s#s#)", broker.c_str(),
                                                            static_cast<Py_ssize_t>(broker.size()),
                                                            data, static_cast<Py_ssize_t>(size)));
                }
                PyGILState_Release(g);
            });

            feed_->set_close_callback([this](const std::string &broker) {
                PyGILState_STATE g = PyGILState_Ensure();
                if (close_cb_.callable) {
                    invoke(close_cb_.callable, Py_BuildValue("(s#)", broker.c_str(),
                                                             static_cast<Py_ssize_t>(broker.size())));
                }
                PyGILState_Release(g);
            });

            feed_->set_stalled_callback([this](const std::string &broker, std::uint32_t n) {
                PyGILState_STATE g = PyGILState_Ensure();
                if (stalled_cb_.callable) {
                    invoke(stalled_cb_.callable, Py_BuildValue("(s#I)", broker.c_str(),
                                                               static_cast<Py_ssize_t>(broker.size()),
                                                               static_cast<unsigned int>(n)));
                }
                PyGILState_Release(g);
            });

            feed_->set_shutdown_callback([this] {
                PyGILState_STATE g = PyGILState_Ensure();
                if (shutdown_cb_.callable) {
                    invoke(shutdown_cb_.callable, PyTuple_New(0));
                }
                PyGILState_Release(g);
                closed_flag_.store(true, std::memory_order_release);
                notify_waiters();
            });

            feed_->set_log_callback([this](const std::string &broker, int level, std::string_view msg) {
                if (log_cb_.callable) {
                    PyGILState_STATE g = PyGILState_Ensure();
                    invoke(log_cb_.callable,
                           Py_BuildValue("(s#is#)", broker.c_str(),
                                         static_cast<Py_ssize_t>(broker.size()), level, msg.data(),
                                         static_cast<Py_ssize_t>(msg.size())));
                    PyGILState_Release(g);
                    return;
                }
                fsticker::emit_log(level, fsticker::kDebug, fsticker::LogCallback {},
                                   broker + ": " + std::string(msg));
            });


            feed_->set_candle_gap_callback([this](const fsticker::merge::CandleGap &g) {
                PyGILState_STATE gs = PyGILState_Ensure();
                if (gap_cb_.callable) {
                    PyObject *d = gap_to_pydict(g);
                    if (d) {
                        PyObject *args = PyTuple_New(1);
                        if (args) {
                            PyTuple_SET_ITEM(args, 0, d);
                            invoke(gap_cb_.callable, args);
                        } else {
                            Py_DECREF(d);
                            PyErr_Print();
                        }
                    } else {
                        PyErr_Print();
                    }
                }
                PyGILState_Release(gs);
            });
        }

        ~PyMergedFeedBridge() {
            if (!feed_)
                return;
            feed_->stop();
            if (!feed_->join_all())
                std::terminate(); // <--- possible leak --->
            feed_->release_callbacks();
            feed_.reset();
        }

        void set_tick_callback(PyObject *callable) {
            tick_cb_.set(callable);
        }

        void set_order_callback(PyObject *callable) {
            order_cb_.set(callable);
        }

        void set_error_callback(PyObject *callable) {
            error_cb_.set(callable);
        }

        void set_open_callback(PyObject *callable) {
            open_cb_.set(callable);
        }

        void set_close_callback(PyObject *callable) {
            close_cb_.set(callable);
        }

        void set_stalled_callback(PyObject *callable) {
            stalled_cb_.set(callable);
        }

        void set_shutdown_callback(PyObject *callable) {
            shutdown_cb_.set(callable);
        }

        void set_log_callback(PyObject *callable) {
            log_cb_.set(callable);
        }

        void set_candle_callback(PyObject *callable) {
            candle_cb_.set(callable);
        }

        void set_candle_gap_callback(PyObject *callable) {
            gap_cb_.set(callable);
        }

        void configure_candles(std::vector<PyTimeframeSpec>  specs,
                               std::vector<PyExchangeAnchor> anchors = {}) {
            std::vector<fsticker::merge::TimeframeSpec> cpp_specs;
            cpp_specs.reserve(specs.size());
            for (const auto &s : specs)
                cpp_specs.push_back(fsticker::merge::TimeframeSpec {
                    std::chrono::seconds(s.seconds), s.live, s.auto_finalize,
                    std::chrono::seconds(s.auto_finalize_grace_seconds)});

            fsticker::merge::ExchangeAnchors cpp_anchors;
            for (const auto &a : anchors)
                cpp_anchors[a.exchange] = a.anchor_seconds;


            feed_->set_candle_callback(
                std::move(cpp_specs),
                [this](const std::vector<fsticker::merge::Candle> &batch) {
                    if (batch.empty())
                        return;
                    if (async_candles_) {

                        async_candles_->push(batch);
                        return;
                    }
                    PyGILState_STATE g = PyGILState_Ensure();
                    if (candle_cb_.callable) {

                        PyObject *payload = (batch.size() == 1 && !batch[0].live)
                                                ? candle_to_pydict(batch[0])
                                                : candle_live_event_to_pydict(batch);
                        if (payload) {
                            PyObject *args = PyTuple_New(1);
                            if (args) {
                                PyTuple_SET_ITEM(args, 0, payload);
                                invoke(candle_cb_.callable, args);
                            } else {
                                Py_DECREF(payload);
                                PyErr_Print();
                            }
                        } else {
                            PyErr_Print();
                        }
                    }
                    PyGILState_Release(g);
                },
                std::move(cpp_anchors));
        }

        void configure_timescale(PyTimescaleParams p) {
            fsticker::merge::TimescaleParams cpp;
            cpp.host            = p.host;
            cpp.port            = p.port;
            cpp.dbname          = p.dbname;
            cpp.user            = p.user;
            cpp.password        = p.password;
            cpp.table           = p.table;
            cpp.table_mode      = p.table_mode == 1
                                      ? fsticker::merge::TimescaleParams::TableMode::PerTimeframe
                                      : fsticker::merge::TimescaleParams::TableMode::Single;
            cpp.connect_timeout = std::chrono::milliseconds(p.connect_timeout_ms);
            cpp.queue_capacity  = p.queue_capacity;
            cpp.queue_overwrite = p.queue_overwrite;
            cpp.auto_bootstrap_schema = p.auto_bootstrap_schema;
            feed_->configure_timescale(std::move(cpp));
        }

        void enable_async_ticks(int notify_fd) {
            async_ticks_ =
                std::make_unique<AsyncTickBridge>(notify_fd, tick_capacity_, tick_overwrite_);
        }

        std::size_t dropped_ticks() const {
            return async_ticks_ ? async_ticks_->dropped() : 0;
        }

        void begin_ticks_wait() {
            if (async_ticks_)
                async_ticks_->begin_wait();
        }

        void end_ticks_wait() {
            if (async_ticks_)
                async_ticks_->end_wait();
        }

        void enable_async_candles(int notify_fd) {
            async_candles_ =
                std::make_unique<AsyncCandleBridge>(notify_fd, candle_capacity_, candle_overwrite_);
        }

        std::size_t dropped_candles() const {
            return async_candles_ ? async_candles_->dropped() : 0;
        }

        void set_tick_queue_limits(std::size_t capacity, bool overwrite) {
            tick_capacity_  = capacity;
            tick_overwrite_ = overwrite;
        }

        void set_candle_queue_limits(std::size_t capacity, bool overwrite) {
            candle_capacity_  = capacity;
            candle_overwrite_ = overwrite;
        }

        PyObject *pop_tick_as_pydict() {
            if (!async_ticks_)
                Py_RETURN_NONE;
            auto item = async_ticks_->try_pop();
            if (!item)
                Py_RETURN_NONE;
            return tick_to_pydict(*item);
        }

        PyObject *pop_candle_as_py() {
            if (!async_candles_)
                Py_RETURN_NONE;
            auto msg = async_candles_->try_pop();
            if (!msg)
                Py_RETURN_NONE;
            if (msg->size() == 1 && !(*msg)[0].live)
                return candle_to_pydict((*msg)[0]);
            return candle_live_event_to_pydict(*msg);
        }

        void begin_candles_wait() {
            if (async_candles_)
                async_candles_->begin_wait();
        }

        void end_candles_wait() {
            if (async_candles_)
                async_candles_->end_wait();
        }

        void start() {
            feed_->start();
        }

        void stop() {
            feed_->stop();
        }

        [[nodiscard]] bool close() noexcept {
            feed_->stop();
            const bool ok = feed_->join_all();
            if (!ok)
                std::fputs("[fsticker][ERROR] a worker is stuck in a callback; leaking the feed\n",
                           stderr);
            return ok;
        }

        [[nodiscard]] bool join_all() noexcept {
            return feed_->join_all();
        }

        int run(bool handle_signals, int grace_ms) noexcept {
            try {
                using clock = std::chrono::steady_clock;
                Waiting     waiting(*this);
                SignalScope scope(handle_signals ? SignalMode::Own : SignalMode::None);
                std::optional<clock::time_point> deadline;
                if (handle_signals && !scope.active())
                    return -3;
                for (;;) {
                    if (closed_flag_.load(std::memory_order_acquire))
                        break;
                    if (scope.active() && !deadline && SignalScope::caught()) {
                        feed_->stop();
                        deadline = clock::now() + std::chrono::milliseconds(grace_ms);
                    }
                    int timeout_ms = -1;
                    if (deadline) {
                        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
                                              *deadline - clock::now())
                                              .count();
                        if (left <= 0)
                            break;
                        timeout_ms = static_cast<int>(std::min<long long>(left, INT_MAX));
                    }
                    waiting.waker().wait(timeout_ms, scope.active());
                }
                if (!closed_flag_.load(std::memory_order_acquire))
                    feed_->stop();
                (void) feed_->join_all();
                const int sig = scope.active() ? SignalScope::caught() : 0;
                return sig;
            } catch (...) {
                return -4;
            }
        }

        int run_passive(bool main_thread) {
            try {
                Waiting waiting(*this);
                for (;;) {
                    if (closed_flag_.load(std::memory_order_acquire))
                        return 1;
                    if (wait_event(waiting.waker(), -1, main_thread) != 0)
                        return -2;
                }
            } catch (const std::exception &e) {
                PyErr_SetString(PyExc_RuntimeError, e.what());
                return -2;
            }
        }

        void reraise(int sig) {
            std::raise(sig);
        }

        void subscribe(std::vector<std::string> instruments,
                       fsticker::FeedType       feed_type,
                       const std::string       &target) {
            feed_->subscribe(std::move(instruments), feed_type, target);
        }

        void unsubscribe(std::vector<std::string> instruments,
                         fsticker::FeedType       feed_type,
                         const std::string       &target) {
            feed_->unsubscribe(std::move(instruments), feed_type, target);
        }

        int wait_connected(long long timeout_ms, bool main_thread) {
            try {
                using clock         = std::chrono::steady_clock;
                using State         = fsticker::merge::MergedFeed::ConnectState;
                const bool bounded  = timeout_ms >= 0;
                const auto deadline = bounded ? clock::now() + std::chrono::milliseconds(timeout_ms)
                                              : clock::time_point::max();
                Waiting    waiting(*this);
                for (;;) {
                    switch (feed_->connect_state()) {
                        case State::Connected:
                            return 1;
                        case State::Closed:
                            return -1;
                        case State::Pending:
                            break;
                    }
                    int wait_ms = -1;
                    if (bounded) {
                        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
                                              deadline - clock::now())
                                              .count();
                        if (left <= 0)
                            return 0;
                        wait_ms = static_cast<int>(std::min<long long>(left, INT_MAX));
                    }
                    if (wait_event(waiting.waker(), wait_ms, main_thread) != 0)
                        return -2;
                }
            } catch (const std::exception &e) {
                PyErr_SetString(PyExc_RuntimeError, e.what());
                return -2;
            }
        }

        std::size_t broker_count() const {
            return feed_->broker_count();
        }

    private:
        class Waiting {
        public:
            explicit Waiting(PyMergedFeedBridge &owner) : owner_(owner) {
                std::lock_guard<std::mutex> lock(owner_.waiters_mutex_);
                owner_.waiters_.push_back(&waker_);
            }

            ~Waiting() {
                std::lock_guard<std::mutex> lock(owner_.waiters_mutex_);
                owner_.waiters_.erase(
                    std::find(owner_.waiters_.begin(), owner_.waiters_.end(), &waker_));
            }

            Waiting(const Waiting &)            = delete;
            Waiting &operator=(const Waiting &) = delete;

            [[nodiscard]] fsticker::Waker &waker() noexcept {
                return waker_;
            }

        private:
            PyMergedFeedBridge &owner_;
            fsticker::Waker     waker_;
        };

        void notify_waiters() noexcept {
            std::lock_guard<std::mutex> lock(waiters_mutex_);
            for (auto *w : waiters_)
                w->notify();
        }

        static int wait_event(fsticker::Waker &waker, int timeout_ms, bool main_thread) {
            int rc = 0;
            if (main_thread) {
                fsticker::SignalScope scope(fsticker::SignalMode::Chain);
                rc = PyErr_CheckSignals();
                if (rc == 0) {
                    PyThreadState *ts = PyEval_SaveThread();
                    waker.wait(timeout_ms, scope.active());
                    PyEval_RestoreThread(ts);
                }
            } else {
                PyThreadState *ts = PyEval_SaveThread();
                waker.wait(timeout_ms, false);
                PyEval_RestoreThread(ts);
            }

            return rc != 0 ? -1 : PyErr_CheckSignals();
        }

        boost::asio::io_context                      control_ioc_;
        std::shared_ptr<fsticker::merge::MergedFeed> feed_;

        PyCallableSlot tick_cb_;
        PyCallableSlot order_cb_;
        PyCallableSlot error_cb_;
        PyCallableSlot open_cb_;
        PyCallableSlot close_cb_;
        PyCallableSlot stalled_cb_;
        PyCallableSlot shutdown_cb_;
        PyCallableSlot candle_cb_;
        PyCallableSlot log_cb_;
        PyCallableSlot gap_cb_;

        std::function<void(const fsticker::merge::Tick &)> tick_dispatch_ =
            [](const fsticker::merge::Tick &) {};
        std::function<void(const std::vector<fsticker::merge::Candle> &)> candle_dispatch_ =
            [](const std::vector<fsticker::merge::Candle> &) {};


        std::unique_ptr<AsyncTickBridge>   async_ticks_;
        std::unique_ptr<AsyncCandleBridge> async_candles_;
        std::size_t                        tick_capacity_    = 100000;
        bool                               tick_overwrite_   = true;
        std::size_t                        candle_capacity_  = 100000;
        bool                               candle_overwrite_ = true;
        std::atomic<bool>                  closed_flag_ {false};
        std::mutex                         waiters_mutex_;
        std::vector<fsticker::Waker *>     waiters_;
    };

} // namespace fsticker::pybridge
