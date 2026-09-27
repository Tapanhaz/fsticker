// Copyright (c) 2026 Tapanhaz
// Part of fsticker :: https://github.com/Tapanhaz/fsticker
// Licensed under the Apache License, Version 2.0
// Created: 2026-09-25 10:41 IST

#pragma once

#include "backoff.hpp"
#include "merge/candle_engine.hpp"
#include "types.hpp"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <deque>
#include <libpq-fe.h>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>

namespace fsticker::merge {

    struct TimescaleParams {
        std::string   host;
        std::uint16_t port = 5432;
        std::string   dbname;
        std::string   user;
        std::string   password;

        enum class TableMode { Single, PerTimeframe };
        TableMode table_mode = TableMode::Single;

        std::string table = "candles";

        std::chrono::milliseconds     connect_timeout {5000};
        std::size_t                   queue_capacity  = 50000;
        bool                          queue_overwrite = true;
        std::chrono::duration<double> min_backoff {1.0};
        std::chrono::duration<double> max_backoff {30.0};
        bool                          auto_bootstrap_schema = true;
        LogLevel                      min_log_level         = kInfo;
    };

    [[nodiscard]] static std::int64_t chunk_interval_for(std::int64_t period_seconds) {
        constexpr std::int64_t target_bars_per_chunk = 20000;
        constexpr std::int64_t min_interval          = 86400;
        constexpr std::int64_t max_interval          = 31536000L;
        const std::int64_t     interval              = period_seconds * target_bars_per_chunk;
        return std::clamp(interval, min_interval, max_interval);
    }

    class TimescaleSink {
    public:
        explicit TimescaleSink(
            TimescaleParams       params,
            fsticker::LogCallback on_log = nullptr) : params_(std::move(params)),
                                                      on_log_(std::move(on_log)),
                                                      backoff_(fsticker::Backoff::Params {
                                                          params_.min_backoff, params_.max_backoff,
                                                          2.0, 0.3}) {
            worker_ = std::thread([this] { run(); });
        }

        ~TimescaleSink() {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                stop_ = true;
            }
            cv_.notify_all();
            if (worker_.joinable())
                worker_.join();
        }

        TimescaleSink(const TimescaleSink &)            = delete;
        TimescaleSink &operator=(const TimescaleSink &) = delete;

        void on_candle(const std::vector<Candle> &batch) {
            if (unavailable_.load(std::memory_order_relaxed))
                return;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                for (const Candle &c : batch) {
                    if (c.state != CandleState::Complete)
                        continue;
                    QueueItem item {c, table_for(c.period_seconds)};
                    if (queue_.size() >= params_.queue_capacity) {
                        if (!params_.queue_overwrite)
                            continue;
                        queue_.pop_front();
                        ++dropped_;
                    }
                    queue_.push_back(std::move(item));
                }
            }
            cv_.notify_one();
        }

        [[nodiscard]] bool unavailable() const noexcept {
            return unavailable_.load(std::memory_order_relaxed);
        }

        [[nodiscard]] std::size_t dropped() const noexcept {
            return dropped_.load(std::memory_order_relaxed);
        }

    private:
        void log(int level, std::string_view msg) const {
            emit_log(level, params_.min_log_level, on_log_, msg);
        }

        struct PGconnDeleter {
            void operator()(PGconn *c) const noexcept {
                if (c)
                    PQfinish(c);
            }
        };

        struct PGresultDeleter {
            void operator()(PGresult *r) const noexcept {
                if (r)
                    PQclear(r);
            }
        };

        using PGconnPtr   = std::unique_ptr<PGconn, PGconnDeleter>;
        using PGresultPtr = std::unique_ptr<PGresult, PGresultDeleter>;

        struct QueueItem {
            Candle      candle;
            std::string table;
        };

        [[nodiscard]] std::string table_for(std::int64_t period_seconds) const {
            if (params_.table_mode == TimescaleParams::TableMode::Single)
                return params_.table;
            return params_.table + "_" + std::to_string(period_seconds);
        }

        void run() {
            for (;;) {
                if (!connect())
                    return;
                drain_until_disconnected();
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stop_)
                        return;
                }
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait_for(
                    lock,
                    std::chrono::duration_cast<std::chrono::milliseconds>(backoff_.next_delay()),
                    [this] { return stop_; });
                if (stop_)
                    return;
            }
        }

        [[nodiscard]] bool connect() {
            std::ostringstream conninfo;
            conninfo << "host=" << params_.host << " port=" << params_.port
                     << " dbname=" << params_.dbname << " user=" << params_.user
                     << " password=" << params_.password
                     << " connect_timeout=" << (params_.connect_timeout.count() / 1000);
            conn_.reset(PQconnectdb(conninfo.str().c_str()));

            if (!conn_ || PQstatus(conn_.get()) != CONNECTION_OK) {
                log(kWarn,
                    std::string("TimescaleDB sink: cannot connect (") +
                        (conn_ ? PQerrorMessage(conn_.get()) : "PQconnectdb returned null") +
                        ") -- candle persistence disabled; everything else continues normally.");
                if (first_attempt_) {
                    unavailable_.store(true, std::memory_order_relaxed);
                    return false;
                }
                return true;
            }
            first_attempt_ = false;
            backoff_.reset();

            bootstrapped_.clear();
            if (params_.auto_bootstrap_schema &&
                params_.table_mode == TimescaleParams::TableMode::Single)
                ensure_table_bootstrapped(params_.table);
            return true;
        }

        void ensure_table_bootstrapped(const std::string &table, std::int64_t period_seconds = 0) {
            if (!params_.auto_bootstrap_schema || bootstrapped_.count(table))
                return;
            bootstrapped_.insert(table);

            const std::string create_table =
                "CREATE TABLE IF NOT EXISTS " + table +
                " ("
                "  exchange        TEXT        NOT NULL,"
                "  token           TEXT        NOT NULL,"
                "  trading_symbol  TEXT,"
                "  period          BIGINT      NOT NULL,"
                "  \"time\"        BIGINT      NOT NULL,"
                "  ts              TIMESTAMP   GENERATED ALWAYS AS "
                "                  (to_timestamp(\"time\") AT TIME ZONE 'Asia/Kolkata') STORED,"
                "  open            DOUBLE PRECISION,"
                "  high            DOUBLE PRECISION,"
                "  low             DOUBLE PRECISION,"
                "  close           DOUBLE PRECISION,"
                "  volume          BIGINT,"
                "  open_interest   BIGINT,"
                "  oi_delta        BIGINT,"
                "  PRIMARY KEY (exchange, token, period, \"time\")"
                ")";

            if (!exec_ignore_errors(create_table))
                return;

            if (!exec_ignore_errors("CREATE EXTENSION IF NOT EXISTS timescaledb")) {
                log(kInfo, "TimescaleDB extension unavailable (missing privilege, or not installed "
                           "on the server) -- '" +
                               table +
                               "' will be a regular Postgres table, not a hypertable. Everything "
                               "still works.");
                return;
            }

            if (!exec_ignore_errors(
                    "CREATE OR REPLACE FUNCTION fsticker_sec_now() RETURNS BIGINT "
                    "LANGUAGE SQL STABLE AS $$ SELECT EXTRACT(EPOCH FROM now())::BIGINT $$")) {
                log(kInfo, "could not create fsticker_sec_now() -- '" + table +
                               "' will be a regular Postgres table, not a hypertable.");
                return;
            }

            const std::int64_t chunk_interval  = chunk_interval_for(period_seconds);
            const std::string  make_hypertable = "SELECT create_hypertable('" + table +
                                                 "', by_range('time', " +
                                                 std::to_string(chunk_interval) +
                                                 "::BIGINT), "
                                                 "if_not_exists => TRUE, migrate_data => TRUE)";
            if (!exec_ignore_errors(make_hypertable)) {
                log(kWarn, "create_hypertable() on '" + table +
                               "' failed -- continuing as a regular Postgres table. This is "
                               "unexpected with fsticker's own schema; if you're using "
                               "auto_bootstrap_schema=False with a hand-written table, make sure "
                               "its primary key/unique index includes the partitioning column "
                               "('time').");
                return;
            }

            if (!exec_ignore_errors("SELECT set_integer_now_func('" + table +
                                    "', 'fsticker_sec_now')"))
                log(kInfo,
                    "set_integer_now_func() on '" + table +
                        "' failed -- hypertable created, but time_bucket_gapfill/retention "
                        "policies relying on \"now\" may not work correctly for this table.");
        }

        [[nodiscard]] bool exec_ignore_errors(const std::string &sql) {
            PGresultPtr res(PQexec(conn_.get(), sql.c_str()));
            const auto  status = res ? PQresultStatus(res.get()) : PGRES_FATAL_ERROR;
            return status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK;
        }

        void drain_until_disconnected() {
            for (;;) {
                QueueItem item;
                {
                    std::unique_lock<std::mutex> lock(mutex_);
                    cv_.wait(lock, [this] { return stop_ || !queue_.empty(); });
                    if (stop_)
                        return;
                    item = queue_.front();
                    queue_.pop_front();
                }

                if (params_.table_mode == TimescaleParams::TableMode::PerTimeframe)
                    ensure_table_bootstrapped(item.table, item.candle.period_seconds);
                if (!insert(item.table, item.candle))
                    return;
            }
        }

        [[nodiscard]] bool insert(const std::string &table, const Candle &c) {

            std::string full_sql = "INSERT INTO " + table +
                                   " (exchange, token, trading_symbol, period, \"time\", "
                                   "open, high, low, close, volume, open_interest, oi_delta) "
                                   "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) "
                                   "ON CONFLICT (exchange, token, period, \"time\") DO NOTHING";

            const std::string ps  = std::to_string(c.period_seconds),
                              pst = std::to_string(c.period_start), o = std::to_string(c.open),
                              h = std::to_string(c.high), l = std::to_string(c.low),
                              cl = std::to_string(c.close), v = std::to_string(c.volume),
                              oi = std::to_string(c.open_interest), oid = std::to_string(c.oi_delta);
            const char *vals[12] = {c.exchange.c_str(), c.token.c_str(), c.trading_symbol.c_str(),
                                    ps.c_str(),         pst.c_str(),     o.c_str(),
                                    h.c_str(),          l.c_str(),       cl.c_str(),
                                    v.c_str(),          oi.c_str(),      oid.c_str()};

            PGresultPtr res(PQexecParams(conn_.get(), full_sql.c_str(), 12, nullptr, vals, nullptr,
                                         nullptr, 0));
            if (res && PQresultStatus(res.get()) == PGRES_COMMAND_OK)
                return true;

            const bool connection_lost = PQstatus(conn_.get()) != CONNECTION_OK;
            log(kWarn,
                "TimescaleDB insert into '" + table + "' failed (" +
                    (res ? PQresultErrorMessage(res.get())
                         : (conn_ ? PQerrorMessage(conn_.get()) : "no connection")) +
                    ")" +
                    (connection_lost ? " -- connection lost, reconnecting."
                                     : " -- dropping this candle, connection still healthy."));
            return !connection_lost;
        }

        TimescaleParams          params_;
        fsticker::LogCallback    on_log_;
        fsticker::Backoff        backoff_;
        PGconnPtr                conn_;
        std::thread              worker_;
        std::mutex               mutex_;
        std::condition_variable  cv_;
        std::deque<QueueItem>    queue_;
        bool                     stop_          = false;
        bool                     first_attempt_ = true;
        std::set<std::string>    bootstrapped_;
        std::atomic<bool>        unavailable_ {false};
        std::atomic<std::size_t> dropped_ {0};
    };

} // namespace fsticker::merge