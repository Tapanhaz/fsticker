// Copyright (c) 2026 Tapanhaz
// Part of fsticker :: https://github.com/Tapanhaz/fsticker
// Licensed under the Apache License, Version 2.0
// Created: 2026-09-25 10:41 IST


#pragma once

#include "merge/candle_engine.hpp"
#include "merge/feed_store.hpp"
#include "merge/reconstructor.hpp"
#include "merge/tick.hpp"
#include "merge/timescale_sink.hpp"
#include "ticker.hpp"

#include <atomic>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace fsticker::merge {

    namespace net = boost::asio;

    struct BrokerSpec {
        std::string              name;
        fsticker::Credentials    credentials;
        fsticker::Ticker::Params params {};
    };

    struct CandleGap {
        std::string                                        broker;
        std::vector<std::string>                           tokens;
        std::vector<std::pair<std::int64_t, std::int64_t>> periods;
    };

    struct CandleGapReport {
        struct PeriodReport {
            std::int64_t              period;
            std::vector<std::int64_t> partial;
            bool                      has_range = false;
            std::int64_t              start     = 0;
            std::int64_t              end       = 0;
        };

        std::string               token;
        std::string               broker;
        std::vector<PeriodReport> periods;
    };

    class MergedFeed : public std::enable_shared_from_this<MergedFeed> {
    public:
        using TickCallback   = std::function<void(const Tick &)>;
        using CandleCallback = std::function<void(const std::vector<Candle> &)>;
        using OrderCallback =
            std::function<void(const std::string &broker, const char *data, std::size_t size)>;
        using ErrorCallback =
            std::function<void(const std::string &broker, const char *data, std::size_t size)>;
        using OpenCallback =
            std::function<void(const std::string &broker, const char *data, std::size_t size)>;
        using CloseCallback = std::function<void(const std::string &broker)>;
        using StalledCallback =
            std::function<void(const std::string &broker, std::uint32_t consecutive_failures)>;
        using ShutdownCallback = std::function<void()>;
        using LogCallback =
            std::function<void(const std::string &broker, int level, std::string_view message)>;
        using CandleGapCallback       = std::function<void(const CandleGap &)>;
        using CandleGapReportCallback = std::function<void(const CandleGapReport &)>;

        MergedFeed(net::io_context        &control_ioc,
                   std::vector<BrokerSpec> brokers) : specs_(std::move(brokers)),
                                                      store_(specs_.size() == 1),
                                                      signals_(control_ioc),
                                                      signal_grace_timer_(control_ioc) {
            if (specs_.empty())
                throw std::invalid_argument(
                    "fsticker::MergedFeed: at least one broker is required");

            std::unordered_set<std::string> seen;
            for (const auto &s : specs_) {
                if (s.name.empty() || !seen.insert(s.name).second)
                    throw std::invalid_argument(
                        "fsticker::MergedFeed: broker names must be non-empty and unique (got '" +
                        s.name + "')");
            }
        }

        ~MergedFeed() {
            stop();
            if (!join_all()) {
                // Scenario :: worker stuck in user - space :: fail loudly instead of corrupting memory.
                std::fputs("[fsticker][FATAL] MergedFeed destroyed while a worker is stuck in a "
                           "callback\n",
                           stderr);
                std::terminate();
            }
        }

        void set_tick_callback(TickCallback cb) {
            on_tick_ = std::move(cb);
        }

        void set_candle_callback(std::vector<TimeframeSpec> specs,
                                 CandleCallback             cb,
                                 ExchangeAnchors            anchors = {}) {
            if (specs.empty() || (!cb && !timescale_)) {
                std::lock_guard<std::mutex> lock(candle_mutex_);
                candle_.reset();
                candle_needs_timer_ = false;
                return;
            }
            const bool needs_timer = std::any_of(
                specs.begin(), specs.end(), [](const TimeframeSpec &s) { return s.auto_finalize; });

            {
                std::lock_guard<std::mutex> lk(track_mutex_);
                gap_periods_.clear();
                for (const auto &s : specs)
                    gap_periods_.push_back(s.period.count());
                gap_anchors_ = anchors;
            }

            CandleCallback dispatch = [this, cb = std::move(cb)](const std::vector<Candle> &batch) {
                if (timescale_)
                    timescale_->on_candle(batch);
                if (cb)
                    cb(batch);
            };
            {
                std::lock_guard<std::mutex> lock(candle_mutex_);
                candle_.emplace(std::move(specs), std::move(dispatch), std::move(anchors));
                candle_needs_timer_ = needs_timer;
            }
            kick_candle_thread();
            if (needs_timer && started_.load(std::memory_order_acquire))
                ensure_candle_thread();
        }

        void configure_timescale(TimescaleParams params) {
            std::lock_guard<std::mutex> lock(candle_mutex_);
            timescale_ = std::make_unique<TimescaleSink>(std::move(params),
                                                         [this](int level, std::string_view msg) {
                                                             if (on_log_)
                                                                 on_log_("timescale", level, msg);
                                                         });
        }

        void set_order_callback(OrderCallback cb) {
            on_order_ = std::move(cb);
        }

        void set_error_callback(ErrorCallback cb) {
            on_error_ = std::move(cb);
        }

        void set_open_callback(OpenCallback cb) {
            on_open_ = std::move(cb);
        }

        void set_close_callback(CloseCallback cb) {
            on_close_ = std::move(cb);
        }

        void set_stalled_callback(StalledCallback cb) {
            on_stalled_ = std::move(cb);
        }

        void set_shutdown_callback(ShutdownCallback cb) {
            on_shutdown_ = std::move(cb);
        }

        void set_log_callback(LogCallback cb) {
            on_log_ = std::move(cb);
        }

        void set_candle_gap_callback(CandleGapCallback cb) {
            on_gap_ = std::move(cb);
        }

        void set_candle_gap_report_callback(CandleGapReportCallback cb) {
            on_gap_report_ = std::move(cb);
        }

        void set_wake_callback(std::function<void()> cb) {
            wake_ = std::move(cb);
        }

        void release_callbacks() {
            wake_    = nullptr;
            on_tick_ = nullptr;
            {
                std::lock_guard<std::mutex> lock(candle_mutex_);
                candle_.reset();
                timescale_.reset();
            }
            on_order_      = nullptr;
            on_error_      = nullptr;
            on_open_       = nullptr;
            on_close_      = nullptr;
            on_stalled_    = nullptr;
            on_shutdown_   = nullptr;
            on_log_        = nullptr;
            on_gap_        = nullptr;
            on_gap_report_ = nullptr;
            {
                std::lock_guard<std::mutex> lk(track_mutex_);
                pending_outage_.clear();
            }

            for (auto &b : brokers_)
                if (b.ticker)
                    b.ticker->release_callbacks();
        }

        void start() {
            if (brokers_.empty()) {
                brokers_.resize(specs_.size());
                pending_shutdowns_ = brokers_.size();
                for (std::size_t i = 0; i < specs_.size(); ++i) {
                    brokers_[i].name = specs_[i].name;
                    brokers_[i].ioc  = std::make_unique<net::io_context>();
                }
                for (std::size_t i = 0; i < specs_.size(); ++i)
                    wire_broker(i, specs_[i]);
            }

            for (auto &b : brokers_) {
                b.work_guard.emplace(net::make_work_guard(*b.ioc));
                b.thread = std::thread([this, &b] {
                    b.ticker->start();
                    b.ioc->run();
                    b.thread_finished->store(true, std::memory_order_release);
                    { std::lock_guard<std::mutex> lk(reap_mutex_); }
                    reap_cv_.notify_all();
                });
            }

            started_.store(true, std::memory_order_release);
            bool want_timer;
            {
                std::lock_guard<std::mutex> lock(candle_mutex_);
                want_timer = candle_needs_timer_;
            }
            if (want_timer)
                ensure_candle_thread();
        }

        void stop() {
            for (auto &b : brokers_)
                b.ticker->stop();
        }

        [[nodiscard]] bool join_all(std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
            std::lock_guard<std::mutex> serial(join_mutex_);
            if (joined_)
                return true;
            const auto all_finished = [this] {
                for (auto &b : brokers_)
                    if (b.thread.joinable() && !b.thread_finished->load(std::memory_order_acquire))
                        return false;
                return true;
            };
            {
                std::unique_lock<std::mutex> lock(reap_mutex_);
                if (!reap_cv_.wait_for(lock, timeout, all_finished)) {
                    for (auto &b : brokers_)
                        if (b.ioc)
                            b.ioc->stop();
                    reap_cv_.wait_for(lock, timeout, all_finished);
                }
            }
            bool ok = true;
            for (auto &b : brokers_) {
                if (!b.thread.joinable())
                    continue;
                if (b.thread_finished->load(std::memory_order_acquire))
                    b.thread.join();
                else
                    ok = false;
            }
            joined_ = ok;
            if (ok)
                stop_candle_thread();
            return ok;
        }

        void install_signal_handlers(
            std::chrono::seconds signal_shutdown_grace = std::chrono::seconds(2)) {
            signal_shutdown_grace_ = signal_shutdown_grace;
            signals_installed_     = true;
            signals_.add(SIGINT);
            signals_.add(SIGTERM);
            auto self = shared_from_this();
            signals_.async_wait([self](boost::system::error_code ec, int signum) {
                if (ec)
                    return;
                self->handle_stop_signal(signum);
            });
        }

        void subscribe(std::vector<std::string> instruments,
                       fsticker::FeedType       feed_type,
                       const std::string       &target = "") {
            for (auto &b : brokers_) {
                if (!target.empty() && target != b.name)
                    continue;
                b.ticker->subscribe(instruments, feed_type);
                {
                    std::lock_guard<std::mutex> lk(track_mutex_);
                    auto                       &set = broker_tokens_[b.name];
                    set.insert(instruments.begin(), instruments.end());
                }
            }
        }

        void unsubscribe(std::vector<std::string> instruments,
                         fsticker::FeedType       feed_type,
                         const std::string       &target = "") {
            for (auto &b : brokers_) {
                if (!target.empty() && target != b.name)
                    continue;
                b.ticker->unsubscribe(instruments, feed_type);

                {
                    std::lock_guard<std::mutex> lk(track_mutex_);
                    auto                       &set = broker_tokens_[b.name];
                    for (const auto &i : instruments)
                        set.erase(i);
                }

                net::post(*b.ioc, [&b, instruments] {
                    for (const auto &instrument : instruments)
                        b.reconstructor.forget(instrument);
                });
            }
            if (target.empty()) {

                {
                    std::lock_guard<std::mutex> store_lock(store_mutex_);
                    for (const auto &instrument : instruments)
                        store_.forget(instrument);
                }

                std::lock_guard<std::mutex> candle_lock(candle_mutex_);
                if (candle_)
                    for (const auto &instrument : instruments)
                        candle_->forget(instrument);
            }
        }

        [[nodiscard]] bool wait_connected(
            std::optional<std::chrono::milliseconds> timeout = std::nullopt) {
            const auto deadline =
                timeout ? std::optional(std::chrono::steady_clock::now() + *timeout) : std::nullopt;
            for (auto &b : brokers_) {
                std::optional<std::chrono::milliseconds> remaining;
                if (deadline) {
                    const auto now = std::chrono::steady_clock::now();
                    if (now >= *deadline)
                        return false;
                    remaining =
                        std::chrono::duration_cast<std::chrono::milliseconds>(*deadline - now);
                }
                if (!b.ticker->wait_connected(remaining))
                    return false;
            }
            return true;
        }

        enum class ConnectState { Pending, Connected, Closed };

        [[nodiscard]] ConnectState connect_state() const noexcept {
            if (brokers_.empty())
                return ConnectState::Pending;
            if (any_broker_closed())
                return ConnectState::Closed;
            for (const auto &b : brokers_)
                if (!b.ticker->is_connected())
                    return ConnectState::Pending;
            return ConnectState::Connected;
        }

        [[nodiscard]] bool wait_closed(std::chrono::milliseconds timeout) {
            std::unique_lock<std::mutex> lock(closed_mutex_);
            return closed_cv_.wait_for(lock, timeout, [this] { return closed_; });
        }

        [[nodiscard]] bool any_broker_closed() const noexcept {
            return pending_shutdowns_.load(std::memory_order_acquire) < brokers_.size();
        }

        [[nodiscard]] std::size_t broker_count() const noexcept {
            return specs_.size();
        }

    private:
        using WorkGuard = net::executor_work_guard<net::io_context::executor_type>;

        struct BrokerState {
            std::string                        name;
            std::unique_ptr<net::io_context>   ioc;
            std::optional<WorkGuard>           work_guard;
            std::shared_ptr<fsticker::Ticker>  ticker;
            TickReconstructor                  reconstructor;
            std::thread                        thread;
            std::unique_ptr<std::atomic<bool>> thread_finished =
                std::make_unique<std::atomic<bool>>(false);
        };

        struct PendingOutage {
            std::vector<CandleEngine::OpenBucketInfo> buckets;
        };

        void wire_broker(std::size_t index, const BrokerSpec &spec) {
            auto             self       = shared_from_this();
            net::io_context &broker_ioc = *brokers_[index].ioc;

            fsticker::Callbacks callbacks;

            callbacks.subscribe_callback = [self, index](const char *data, std::size_t size) {
                auto tick = self->brokers_[index].reconstructor.on_message(
                    self->brokers_[index].name, data, size);
                if (!tick)
                    return;

                std::optional<Tick> merged;
                {
                    std::lock_guard<std::mutex> lock(self->store_mutex_);
                    merged = self->store_.diff_and_update(std::move(*tick));
                }
                if (merged) {
                    const std::string instrument_key =
                        get_string(*merged, "e") + "|" + get_string(*merged, "tk");


                    std::optional<PendingOutage> resumed;
                    std::int64_t                 anchor = 0;
                    {
                        std::lock_guard<std::mutex> lk(self->track_mutex_);
                        const auto                  it = self->pending_outage_.find(instrument_key);
                        if (it != self->pending_outage_.end()) {
                            resumed = std::move(it->second);
                            self->pending_outage_.erase(it);
                            anchor = self->anchor_of(instrument_key);
                        }
                    }
                    bool wake_finalizer = false;
                    {
                        std::lock_guard<std::mutex> lock(self->candle_mutex_);
                        if (self->candle_) {
                            self->candle_->on_tick(*merged);
                            wake_finalizer = self->candle_->consume_earlier_deadline_flag();

                            if (resumed) {
                                const auto ft_opt = get_int(*merged, "ft");
                                if (ft_opt)
                                    for (const auto &b : resumed->buckets) {
                                        const std::int64_t resume_bucket =
                                            CandleEngine::bucket_start(*ft_opt, anchor, b.period_s);
                                        if (*ft_opt > resume_bucket)
                                            self->candle_->mark_suspect(instrument_key, b.slot_index);
                                    }
                            }
                        }
                    }
                    if (wake_finalizer)
                        self->kick_candle_thread();

                    if (resumed)
                        self->build_and_fire_gap_report(instrument_key, self->brokers_[index].name,
                                                        *merged, *resumed);
                }
                if (merged && self->on_tick_)
                    self->on_tick_(*merged);
            };
            callbacks.order_update_callback = [self, index](const char *data, std::size_t size) {
                if (self->on_order_)
                    self->on_order_(self->brokers_[index].name, data, size);
            };
            callbacks.error_callback = [self, index](const char *data, std::size_t size) {
                if (self->on_error_)
                    self->on_error_(self->brokers_[index].name, data, size);
            };
            callbacks.open_callback = [self, index](const char *data, std::size_t size) {
                self->notify_wake();
                self->note_broker_connected(self->brokers_[index].name);
                if (self->on_open_)
                    self->on_open_(self->brokers_[index].name, data, size);
            };
            callbacks.close_callback = [self, index] {
                if (self->on_close_)
                    self->on_close_(self->brokers_[index].name);
                self->note_broker_down(self->brokers_[index].name);
            };
            callbacks.stalled_callback = [self, index](std::uint32_t n) {
                if (self->on_stalled_)
                    self->on_stalled_(self->brokers_[index].name, n);
            };

            callbacks.log_callback = [self, index](int level, std::string_view msg) {
                if (self->on_log_)
                    self->on_log_(self->brokers_[index].name, level, msg);
            };

            brokers_[index].ticker = std::make_shared<fsticker::Ticker>(broker_ioc, spec.credentials,
                                                                        callbacks, spec.params);
            brokers_[index].ticker->set_shutdown_callback([self, index] {
                self->brokers_[index].work_guard.reset();
                self->on_broker_shutdown();
            });
        }

        void ensure_candle_thread() {
            std::lock_guard<std::mutex> lock(candle_thread_mutex_);
            if (candle_thread_.joinable() || candle_thread_stop_)
                return;
            candle_thread_ = std::thread([this] { candle_thread_main(); });
        }

        void stop_candle_thread() {
            std::thread t;
            {
                std::lock_guard<std::mutex> lock(candle_thread_mutex_);
                candle_thread_stop_ = true;
                t                   = std::move(candle_thread_);
            }
            candle_thread_cv_.notify_all();
            if (t.joinable())
                t.join();
        }

        void kick_candle_thread() {
            {
                std::lock_guard<std::mutex> lock(candle_thread_mutex_);
                candle_thread_kick_ = true;
            }
            candle_thread_cv_.notify_all();
        }

        void candle_thread_main() {
            std::unique_lock<std::mutex> lock(candle_thread_mutex_);
            while (!candle_thread_stop_) {
                candle_thread_kick_ = false;
                std::optional<std::int64_t> deadline;
                {
                    std::lock_guard<std::mutex> g(candle_mutex_);
                    if (candle_)
                        deadline = candle_->next_deadline();
                }
                const auto ready = [this] { return candle_thread_stop_ || candle_thread_kick_; };
                if (!deadline)
                    candle_thread_cv_.wait(lock, ready); // nothing pending: sleep until kicked
                else
                    candle_thread_cv_.wait_until(
                        lock,
                        std::chrono::system_clock::time_point {std::chrono::seconds {*deadline}},
                        ready);
                if (candle_thread_stop_)
                    break;
                lock.unlock();
                const auto now = std::chrono::duration_cast<std::chrono::seconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count();
                {
                    std::lock_guard<std::mutex> g(candle_mutex_);
                    if (candle_)
                        candle_->check_late_candles(now);
                }
                lock.lock();
            }
        }

        void handle_stop_signal(int signum) {
            if (signal_handling_started_)
                return;
            signal_handling_started_ = true;
            pending_signum_          = signum;

            stop();

            auto self = shared_from_this();
            signal_grace_timer_.expires_after(signal_shutdown_grace_);
            signal_grace_timer_.async_wait([self](boost::system::error_code ec) {
                if (ec)
                    return;
                self->finish_signal_shutdown();
            });
        }

        void finish_signal_shutdown() {
            if (signal_shutdown_finished_)
                return;
            signal_shutdown_finished_ = true;
            signal_grace_timer_.cancel();
            const int signum = pending_signum_;
            std::signal(signum, SIG_DFL);
            std::raise(signum);
        }

        void on_broker_shutdown() {
            const auto remaining = --pending_shutdowns_;
            notify_wake();
            if (remaining != 0)
                return;
            if (on_shutdown_)
                on_shutdown_();

            {
                std::lock_guard<std::mutex> lock(closed_mutex_);
                closed_ = true;
            }
            closed_cv_.notify_all();

            if (signal_handling_started_) {
                finish_signal_shutdown();
            } else if (signals_installed_) {
                auto self = shared_from_this();
                net::post(signals_.get_executor(), [self] {
                    boost::system::error_code ignore;
                    self->signals_.cancel(ignore);
                });
            }

            // std::thread([self] { self->reap_worker_threads(std::chrono::seconds(5)); }).detach();
        }

        void note_broker_connected(const std::string &broker) {
            std::lock_guard<std::mutex> lk(track_mutex_);
            connected_.insert(broker);
        }

        [[nodiscard]] std::int64_t anchor_of(const std::string &token) const {
            const auto bar = token.find('|');
            const auto it  = gap_anchors_.find(token.substr(0, bar));
            return it == gap_anchors_.end() ? 0 : it->second;
        }

        void note_broker_down(const std::string &broker) {
            std::vector<std::string> affected_tokens;
            {
                std::lock_guard<std::mutex> lk(track_mutex_);
                if (connected_.erase(broker) == 0)
                    return;
                const auto mine = broker_tokens_.find(broker);
                if (mine == broker_tokens_.end())
                    return;
                for (const auto &tok : mine->second) {
                    bool covered = false;
                    for (const auto &other : connected_) {
                        const auto o = broker_tokens_.find(other);
                        if (o != broker_tokens_.end() && o->second.count(tok)) {
                            covered = true;
                            break;
                        }
                    }
                    if (!covered)
                        affected_tokens.push_back(tok);
                }
            }
            if (affected_tokens.empty())
                return;


            {
                std::lock_guard<std::mutex> lock(candle_mutex_);
                if (candle_) {
                    std::lock_guard<std::mutex> lk(track_mutex_);
                    for (const auto &tok : affected_tokens) {
                        auto buckets = candle_->open_buckets(tok);
                        for (const auto &b : buckets)
                            if (b.is_open)
                                candle_->mark_suspect(tok, b.slot_index);
                        pending_outage_[tok] = PendingOutage {std::move(buckets)};
                    }
                }
            }

            if (!on_gap_ || gap_periods_.empty())
                return;
            std::vector<CandleGap> events;
            {
                std::lock_guard<std::mutex>                      lk(track_mutex_);
                std::map<std::int64_t, std::vector<std::string>> by_anchor;
                for (const auto &tok : affected_tokens)
                    by_anchor[anchor_of(tok)].push_back(tok);

                const std::int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                                             std::chrono::system_clock::now().time_since_epoch())
                                             .count();
                for (auto &[anchor, toks] : by_anchor) {
                    CandleGap g;
                    g.broker = broker;
                    g.tokens = std::move(toks);
                    for (const auto p : gap_periods_)
                        g.periods.emplace_back(p, CandleEngine::bucket_start(now, anchor, p));
                    events.push_back(std::move(g));
                }
            }
            for (const auto &e : events)
                on_gap_(e);
        }

        void build_and_fire_gap_report(const std::string   &instrument_key,
                                       const std::string   &broker,
                                       const Tick          &merged,
                                       const PendingOutage &outage) {
            if (!on_gap_report_)
                return;
            const auto ft_opt = get_int(merged, "ft");
            if (!ft_opt)
                return;
            const std::int64_t resume_ft = *ft_opt;
            const std::int64_t anchor    = anchor_of(instrument_key);

            CandleGapReport report;
            report.token  = instrument_key;
            report.broker = broker;

            for (const auto &b : outage.buckets) {
                const std::int64_t period_s = b.period_s;
                const std::int64_t baseline = b.is_open ? b.period_start : b.closed_through;
                if (baseline == -1)
                    continue;

                const std::int64_t resume_bucket =
                    CandleEngine::bucket_start(resume_ft, anchor, period_s);
                const bool         disc_partial   = b.is_open;
                const bool         resume_partial = resume_ft > resume_bucket;
                const std::int64_t missing_start  = baseline + period_s;
                const std::int64_t missing_end    = resume_bucket - period_s;
                const bool         has_missing    = missing_start <= missing_end;

                CandleGapReport::PeriodReport pr;
                pr.period = period_s;

                if (b.omit_possible_partial) {
                    std::vector<std::int64_t> bounds;
                    if (disc_partial)
                        bounds.push_back(b.period_start);
                    if (has_missing) {
                        bounds.push_back(missing_start);
                        bounds.push_back(missing_end);
                    }
                    if (resume_partial)
                        bounds.push_back(resume_bucket);
                    if (bounds.empty())
                        continue;
                    pr.has_range = true;
                    pr.start     = bounds.front();
                    pr.end       = bounds.back();
                } else {
                    if (disc_partial)
                        pr.partial.push_back(b.period_start);
                    if (resume_partial && (pr.partial.empty() || pr.partial.back() != resume_bucket))
                        pr.partial.push_back(resume_bucket);
                    if (has_missing) {
                        pr.has_range = true;
                        pr.start     = missing_start;
                        pr.end       = missing_end;
                    }
                    if (pr.partial.empty() && !pr.has_range)
                        continue;
                }
                report.periods.push_back(std::move(pr));
            }

            if (!report.periods.empty())
                on_gap_report_(report);
        }

        void notify_wake() const {
            if (wake_)
                wake_();
        }

        std::vector<BrokerSpec>  specs_;
        std::vector<BrokerState> brokers_;

        std::mutex store_mutex_;
        FeedStore  store_;

        std::mutex                     candle_mutex_;
        std::optional<CandleEngine>    candle_;
        bool                           candle_needs_timer_ = false;
        std::unique_ptr<TimescaleSink> timescale_;

        std::atomic<bool>       started_ {false};
        std::mutex              candle_thread_mutex_;
        std::condition_variable candle_thread_cv_;
        bool                    candle_thread_stop_ = false;
        bool                    candle_thread_kick_ = false;
        std::thread             candle_thread_;

        TickCallback            on_tick_;
        OrderCallback           on_order_;
        ErrorCallback           on_error_;
        OpenCallback            on_open_;
        CloseCallback           on_close_;
        StalledCallback         on_stalled_;
        ShutdownCallback        on_shutdown_;
        LogCallback             on_log_;
        CandleGapCallback       on_gap_;
        CandleGapReportCallback on_gap_report_;

        std::mutex                                                       track_mutex_;
        std::unordered_map<std::string, std::unordered_set<std::string>> broker_tokens_;
        std::unordered_set<std::string>                                  connected_;
        std::vector<std::int64_t>                                        gap_periods_;
        ExchangeAnchors                                                  gap_anchors_;
        std::unordered_map<std::string, PendingOutage>                   pending_outage_;

        std::atomic<std::size_t> pending_shutdowns_ {0};
        std::atomic<bool>        signals_installed_ {false};

        std::mutex              join_mutex_;
        bool                    joined_ = false;
        std::mutex              reap_mutex_;
        std::condition_variable reap_cv_;
        std::function<void()>   wake_;

        std::mutex              closed_mutex_;
        std::condition_variable closed_cv_;
        bool                    closed_ = false;

        net::signal_set      signals_;
        net::steady_timer    signal_grace_timer_;
        std::chrono::seconds signal_shutdown_grace_ {2};
        bool                 signal_handling_started_  = false;
        bool                 signal_shutdown_finished_ = false;
        int                  pending_signum_           = 0;
    };

} // namespace fsticker::merge