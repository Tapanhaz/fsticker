// Copyright (c) 2026 Tapanhaz
// Part of fsticker :: https://github.com/Tapanhaz/fsticker
// Licensed under the Apache License, Version 2.0
// Created: 2026-09-25 10:41 IST

#pragma once

#include "backoff.hpp"
#include "ip_pinning.hpp"
#include "session.hpp"
#include "tls_roots.hpp"
#include "types.hpp"

#include <algorithm>
#include <atomic>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <memory>
#include <mutex>
#include <optional>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <string>
#include <vector>

namespace fsticker {
    namespace net = boost::asio;

    struct TickerParams {
        std::size_t               token_limit = 30;
        std::chrono::milliseconds resolve_timeout {3000};
        std::chrono::milliseconds handshake_timeout {4000};
        std::chrono::milliseconds idle_ping_timeout {3000};
        std::chrono::milliseconds ping_reply_timeout {2000};
        std::string               ping_payload {R"({"t":"h"})"};
        std::chrono::seconds      pin_quarantine_ttl {20};

        std::chrono::duration<double> min_backoff {0.25};
        std::chrono::duration<double> max_backoff {5.0};
        double                        backoff_multiplier = 2.0;
        double                        backoff_jitter     = 0.3;

        std::chrono::milliseconds     no_network_retry_interval {500};
        std::chrono::duration<double> pinning_probe_interval {300.0};
        std::chrono::duration<double> min_stable_before_probe {30.0};
        std::uint32_t                 stall_alert_every = 5;

        std::chrono::seconds signal_shutdown_grace {2};

        LogLevel min_log_level = kInfo;
    };

    class Ticker : public std::enable_shared_from_this<Ticker> {
    public:
        using Params           = TickerParams;
        using ShutdownCallback = std::function<void()>;

        Ticker(net::io_context &ioc,
               Credentials      credentials,
               Callbacks        callbacks,
               Params params = {}) : ioc_(ioc),
                                     ssl_ctx_(net::ssl::context::tls_client),
                                     credentials_(std::move(credentials)),
                                     callbacks_(std::move(callbacks)),
                                     params_(params),
                                     pinning_(ioc),
                                     backoff_(Backoff::Params {
                                         params_.min_backoff, params_.max_backoff,
                                         params_.backoff_multiplier, params_.backoff_jitter}),
                                     sleep_timer_(ioc),
                                     signals_(ioc),
                                     signal_grace_timer_(ioc) {

            ::fsticker::detail::configure_trust_store(ssl_ctx_);
        }

        void start() {
            auto self = shared_from_this();
            net::post(ioc_, [self] { self->run_connect_cycle(); });
        }

        void stop() {
            auto self = shared_from_this();
            net::post(ioc_, [self] {
                if (!self->session_had_connected_ || self->disconnect_requested_) {
                    self->disconnect_requested_ = true;
                    self->abort_pending_sleep();
                    if (self->session_) {
                        self->session_->force_close();
                    }
                    return;
                }
                self->disconnect_requested_ = true;
                self->abort_pending_sleep();
                if (self->session_) {
                    self->session_->send_close(1000, "Connection closed by the user.");
                }
            });
        }

        void install_signal_handlers() {
            signals_.add(SIGINT);
            signals_.add(SIGTERM);
            auto self = shared_from_this();
            signals_.async_wait([self](boost::system::error_code ec, int signum) {
                if (ec)
                    return;
                self->handle_stop_signal(signum);
            });
        }

        void set_shutdown_callback(ShutdownCallback cb) {
            on_shutdown_ = std::move(cb);
        }

        void release_callbacks() {
            callbacks_   = {};
            on_shutdown_ = nullptr;
        }

        void subscribe(std::vector<std::string> instruments,
                       FeedType                 feed_type = FeedType::SnapQuote) {
            auto self = shared_from_this();
            net::post(ioc_, [self, instruments = std::move(instruments), feed_type]() mutable {
                auto &tracked = self->tracked_list(feed_type);
                for (const auto &i : instruments)
                    add_unique(tracked, i);
                if (self->connected_ready_)
                    self->send_feed_command(instruments, feed_type, true);
            });
        }

        void subscribe(std::string instrument, FeedType feed_type = FeedType::SnapQuote) {
            subscribe(std::vector<std::string> {std::move(instrument)}, feed_type);
        }

        void unsubscribe(std::vector<std::string> instruments,
                         FeedType                 feed_type = FeedType::SnapQuote) {
            auto self = shared_from_this();
            net::post(ioc_, [self, instruments = std::move(instruments), feed_type]() mutable {
                auto &tracked = self->tracked_list(feed_type);
                remove_all(tracked, instruments);
                if (self->connected_ready_)
                    self->send_feed_command(instruments, feed_type, false);
            });
        }

        void unsubscribe(std::string instrument, FeedType feed_type = FeedType::SnapQuote) {
            unsubscribe(std::vector<std::string> {std::move(instrument)}, feed_type);
        }

        [[nodiscard]] bool is_connected() const noexcept {
            return connected_ready_;
        }

        bool wait_connected(std::optional<std::chrono::milliseconds> timeout = std::nullopt) {
            std::unique_lock<std::mutex> lock(connected_mutex_);
            const auto                   settled = [this] {
                return connected_ready_.load() || shutdown_initiated_.load();
            };
            if (timeout) {
                if (!connected_cv_.wait_for(lock, *timeout, settled))
                    return false;
            } else {
                connected_cv_.wait(lock, settled);
            }
            return connected_ready_.load();
        }

    private:
        static constexpr std::chrono::seconds kMinStableSession {5};

        void run_connect_cycle() {
            if (disconnect_requested_) {
                initiate_shutdown();
                return;
            }

            if (pinning_enabled_ && pin_probe_due()) {
                log(kInfo, "Pinned session was stable -- probing normal path for recovery");
                pinning_enabled_ = false;
            }

            WsSession::Params sp;
            sp.resolve_timeout    = params_.resolve_timeout;
            sp.handshake_timeout  = params_.handshake_timeout;
            sp.idle_ping_timeout  = params_.idle_ping_timeout;
            sp.ping_reply_timeout = params_.ping_reply_timeout;
            sp.ping_payload       = params_.ping_payload;
            sp.pin_quarantine_ttl = params_.pin_quarantine_ttl;
            sp.verify_ssl         = credentials_.verify_ssl;

            session_               = std::make_shared<WsSession>(ioc_, ssl_ctx_, pinning_, sp);
            session_had_connected_ = false;
            connected_ready_       = false;

            const std::string full_url = credentials_.ws_endpoint + credentials_.token;
            auto              self     = shared_from_this();
            session_->async_run(
                full_url, pinning_enabled_,
                [self](FrameType t, const char *d, std::size_t n) { self->on_frame(t, d, n); },
                [self] { self->on_transport_connected(); },
                [self](boost::system::error_code ec, std::uint16_t code, std::string reason,
                       std::uint16_t http_status) {
                    self->on_disconnected(ec, code, std::move(reason), http_status);
                },
                [self](int level, std::string msg) { self->log(level, msg); });
        }

        void on_transport_connected() {
            session_had_connected_ = true;

            connected_at_ = std::chrono::steady_clock::now();
            log(kInfo, pinning_enabled_ ? ("Connected via pinned IP " + session_->last_used_ip())
                                        : "Connected to " + credentials_.ws_endpoint);
            send_auth();
        }

        void on_frame(FrameType type, const char *data, std::size_t size) {
            if (callbacks_.raw_frame_callback)
                callbacks_.raw_frame_callback(type, data, size);

            if (type != FrameType::Text)
                return;

            rapidjson::Document doc;
            doc.Parse(data, size);
            if (doc.HasParseError() || !doc.IsObject()) {
                log(kWarn, "Received non-JSON text frame; skipping routed dispatch");
                return;
            }

            const auto t_it = doc.FindMember("t");
            if (t_it == doc.MemberEnd() || !t_it->value.IsString()) {
                log(kWarn, "Text frame missing 't' field; skipping routed dispatch");
                return;
            }
            const std::string t(t_it->value.GetString(), t_it->value.GetStringLength());

            if (t == "ak" || t == "ck") {
                handle_connection_ack(data, size);
            } else if (t == "udk" || t == "uk") {
                if (callbacks_.unsubscribe_callback)
                    callbacks_.unsubscribe_callback(data, size);
            } else if (t == "am" || t == "ms") {
                if (callbacks_.alert_callback)
                    callbacks_.alert_callback(data, size);
            } else if (t == "om") {
                if (callbacks_.order_update_callback)
                    callbacks_.order_update_callback(data, size);
            } else if (t == "tf" || t == "df" || t == "tk" || t == "dk") {
                if (callbacks_.subscribe_callback)
                    callbacks_.subscribe_callback(data, size);
            } else {
                log(kDebug, "Unrouted message type: " + t);
            }
        }

        void on_disconnected(boost::system::error_code ec,
                             std::uint16_t             close_code,
                             std::string               reason,
                             std::uint16_t             http_status) {
            const bool was_connected = session_had_connected_;
            session_had_connected_   = false;
            connected_ready_         = false;


            const bool stable = was_connected && std::chrono::steady_clock::now() - connected_at_ >=
                                                     kMinStableSession;
            if (stable)
                backoff_.reset();


            if (was_connected && pinning_enabled_) {
                last_pin_stable_duration_ =
                    std::chrono::duration<double>(std::chrono::steady_clock::now() - connected_at_)
                        .count();
            }

            if (was_connected && callbacks_.close_callback)
                callbacks_.close_callback();

            if (disconnect_requested_) {
                initiate_shutdown();
                return;
            }

            if (close_code == 1008) {
                disconnect_requested_ = true;
                log(kError, "Invalid credentials, closing (" + reason + ")");
                initiate_shutdown();
                return;
            }

            if (!was_connected && is_permanent_rejection(http_status)) {
                disconnect_requested_ = true;
                const std::string msg = "WebSocket upgrade rejected (HTTP " +
                                        std::to_string(http_status) + "), not retrying";
                log(kError, msg);
                if (callbacks_.error_callback)
                    callbacks_.error_callback(msg.data(), msg.size());
                initiate_shutdown();
                return;
            }

            const std::string detail =
                ec.message() + (http_status ? " [HTTP " + std::to_string(http_status) + "]" : "");

            if (was_connected) {
                if (stable) {
                    log(kInfo, "Disconnected, reconnecting...");
                    schedule_reconnect_immediate();
                    return;
                }
                log(kWarn, "Session dropped shortly after connecting, backing off");
                sleep_backoff_then_reconnect();
                return;
            }

            if (is_no_network_error(ec)) {
                log(kWarn, "No local network path (" + ec.message() + "), retrying shortly");
                sleep_no_network_then_reconnect();
                return;
            }

            if (credentials_.enable_ip_pinning && !pinning_enabled_) {
                pinning_enabled_ = true;
                log(kWarn, "Connect failed (" + detail + "), switching to per-IP failover");
                schedule_reconnect_immediate();
                return;
            }

            log(kWarn, "Connect failed (" + detail + "), backing off");
            sleep_backoff_then_reconnect();
        }

        void handle_connection_ack(const char *data, std::size_t size) {
            rapidjson::Document doc;
            doc.Parse(data, size);
            const auto        s_it = doc.FindMember("s");
            const std::string s =
                (s_it != doc.MemberEnd() && s_it->value.IsString())
                    ? std::string(s_it->value.GetString(), s_it->value.GetStringLength())
                    : std::string {};

            if (s != "OK") {
                if (callbacks_.error_callback)
                    callbacks_.error_callback(data, size);
                return;
            }

            {
                std::lock_guard<std::mutex> lock(connected_mutex_);
                connected_ready_ = true;
            }
            connected_cv_.notify_all();
            flush_pending_subscriptions();
            if (callbacks_.open_callback)
                callbacks_.open_callback(data, size);
        }

        void schedule_reconnect_immediate() {
            auto self = shared_from_this();
            net::post(ioc_, [self] { self->run_connect_cycle(); });
        }

        void sleep_no_network_then_reconnect() {
            auto self = shared_from_this();
            sleep_timer_.expires_after(params_.no_network_retry_interval);
            sleep_timer_.async_wait([self](boost::system::error_code) { self->run_connect_cycle(); });
        }

        void sleep_backoff_then_reconnect() {
            const auto delay = backoff_.next_delay();
            const auto n     = backoff_.consecutive_failures();

            if (callbacks_.stalled_callback && params_.stall_alert_every > 0 &&
                n % params_.stall_alert_every == 0) {
                callbacks_.stalled_callback(n);
            }

            auto self = shared_from_this();
            sleep_timer_.expires_after(std::chrono::duration_cast<std::chrono::milliseconds>(delay));
            sleep_timer_.async_wait([self](boost::system::error_code) { self->run_connect_cycle(); });
        }

        void abort_pending_sleep() {
            sleep_timer_.cancel();
        }

        void initiate_shutdown() {
            {
                std::lock_guard<std::mutex> lock(connected_mutex_);
                if (shutdown_initiated_)
                    return;
                shutdown_initiated_ = true;
            }
            connected_cv_.notify_all();
            log(kInfo, "Websocket disconnected, shutdown complete.");
            if (on_shutdown_)
                on_shutdown_();

            session_.reset();

            if (signal_handling_started_) {
                signal_grace_timer_.cancel();
                const int signum = pending_signum_;
                std::signal(signum, SIG_DFL);
                std::raise(signum);
            }
        }

        void handle_stop_signal(int signum) {
            if (signal_handling_started_)
                return;
            signal_handling_started_ = true;
            pending_signum_          = signum;

            std::fputc('\n', stdout);
            log(kInfo, "WebSocket closure initiated by user interrupt.");
            stop();

            auto self = shared_from_this();
            signal_grace_timer_.expires_after(params_.signal_shutdown_grace);
            signal_grace_timer_.async_wait([self](boost::system::error_code ec) {
                if (ec)
                    return;
                self->initiate_shutdown();
            });
        }

        [[nodiscard]] bool pin_probe_due() {
            if (last_pin_stable_duration_ < params_.min_stable_before_probe.count())
                return false;
            const auto now = std::chrono::steady_clock::now();
            if (std::chrono::duration<double>(now - last_pin_probe_at_).count() <
                params_.pinning_probe_interval.count())
                return false;
            last_pin_probe_at_ = now;
            return true;
        }

        [[nodiscard]] static bool is_permanent_rejection(std::uint16_t http_status) noexcept {
            return http_status >= 400 && http_status < 500 && http_status != 408 &&
                   http_status != 429;
        }

        [[nodiscard]] static bool is_no_network_error(boost::system::error_code ec) {
            return ec == net::error::network_unreachable || ec == net::error::network_down ||
                   ec == net::error::host_unreachable || ec == net::error::host_not_found ||
                   ec == net::error::host_not_found_try_again;
        }

        void send_auth() {
            const bool is_api = credentials_.access_type == AccessType::API;

            rapidjson::StringBuffer                    sb;
            rapidjson::Writer<rapidjson::StringBuffer> w(sb);
            w.StartObject();
            w.Key("t");
            w.String(is_api ? "a" : "c");
            w.Key("uid");
            w.String(credentials_.user_id.c_str(),
                     static_cast<rapidjson::SizeType>(credentials_.user_id.size()));
            w.Key("actid");
            w.String(credentials_.user_id.c_str(),
                     static_cast<rapidjson::SizeType>(credentials_.user_id.size()));
            w.Key(is_api ? "accesstoken" : "susertoken");
            w.String(credentials_.token.c_str(),
                     static_cast<rapidjson::SizeType>(credentials_.token.size()));
            w.Key("source");
            const auto src = to_string(credentials_.access_type);
            w.String(src.data(), static_cast<rapidjson::SizeType>(src.size()));
            w.EndObject();

            session_->send(sb.GetString(), /*binary=*/true);
        }

        void flush_pending_subscriptions() {
            if (!snapquote_list_.empty())
                send_feed_command(snapquote_list_, FeedType::SnapQuote, true);
            if (!touchline_list_.empty())
                send_feed_command(touchline_list_, FeedType::Touchline, true);
        }

        void send_feed_command(const std::vector<std::string> &instruments,
                               FeedType                        feed_type,
                               bool                            subscribing) {
            if (instruments.empty() || !session_)
                return;

            const char *type_field = subscribing ? (feed_type == FeedType::Touchline ? "t" : "d")
                                                 : (feed_type == FeedType::Touchline ? "u" : "ud");

            for (std::size_t offset = 0; offset < instruments.size(); offset += params_.token_limit) {
                const std::size_t end = std::min(instruments.size(), offset + params_.token_limit);

                std::string joined;
                for (std::size_t i = offset; i < end; ++i) {
                    if (i > offset)
                        joined += '#';
                    joined += instruments[i];
                }

                rapidjson::StringBuffer                    sb;
                rapidjson::Writer<rapidjson::StringBuffer> w(sb);
                w.StartObject();
                w.Key("t");
                w.String(type_field);
                w.Key("k");
                w.String(joined.c_str(), static_cast<rapidjson::SizeType>(joined.size()));
                w.EndObject();

                session_->send(sb.GetString(), /*binary=*/true);
            }
        }

        std::vector<std::string> &tracked_list(FeedType feed_type) {
            return feed_type == FeedType::Touchline ? touchline_list_ : snapquote_list_;
        }

        static void add_unique(std::vector<std::string> &list, const std::string &item) {
            if (std::find(list.begin(), list.end(), item) == list.end())
                list.push_back(item);
        }

        static void remove_all(std::vector<std::string> &list, const std::vector<std::string> &items) {
            list.erase(std::remove_if(list.begin(), list.end(),
                                      [&items](const std::string &v) {
                                          return std::find(items.begin(), items.end(), v) !=
                                                 items.end();
                                      }),
                       list.end());
        }

        void log(int level, std::string_view msg) {
            emit_log(level, params_.min_log_level, callbacks_.log_callback, msg);
        }

        net::io_context  &ioc_;
        net::ssl::context ssl_ctx_;
        Credentials       credentials_;
        Callbacks         callbacks_;
        Params            params_;

        IpPinning                  pinning_;
        Backoff                    backoff_;
        std::shared_ptr<WsSession> session_;

        std::vector<std::string> touchline_list_;
        std::vector<std::string> snapquote_list_;

        bool              pinning_enabled_       = false;
        bool              session_had_connected_ = false;
        std::atomic<bool> connected_ready_       = false;
        bool              disconnect_requested_  = false;
        std::atomic<bool> shutdown_initiated_    = false;

        std::mutex              connected_mutex_;
        std::condition_variable connected_cv_;

        double                                last_pin_stable_duration_ = 0.0;
        std::chrono::steady_clock::time_point last_pin_probe_at_ {};
        std::chrono::steady_clock::time_point connected_at_ {};

        net::steady_timer sleep_timer_;
        net::signal_set   signals_;
        net::steady_timer signal_grace_timer_;
        bool              signal_handling_started_ = false;
        int               pending_signum_          = 0;

        ShutdownCallback on_shutdown_;
    };

} // namespace fsticker
