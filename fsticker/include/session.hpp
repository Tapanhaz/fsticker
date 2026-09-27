// Copyright (c) 2026 Tapanhaz
// Part of fsticker :: https://github.com/Tapanhaz/fsticker
// Licensed under the Apache License, Version 2.0
// Created: 2026-09-25 10:41 IST

#pragma once

#include "ip_pinning.hpp"
#include "types.hpp"

#include <boost/asio/connect.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/host_name_verification.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <openssl/ssl.h>
#include <string>

namespace fsticker {

    namespace net       = boost::asio;
    namespace beast     = boost::beast;
    namespace websocket = beast::websocket;
    namespace ssl       = net::ssl;

    namespace {

        struct SplitUrl {
            std::string   scheme, host, target;
            std::uint16_t port;
        };

        [[nodiscard]] SplitUrl split_ws_url(const std::string &url) {
            SplitUrl out;
            auto     scheme_end = url.find("://");
            if (scheme_end == std::string::npos) {
                throw std::invalid_argument("fsticker::WsSession: URL missing scheme: " + url);
            }
            out.scheme                   = url.substr(0, scheme_end);
            const std::size_t host_start = scheme_end + 3;

            auto              path_start = url.find('/', host_start);
            const std::string authority  = (path_start == std::string::npos)
                                               ? url.substr(host_start)
                                               : url.substr(host_start, path_start - host_start);
            out.target = (path_start == std::string::npos) ? "/" : url.substr(path_start);
            if (out.target.empty())
                out.target = "/";

            auto colon = authority.rfind(':');

            if (colon != std::string::npos && authority.find(']') == std::string::npos) {
                out.host = authority.substr(0, colon);
                out.port = static_cast<std::uint16_t>(std::stoi(authority.substr(colon + 1)));
            } else {
                out.host = authority;
                out.port = (out.scheme == "wss") ? 443 : 80;
            }
            return out;
        }

        constexpr std::chrono::hours kEffectivelyNoBeastIdleTimeout {24 * 365 * 10};

    } // namespace

    class WsSession : public std::enable_shared_from_this<WsSession> {
    public:
        struct Params {
            std::chrono::milliseconds resolve_timeout {3000};
            std::chrono::milliseconds handshake_timeout {4000};
            std::chrono::milliseconds idle_ping_timeout {3000};
            std::chrono::milliseconds ping_reply_timeout {2000};
            std::string               ping_payload {R"({"t":"h"})"};
            std::chrono::seconds      pin_quarantine_ttl {20};
            bool                      verify_ssl = false;
        };

        using FrameHandler = std::function<void(FrameType, const char *, std::size_t)>;

        using ConnectedHandler = std::function<void()>;

        using LogHandler = std::function<void(int level, std::string)>;

        using DisconnectedHandler = std::function<void(boost::system::error_code ec,
                                                       std::uint16_t             close_code,
                                                       std::string               reason,
                                                       std::uint16_t             http_status)>;

        WsSession(net::io_context &ioc,
                  ssl::context    &ssl_ctx,
                  IpPinning       &pinning,
                  Params           params) : pinning_(pinning),
                                   params_(params),
                                   strand_(net::make_strand(ioc)),
                                   ws_(strand_, ssl_ctx),
                                   heartbeat_timer_(strand_) {
        }

        void async_run(std::string         full_url,
                       bool                use_ip_pinning,
                       FrameHandler        on_frame,
                       ConnectedHandler    on_connected,
                       DisconnectedHandler on_disconnected,
                       LogHandler          on_log = nullptr) {
            on_frame_        = std::move(on_frame);
            on_connected_    = std::move(on_connected);
            on_disconnected_ = std::move(on_disconnected);
            on_log_          = std::move(on_log);
            use_pinning_     = use_ip_pinning;

            SplitUrl parts;
            try {
                parts = split_ws_url(full_url);
            } catch (const std::exception &) {
                net::post(strand_,
                          [self = shared_from_this()] { self->fail(net::error::invalid_argument); });
                return;
            }
            scheme_ = parts.scheme;
            host_   = parts.host;
            target_ = parts.target;
            port_   = parts.port;

            net::post(strand_, [self = shared_from_this()] { self->start_connect(); });
        }

        void send(std::string payload, bool binary) {
            net::post(strand_,
                      [self = shared_from_this(), payload = std::move(payload), binary]() mutable {
                          self->outbox_.push_back(
                              OutItem {OutItem::Kind::Data, std::move(payload), binary, 1000});
                          self->pump_write();
                      });
        }

        void send_ping(std::string payload = {}) {
            net::post(strand_, [self = shared_from_this(), payload = std::move(payload)]() mutable {
                self->outbox_.push_back(
                    OutItem {OutItem::Kind::Ping, std::move(payload), false, 1000});
                self->pump_write();
            });
        }

        void send_close(std::uint16_t code, std::string reason) {
            net::post(strand_,
                      [self = shared_from_this(), code, reason = std::move(reason)]() mutable {
                          self->outbox_.push_back(
                              OutItem {OutItem::Kind::Close, std::move(reason), false, code});
                          self->pump_write();
                      });
        }

        void force_close() {
            net::post(strand_, [self = shared_from_this()] {
                boost::system::error_code ignore;
                beast::get_lowest_layer(self->ws_).socket().close(ignore);
                self->connected_ = false;
            });
        }

        [[nodiscard]] bool is_open() const noexcept {
            return connected_;
        }

        [[nodiscard]] const std::string &last_used_ip() const noexcept {
            return pinning_.last_used_ip();
        }

    private:
        struct OutItem {
            enum class Kind { Data, Ping, Close } kind;
            std::string   payload;
            bool          binary     = false;
            std::uint16_t close_code = 1000;
        };

        void start_connect() {
            if (use_pinning_)
                connect_pinned();
            else
                connect_normal();
        }

        void connect_normal() {
            auto self     = shared_from_this();
            auto resolver = std::make_shared<net::ip::tcp::resolver>(strand_);

            beast::get_lowest_layer(ws_).expires_after(params_.resolve_timeout +
                                                       params_.handshake_timeout);

            resolver->async_resolve(
                host_, std::to_string(port_),
                [self, resolver](beast::error_code ec, net::ip::tcp::resolver::results_type results) {
                    if (ec)
                        return self->fail(ec);
                    beast::get_lowest_layer(self->ws_).async_connect(
                        results, [self](beast::error_code ec2, net::ip::tcp::endpoint) {
                            if (ec2)
                                return self->fail(ec2);
                            self->on_tcp_connected();
                        });
                });
        }

        void connect_pinned() {
            auto self = shared_from_this();
            pinning_.async_resolve(
                host_, port_, params_.resolve_timeout,
                [self](boost::system::error_code ec, std::vector<std::string> ips) {
                    if (ec)
                        return self->fail(ec);
                    auto ordered = self->pinning_.order_candidates(ips);
                    self->pinning_.async_connect_pinned(
                        std::move(ordered), self->port_, self->params_.handshake_timeout,
                        self->params_.pin_quarantine_ttl,
                        [self](boost::system::error_code ec2, net::ip::tcp::socket socket,
                               std::string) {
                            net::post(self->strand_, [self, ec2, socket = std::move(socket)]() mutable {
                                if (ec2)
                                    return self->fail(ec2);
                                beast::get_lowest_layer(self->ws_).socket() = std::move(socket);
                                self->on_tcp_connected();
                            });
                        });
                });
        }

        void on_tcp_connected() {
            if (!SSL_set_tlsext_host_name(ws_.next_layer().native_handle(), host_.c_str())) {
                boost::system::error_code ec {static_cast<int>(::ERR_get_error()),
                                              net::error::get_ssl_category()};
                return fail(ec);
            }

            ws_.next_layer().set_verify_mode(params_.verify_ssl ? ssl::verify_peer
                                                                : ssl::verify_none);
            if (params_.verify_ssl) {
                ws_.next_layer().set_verify_callback(ssl::host_name_verification(host_));
            }

            beast::get_lowest_layer(ws_).expires_after(params_.handshake_timeout);
            auto self = shared_from_this();
            ws_.next_layer().async_handshake(ssl::stream_base::client, [self](beast::error_code ec) {
                if (ec)
                    return self->fail(ec);
                self->on_ssl_connected();
            });
        }

        void on_ssl_connected() {

            beast::get_lowest_layer(ws_).expires_never();

            websocket::stream_base::timeout opt;
            opt.handshake_timeout = params_.handshake_timeout;
            opt.idle_timeout      = kEffectivelyNoBeastIdleTimeout;
            opt.keep_alive_pings  = false;
            ws_.set_option(opt);

            ws_.set_option(websocket::stream_base::decorator([](websocket::request_type &req) {
                req.set(beast::http::field::user_agent, "fsticker-broker-ws-client/1.0");
            }));

            auto self = shared_from_this();
            ws_.control_callback([self](websocket::frame_type kind, beast::string_view payload) {
                self->on_control_frame(kind, payload);
            });

            ws_.async_handshake(handshake_res_, host_, target_, [self](beast::error_code ec) {
                if (ec) {
                    if (ec == websocket::error::upgrade_declined)
                        self->http_status_ =
                            static_cast<std::uint16_t>(self->handshake_res_.result_int());
                    return self->fail(ec);
                }
                self->on_ws_connected();
            });
        }

        void on_ws_connected() {
            connected_           = true;
            disconnect_reported_ = false;
            last_activity_       = std::chrono::steady_clock::now();
            ping_awaiting_reply_ = false;
            if (on_connected_)
                on_connected_();
            start_read();
            arm_heartbeat_timer();
        }

        void start_read() {
            auto self = shared_from_this();
            buffer_.consume(buffer_.size());
            ws_.async_read(buffer_, [self](beast::error_code ec, std::size_t bytes) {
                self->on_read(ec, bytes);
            });
        }

        void on_read(beast::error_code ec, std::size_t bytes_transferred) {
            if (ec) {
                connected_ = false;
                return finish_disconnect(ec);
            }
            note_activity();

            const auto      cbuf = buffer_.data();
            const char     *data = static_cast<const char *>(cbuf.data());
            const FrameType type = ws_.got_text() ? FrameType::Text : FrameType::Binary;

            if (on_frame_)
                on_frame_(type, data, bytes_transferred);

            start_read();
        }

        void on_control_frame(websocket::frame_type kind, beast::string_view payload) {

            switch (kind) {
                case websocket::frame_type::ping:
                    if (on_frame_)
                        on_frame_(FrameType::Ping, payload.data(), payload.size());
                    break;
                case websocket::frame_type::pong:
                    note_activity();
                    if (on_frame_)
                        on_frame_(FrameType::Pong, payload.data(), payload.size());
                    break;
                case websocket::frame_type::close: {
                    auto reason      = ws_.reason();
                    last_close_code_ = static_cast<std::uint16_t>(reason.code);
                    last_close_reason_.assign(reason.reason.data(), reason.reason.size());
                    if (on_frame_)
                        on_frame_(FrameType::Close, payload.data(), payload.size());
                    break;
                }
            }
        }

        void pump_write() {
            if (writing_ || outbox_.empty() || !connected_)
                return;
            writing_      = true;
            auto     self = shared_from_this();
            OutItem &item = outbox_.front();

            switch (item.kind) {
                case OutItem::Kind::Data:
                    ws_.binary(item.binary);
                    ws_.async_write(net::buffer(item.payload),
                                    [self](beast::error_code ec, std::size_t) { self->on_write(ec); });
                    break;
                case OutItem::Kind::Ping:
                    ws_.async_ping(websocket::ping_data(item.payload),
                                   [self](beast::error_code ec) { self->on_write(ec); });
                    break;
                case OutItem::Kind::Close:
                    ws_.async_close(
                        websocket::close_reason(static_cast<websocket::close_code>(item.close_code),
                                                item.payload),
                        [self](beast::error_code ec) { self->on_write(ec); });
                    break;
            }
        }

        void on_write(beast::error_code ec) {
            writing_ = false;
            if (!outbox_.empty())
                outbox_.pop_front();
            if (ec) {
                return;
            }
            pump_write();
        }

        void fail(boost::system::error_code ec) {
            connected_ = false;
            finish_disconnect(ec);
        }

        void finish_disconnect(boost::system::error_code ec) {
            if (disconnect_reported_)
                return;
            disconnect_reported_ = true;
            ws_.control_callback([](websocket::frame_type, beast::string_view) {});
            heartbeat_timer_.cancel();

            if (on_disconnected_)
                on_disconnected_(ec, last_close_code_, last_close_reason_, http_status_);
        }

        static constexpr std::chrono::milliseconds kHeartbeatCheckInterval {250};

        void arm_heartbeat_timer() {
            auto self = shared_from_this();
            heartbeat_timer_.expires_after(kHeartbeatCheckInterval);
            heartbeat_timer_.async_wait([self](beast::error_code ec) {
                if (ec)
                    return;
                self->on_heartbeat_tick();
            });
        }

        void on_heartbeat_tick() {
            if (!connected_)
                return;
            const auto now = std::chrono::steady_clock::now();

            if (ping_awaiting_reply_) {
                if (now - ping_sent_at_ >= params_.ping_reply_timeout) {
                    return fail(net::error::timed_out);
                }
            } else if (now - last_activity_ >= params_.idle_ping_timeout) {
                ping_awaiting_reply_ = true;
                ping_sent_at_        = now;
                if (on_log_)
                    on_log_(kDebug, "heartbeat: sending idle ping after " +
                                        std::to_string(params_.idle_ping_timeout.count()) +
                                        "ms idle, payload=" + params_.ping_payload);
                send_ping(params_.ping_payload);
            }

            arm_heartbeat_timer();
        }

        void note_activity() {
            last_activity_       = std::chrono::steady_clock::now();
            ping_awaiting_reply_ = false;
        }

        IpPinning &pinning_;
        Params     params_;

        net::strand<net::io_context::executor_type>             strand_;
        websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws_;
        beast::flat_buffer                                      buffer_;

        std::string   scheme_, host_, target_;
        std::uint16_t port_        = 443;
        bool          use_pinning_ = false;

        FrameHandler        on_frame_;
        ConnectedHandler    on_connected_;
        DisconnectedHandler on_disconnected_;
        LogHandler          on_log_;

        std::deque<OutItem> outbox_;
        bool                writing_             = false;
        bool                connected_           = false;
        bool                disconnect_reported_ = false;

        std::uint16_t            last_close_code_ = 0;
        std::string              last_close_reason_;
        websocket::response_type handshake_res_;
        std::uint16_t            http_status_ = 0;

        net::steady_timer                     heartbeat_timer_;
        std::chrono::steady_clock::time_point last_activity_ {};
        bool                                  ping_awaiting_reply_ = false;
        std::chrono::steady_clock::time_point ping_sent_at_ {};
    };

} // namespace fsticker
