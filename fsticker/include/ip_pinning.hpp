// Copyright (c) 2026 Tapanhaz
// Part of fsticker :: https://github.com/Tapanhaz/fsticker
// Licensed under the Apache License, Version 2.0
// Created: 2026-09-25 10:41 IST

#pragma once

#include <algorithm>
#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/system/error_code.hpp>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace fsticker {

    namespace net = boost::asio;
    using tcp     = net::ip::tcp;
    using boost::system::error_code;

    class PinnedConnectOp;

    class IpPinning {
    public:
        using ResolveHandler =
            std::function<void(boost::system::error_code, std::vector<std::string> /*ipv4 literals*/)>;

        using ConnectHandler =
            std::function<void(boost::system::error_code, tcp::socket, std::string /*used_ip*/)>;

        explicit IpPinning(net::io_context &ioc) : ioc_(ioc) {
        }

        void async_resolve(const std::string        &host,
                           std::uint16_t             port,
                           std::chrono::milliseconds timeout,
                           ResolveHandler            handler) {
            auto resolver = std::make_shared<tcp::resolver>(ioc_);
            auto timer    = std::make_shared<net::steady_timer>(ioc_);
            auto done     = std::make_shared<bool>(false);

            timer->expires_after(timeout);
            timer->async_wait([resolver, done](error_code ec) {
                if (ec || *done)
                    return;
                *done = true;
                resolver->cancel();
            });

            resolver->async_resolve(tcp::v4(), host, std::to_string(port),
                                    [resolver, timer, done, handler = std::move(handler)](
                                        error_code ec, tcp::resolver::results_type results) mutable {
                                        if (*done) {
                                            handler(net::error::timed_out, {});
                                            return;
                                        }
                                        *done = true;
                                        timer->cancel();

                                        if (ec) {
                                            handler(ec, {});
                                            return;
                                        }

                                        std::vector<std::string>        ips;
                                        std::unordered_set<std::string> seen;
                                        ips.reserve(results.size());
                                        for (const auto &entry : results) {
                                            auto addr = entry.endpoint().address();
                                            if (!addr.is_v4())
                                                continue;
                                            std::string s = addr.to_string();
                                            if (seen.insert(s).second)
                                                ips.push_back(std::move(s));
                                        }
                                        handler({}, std::move(ips));
                                    });
        }

        [[nodiscard]] std::vector<std::string> order_candidates(
            const std::vector<std::string> &candidates) {
            const auto now = std::chrono::steady_clock::now();
            for (auto it = quarantine_.begin(); it != quarantine_.end();) {
                if (it->second <= now)
                    it = quarantine_.erase(it);
                else
                    ++it;
            }

            std::vector<std::string> live, quarantined;
            live.reserve(candidates.size());
            for (const auto &ip : candidates) {
                if (quarantine_.find(ip) == quarantine_.end())
                    live.push_back(ip);
                else
                    quarantined.push_back(ip);
            }
            live_count_ = live.size();

            std::shuffle(live.begin(), live.end(), rng_);
            std::shuffle(quarantined.begin(), quarantined.end(), rng_);

            live.insert(live.end(), quarantined.begin(), quarantined.end());
            return live;
        }

        [[nodiscard]] std::size_t live_count() const noexcept {
            return live_count_;
        }

        void quarantine(const std::string &ip, std::chrono::seconds ttl) {
            quarantine_[ip] = std::chrono::steady_clock::now() + ttl;
        }

        void clear_quarantine() noexcept {
            quarantine_.clear();
        }

        void async_connect_pinned(std::vector<std::string>  candidates,
                                  std::uint16_t             port,
                                  std::chrono::milliseconds per_attempt_timeout,
                                  std::chrono::seconds      quarantine_ttl,
                                  ConnectHandler            handler);

        [[nodiscard]] const std::string &last_used_ip() const noexcept {
            return last_used_ip_;
        }

    private:
        net::io_context                                                       &ioc_;
        std::unordered_map<std::string, std::chrono::steady_clock::time_point> quarantine_;
        std::string                                                            last_used_ip_;
        std::size_t                                                            live_count_ = 0;
        std::mt19937 rng_ {std::random_device {}()};

        friend class PinnedConnectOp;
    };

    class PinnedConnectOp : public std::enable_shared_from_this<PinnedConnectOp> {
    public:
        PinnedConnectOp(IpPinning                &owner,
                        net::io_context          &ioc,
                        std::vector<std::string>  candidates,
                        std::uint16_t             port,
                        std::chrono::milliseconds per_attempt_timeout,
                        std::chrono::seconds      quarantine_ttl,
                        IpPinning::ConnectHandler handler) : owner_(owner),
                                                             ioc_(ioc),
                                                             candidates_(std::move(candidates)),
                                                             port_(port),
                                                             per_attempt_timeout_(per_attempt_timeout),
                                                             quarantine_ttl_(quarantine_ttl),
                                                             handler_(std::move(handler)),
                                                             socket_(ioc_),
                                                             timer_(ioc_),
                                                             remaining_live_(owner_.live_count()) {
        }

        void start() {
            try_next();
        }

    private:
        void try_next() {
            if (idx_ >= candidates_.size()) {
                auto ec = last_ec_ ? last_ec_ : make_error_code(net::error::host_unreachable);
                handler_(ec, tcp::socket(ioc_), {});
                return;
            }

            const std::string ip       = candidates_[idx_];
            const bool        was_live = idx_ < remaining_live_at_start_marker();
            ++idx_;

            socket_ = tcp::socket(ioc_);

            boost::system::error_code addr_ec;
            auto                      address = net::ip::make_address(ip, addr_ec);
            if (addr_ec) {
                last_ec_ = addr_ec;
                try_next();
                return;
            }
            tcp::endpoint ep(address, port_);

            auto self         = shared_from_this();
            auto attempt_done = std::make_shared<bool>(false);

            timer_.expires_after(per_attempt_timeout_);
            timer_.async_wait([self, attempt_done](error_code ec) {
                if (ec || *attempt_done)
                    return;
                *attempt_done = true;
                boost::system::error_code ignore;
                self->socket_.cancel(ignore);
            });

            socket_.async_connect(ep, [self, ip, was_live, attempt_done](error_code ec) {
                if (*attempt_done) {
                    self->on_attempt_failed(ip, was_live, net::error::timed_out);
                    return;
                }
                *attempt_done = true;
                self->timer_.cancel();

                if (!ec) {
                    self->owner_.last_used_ip_ = ip;
                    self->owner_.quarantine_.erase(ip);
                    self->handler_({}, std::move(self->socket_), ip);
                    return;
                }
                self->on_attempt_failed(ip, was_live, ec);
            });
        }

        void on_attempt_failed(const std::string &ip, bool was_live, error_code ec) {
            last_ec_ = ec;
            if (was_live) {
                if (remaining_live_ > 1) {
                    owner_.quarantine(ip, quarantine_ttl_);
                    --remaining_live_;
                }
            }
            try_next();
        }

        std::size_t remaining_live_at_start_marker() const noexcept {
            return owner_.live_count();
        }

        IpPinning                &owner_;
        net::io_context          &ioc_;
        std::vector<std::string>  candidates_;
        std::uint16_t             port_;
        std::chrono::milliseconds per_attempt_timeout_;
        std::chrono::seconds      quarantine_ttl_;
        IpPinning::ConnectHandler handler_;
        tcp::socket               socket_;
        net::steady_timer         timer_;
        std::size_t               idx_ = 0;
        std::size_t               remaining_live_;
        error_code                last_ec_;
    };

    inline void IpPinning::async_connect_pinned(std::vector<std::string>  candidates,
                                                std::uint16_t             port,
                                                std::chrono::milliseconds per_attempt_timeout,
                                                std::chrono::seconds      quarantine_ttl,
                                                ConnectHandler            handler) {
        auto op = std::make_shared<PinnedConnectOp>(*this, ioc_, std::move(candidates), port,
                                                    per_attempt_timeout, quarantine_ttl,
                                                    std::move(handler));
        op->start();
    }


} // namespace fsticker
