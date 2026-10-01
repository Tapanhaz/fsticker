// Copyright (c) 2026 Tapanhaz
// Part of fsticker :: https://github.com/Tapanhaz/fsticker
// Licensed under the Apache License, Version 2.0
// Created: 2026-09-25 10:41 IST

#pragma once

#include "merge/tick.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace fsticker::merge {

    struct TimeframeSpec {
        std::chrono::seconds period {60};
        bool                 live = false;

        bool                 auto_finalize = false;
        std::chrono::seconds auto_finalize_grace {4};
        bool                 omit_possible_partial = false;
    };

    using ExchangeAnchors = std::unordered_map<std::string, std::int64_t>;

    enum class CandleState : std::uint8_t { Partial, Complete };

    struct Candle {
        std::string  exchange;
        std::string  token;
        std::string  trading_symbol;
        std::int64_t period_seconds = 0;
        std::int64_t period_start   = 0;

        double open  = 0.0;
        double high  = 0.0;
        double low   = 0.0;
        double close = 0.0;

        std::int64_t volume        = 0;
        std::int64_t open_interest = 0;
        std::int64_t oi_delta      = 0;

        CandleState state  = CandleState::Complete;
        bool        live   = false;
        bool        is_new = false;
    };

    using CandleCallback = std::function<void(const std::vector<Candle> &)>;

    class CandleEngine {
    public:
        explicit CandleEngine(std::vector<TimeframeSpec> specs,
                              CandleCallback             on_candle,
                              ExchangeAnchors anchors = {}) : specs_(std::move(specs)),
                                                              on_candle_(std::move(on_candle)),
                                                              anchors_(std::move(anchors)) {
        }

        void on_tick(const Tick &tick) {
            if (specs_.empty() || !on_candle_)
                return;

            const auto ft = get_int(tick, "ft");
            if (!ft)
                return;

            const std::string tag    = get_string(tick, "t");
            const bool        is_ack = (tag == "tk" || tag == "dk");

            const std::string exchange = get_string(tick, "e");
            const std::string token    = get_string(tick, "tk");
            const std::string ts       = get_string(tick, "ts");
            const std::string key      = exchange + "|" + token;

            Instrument &inst = table_[key];
            if (inst.slots.size() != specs_.size())
                inst.slots.resize(specs_.size());
            inst.exchange = exchange;
            inst.token    = token;
            if (!ts.empty())
                inst.trading_symbol = ts;

            if (const auto lp = get_double(tick, "lp"); lp && *lp > 0.0)
                inst.last_price = *lp;
            if (const auto v = get_int(tick, "v"); v && *v > 0)
                inst.last_volume = *v;
            if (const auto oi = get_int(tick, "oi"); oi && *oi > 0)
                inst.last_oi = *oi;

            const std::optional<double> price =
                inst.last_price > 0.0 ? std::optional<double> {inst.last_price} : std::nullopt;

            const std::int64_t anchor = anchor_for(exchange);

            for (std::size_t i = 0; i < specs_.size(); ++i)
                update(key, inst.slots[i], specs_[i], i, exchange, token, inst.trading_symbol, *ft,
                       anchor, price, inst.last_volume, inst.last_oi, is_ack);
        }

        void check_late_candles(std::int64_t now_epoch_s) {
            while (!pending_finalize_.empty()) {
                auto it = pending_finalize_.begin();
                if (it->first > now_epoch_s)
                    break;
                PendingFinalize pf = std::move(it->second);
                pending_finalize_.erase(it);

                const auto inst_it = table_.find(pf.instrument_key);
                if (inst_it == table_.end() || pf.slot_index >= inst_it->second.slots.size())
                    continue;

                Instrument &inst = inst_it->second;
                Slot       &slot = inst.slots[pf.slot_index];
                if (!slot.current.initialized || slot.current.period_start != pf.expected_period_start)
                    continue;

                const TimeframeSpec &spec     = specs_[pf.slot_index];
                const bool           suppress = spec.omit_possible_partial && slot.suspect;
                if (!suppress)
                    on_candle_(
                        {make(inst.exchange, inst.token, inst.trading_symbol, spec.period.count(),
                              slot.current, CandleState::Complete, spec.live, false)});
                slot.previous       = slot.current;
                slot.previous_valid = false;
                slot.closed_through = slot.current.period_start;
                slot.suspect        = false;
                slot.current        = Bucket {};
            }
        }

        void forget(const std::string &instrument) {
            table_.erase(instrument);
        }

        struct OpenBucketInfo {
            std::size_t  slot_index;
            std::int64_t period_s;
            bool         is_open;
            std::int64_t period_start;
            std::int64_t closed_through;
            bool         omit_possible_partial;
        };

        [[nodiscard]] std::vector<OpenBucketInfo> open_buckets(const std::string &instrument_key) const {
            std::vector<OpenBucketInfo> out;
            const auto                  it = table_.find(instrument_key);
            if (it == table_.end())
                return out;
            const Instrument &inst = it->second;
            out.reserve(specs_.size());
            for (std::size_t i = 0; i < specs_.size() && i < inst.slots.size(); ++i) {
                const Slot &slot = inst.slots[i];
                out.push_back({i, specs_[i].period.count(), slot.current.initialized,
                               slot.current.period_start, slot.closed_through,
                               specs_[i].omit_possible_partial});
            }
            return out;
        }

        void mark_suspect(const std::string &instrument_key, std::size_t slot_index) {
            const auto it = table_.find(instrument_key);
            if (it == table_.end() || slot_index >= it->second.slots.size())
                return;
            Slot &slot = it->second.slots[slot_index];
            if (slot.current.initialized)
                slot.suspect = true;
            else
                slot.mark_next_open_suspect = true;
        }

        [[nodiscard]] static std::int64_t bucket_start(std::int64_t ft,
                                                       std::int64_t anchor,
                                                       std::int64_t period_s) noexcept {
            return period_start_for(ft, anchor, period_s);
        }

        [[nodiscard]] std::optional<std::int64_t> next_deadline() const {
            if (pending_finalize_.empty())
                return std::nullopt;
            return pending_finalize_.begin()->first;
        }

        [[nodiscard]] bool consume_earlier_deadline_flag() noexcept {
            const bool f      = earlier_deadline_;
            earlier_deadline_ = false;
            return f;
        }

    private:
        struct Bucket {
            std::int64_t period_start = 0;
            double       open = 0.0, high = 0.0, low = 0.0, close = 0.0;
            std::int64_t volume        = 0;
            std::int64_t open_interest = 0;
            std::int64_t oi_delta      = 0;
            std::int64_t last_volume   = 0;
            std::int64_t last_oi       = 0;
            bool         initialized   = false;
        };

        struct Slot {
            Bucket current;
            Bucket previous;
            bool   previous_valid = false;

            std::int64_t closed_through         = -1;
            bool         suspect                = false;
            bool         mark_next_open_suspect = false;
        };

        struct Instrument {
            std::string       exchange;
            std::string       token;
            std::string       trading_symbol;
            double            last_price  = 0.0;
            std::int64_t      last_volume = 0;
            std::int64_t      last_oi     = 0;
            std::vector<Slot> slots;
        };

        struct PendingFinalize {
            std::string  instrument_key;
            std::size_t  slot_index;
            std::int64_t expected_period_start;
        };

        [[nodiscard]] std::int64_t anchor_for(const std::string &exchange) const {
            const auto it = anchors_.find(exchange);
            return it == anchors_.end() ? 0 : it->second;
        }

        [[nodiscard]] static constexpr std::int64_t floor_div(std::int64_t a,
                                                              std::int64_t b) noexcept {
            std::int64_t       q = a / b;
            const std::int64_t r = a % b;
            if (r != 0 && ((r < 0) != (b < 0)))
                --q;
            return q;
        }

        [[nodiscard]] static std::int64_t period_start_for(std::int64_t ft,
                                                           std::int64_t anchor,
                                                           std::int64_t period_s) noexcept {
            constexpr std::int64_t kDaySeconds = 86400;
            const std::int64_t     day_start   = floor_div(ft, kDaySeconds) * kDaySeconds;
            const std::int64_t     session     = day_start + anchor;
            return session + floor_div(ft - session, period_s) * period_s;
        }

        void update(const std::string    &instrument_key,
                    Slot                 &slot,
                    const TimeframeSpec  &spec,
                    std::size_t           slot_index,
                    const std::string    &exchange,
                    const std::string    &token,
                    const std::string    &ts,
                    std::int64_t          ft,
                    std::int64_t          anchor,
                    std::optional<double> price,
                    std::int64_t          volume,
                    std::int64_t          oi,
                    bool                  is_ack) {
            const std::int64_t period_s = spec.period.count();
            if (period_s <= 0)
                return;
            const std::int64_t period_start = period_start_for(ft, anchor, period_s);

            if (period_start <= slot.closed_through)
                return;

            if (slot.current.initialized && period_start < slot.current.period_start)
                return;

            if (is_ack && !slot.current.initialized && slot.closed_through == -1)
                return;


            bool is_new_candle       = false;
            bool suppress_this_close = false;
            if (slot.current.initialized && period_start > slot.current.period_start) {
                suppress_this_close = spec.omit_possible_partial && slot.suspect;

                slot.previous       = slot.current;
                slot.previous_valid = true;
                slot.closed_through = slot.current.period_start;
                slot.current        = Bucket {};
                slot.suspect        = false;
                is_new_candle       = true;
            } else if (!slot.current.initialized) {
                slot.current.period_start = period_start;
            }

            if (price && *price > 0.0) {
                Bucket &c = slot.current;
                if (!c.initialized) {
                    c.open = c.high = c.low = c.close = *price;
                    c.period_start                    = period_start;
                    c.initialized                     = true;

                    is_new_candle = true;

                    if (slot.mark_next_open_suspect) {
                        slot.suspect                = true;
                        slot.mark_next_open_suspect = false;
                    }

                    if (spec.auto_finalize) {
                        const std::int64_t deadline =
                            period_start + period_s + spec.auto_finalize_grace.count();
                        if (pending_finalize_.empty() || deadline < pending_finalize_.begin()->first)
                            earlier_deadline_ = true;
                        pending_finalize_.emplace(
                            deadline, PendingFinalize {instrument_key, slot_index, period_start});
                    }
                } else {
                    c.high  = std::max(c.high, *price);
                    c.low   = std::min(c.low, *price);
                    c.close = *price;
                }

                const std::int64_t base_v =
                    volume >= slot.previous.last_volume ? slot.previous.last_volume : 0;
                c.volume        = volume - base_v;
                c.last_volume   = volume;
                c.open_interest = oi;
                c.oi_delta      = oi - slot.previous.last_oi;
                c.last_oi       = oi;
            }


            if (spec.live) {
                std::vector<Candle> msg;
                if (is_new_candle && slot.previous_valid && !suppress_this_close)
                    msg.push_back(make(exchange, token, ts, period_s, slot.previous,
                                       CandleState::Complete, true, false));
                if (slot.current.initialized)
                    msg.push_back(make(exchange, token, ts, period_s, slot.current,
                                       CandleState::Partial, true, is_new_candle));
                if (!msg.empty())
                    on_candle_(msg);
            } else if (is_new_candle && slot.previous_valid && !suppress_this_close) {

                on_candle_({make(exchange, token, ts, period_s, slot.previous,
                                 CandleState::Complete, false, false)});
            }
        }

        [[nodiscard]] static Candle make(const std::string &exchange,
                                         const std::string &token,
                                         const std::string &ts,
                                         std::int64_t       period_s,
                                         const Bucket      &b,
                                         CandleState        state,
                                         bool               live,
                                         bool               is_new) {

            Candle c;
            c.exchange       = exchange;
            c.token          = token;
            c.trading_symbol = ts;
            c.period_seconds = period_s;
            c.period_start   = b.period_start;
            c.open           = b.open;
            c.high           = b.high;
            c.low            = b.low;
            c.close          = b.close;
            c.volume         = b.volume;
            c.open_interest  = b.open_interest;
            c.oi_delta       = b.oi_delta;
            c.state          = state;
            c.live           = live;
            c.is_new         = is_new;
            return c;
        }

        std::vector<TimeframeSpec>                   specs_;
        CandleCallback                               on_candle_;
        ExchangeAnchors                              anchors_;
        std::unordered_map<std::string, Instrument>  table_;
        std::multimap<std::int64_t, PendingFinalize> pending_finalize_;
        bool                                         earlier_deadline_ = false;
    };

} // namespace fsticker::merge