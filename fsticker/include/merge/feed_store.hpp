// Copyright (c) 2026 Tapanhaz
// Part of fsticker :: https://github.com/Tapanhaz/fsticker
// Licensed under the Apache License, Version 2.0
// Created: 2026-09-25 10:41 IST

#pragma once

#include "merge/tick.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>

namespace fsticker::merge {

    class FeedStore {
    public:
        FeedStore() = default;

        explicit FeedStore(bool single_broker) noexcept : single_broker_(single_broker) {
        }

        [[nodiscard]] std::optional<Tick> diff_and_update(Tick incoming) {
            if (single_broker_)
                return incoming;

            const std::string  key = get_string(incoming, "e") + "|" + get_string(incoming, "tk");
            const std::string  broker = get_string(incoming, "broker");
            const Family       family = family_for_tag(get_string(incoming, "t"));
            const std::int64_t ft_in  = get_int(incoming, "ft").value_or(0);
            const auto         fam    = static_cast<std::size_t>(family);

            Tick &known = last_emitted_[key];
            Tick  fresh;
            for (auto &[field, value] : incoming) {
                if (is_diff_excluded(field)) {
                    fresh[field] = value;
                    continue;
                }
                const auto kit = known.find(field);
                if (kit != known.end() && kit->second == value)
                    continue;
                fresh[field] = value;
            }
            if (std::none_of(fresh.begin(), fresh.end(),
                             [](const auto &kv) { return !is_diff_excluded(kv.first); }))
                return std::nullopt;

            std::optional<Tick> result;
            auto                it = table_.find(key);
            if (it == table_.end() || ft_in > it->second.ft) {
                Window &w = (it == table_.end()) ? table_[key] : it->second;
                w.ft      = ft_in;
                w.by_broker.clear();
                record_contribution(w, broker, fam, fresh);
                result = fresh;
            } else if (ft_in < it->second.ft) {
                result = std::nullopt;
            } else {
                Window &window = it->second;

                bool others_contributed = false;
                for (const auto &[b, sides] : window.by_broker) {
                    if (b != broker && !sides[fam].emitted.empty()) {
                        others_contributed = true;
                        break;
                    }
                }

                if (!others_contributed) {
                    record_contribution(window, broker, fam, fresh);
                    result = fresh;
                } else {
                    Ledger &mine = window.by_broker[broker][fam];
                    Tick    changed;
                    for (const auto &[field, value] : fresh) {
                        if (is_diff_excluded(field))
                            continue;
                        Key           k {field, value};
                        std::uint32_t others_emitted = 0;
                        for (const auto &[b, sides] : window.by_broker)
                            if (b != broker)
                                others_emitted += count_of(sides[fam].emitted, k);

                        if (others_emitted > count_of(mine.matched, k)) {
                            ++mine.matched[std::move(k)]; // already absorbed
                        } else {
                            changed[field] = value;
                            ++mine.emitted[std::move(k)];
                        }
                    }
                    if (!changed.empty()) {
                        for (const auto &[field, value] : fresh)
                            if (is_diff_excluded(field))
                                changed[field] = value;
                        result = std::move(changed);
                    }
                }
            }

            if (result)
                for (const auto &[field, value] : *result)
                    if (!is_diff_excluded(field))
                        known[field] = value;
            return result;
        }

        void forget(const std::string &instrument) {
            table_.erase(instrument);
            last_emitted_.erase(instrument);
        }


    private:
        struct ValueHash {
            std::size_t operator()(std::monostate) const noexcept {
                return 0;
            }

            std::size_t operator()(const std::string &s) const noexcept {
                return std::hash<std::string> {}(s);
            }

            std::size_t operator()(std::int64_t i) const noexcept {
                return std::hash<std::int64_t> {}(i);
            }

            std::size_t operator()(double d) const noexcept {
                return std::hash<double> {}(d == 0.0 ? 0.0 : d);
            }
        };

        struct Key {
            std::string field;
            FieldValue  value;

            bool operator==(const Key &o) const {
                return field == o.field && value == o.value;
            }
        };

        struct KeyHash {
            std::size_t operator()(const Key &k) const noexcept {
                const std::size_t h = std::hash<std::string> {}(k.field);
                const std::size_t v = std::visit(ValueHash {}, k.value);
                return h ^ (v + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2));
            }
        };

        using ValueCounts = std::unordered_map<Key, std::uint32_t, KeyHash>;

        struct Ledger {
            ValueCounts emitted;
            ValueCounts matched;
        };

        struct Window {
            std::int64_t                                 ft = 0;
            std::map<std::string, std::array<Ledger, 2>> by_broker;
        };

        [[nodiscard]] static std::uint32_t count_of(const ValueCounts &m, const Key &k) {
            const auto it = m.find(k);
            return it == m.end() ? 0u : it->second;
        }

        static void record_contribution(Window            &w,
                                        const std::string &broker,
                                        std::size_t        fam,
                                        const Tick        &t) {
            Ledger &l = w.by_broker[broker][fam];
            for (const auto &[field, value] : t)
                if (!is_diff_excluded(field))
                    ++l.emitted[Key {field, value}];
        }

        std::unordered_map<std::string, Window> table_;
        std::unordered_map<std::string, Tick>   last_emitted_;
        bool                                    single_broker_ = false;
    };

} // namespace fsticker::merge