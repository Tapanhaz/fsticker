// Copyright (c) 2026 Tapanhaz
// Part of fsticker :: https://github.com/Tapanhaz/fsticker
// Licensed under the Apache License, Version 2.0
// Created: 2026-09-25 10:41 IST


#pragma once

#include "merge/schema.hpp"
#include "merge/tick.hpp"

#include <charconv>
#include <cmath>
#include <cstddef>
#include <ctime>
#include <optional>
#include <rapidjson/document.h>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>


#if !defined(__cpp_lib_to_chars)
#if __has_include(<fast_float/fast_float.h>)
#include <fast_float/fast_float.h>
#else
#error "fsticker: needs std::from_chars(double) (GCC>=11, MSVC, libc++>=20) or the fast_float header"
#endif
#endif

namespace fsticker::merge {
    namespace {

        [[nodiscard]] constexpr std::string_view strip_plus(std::string_view s) noexcept {
            if (s.size() > 1 && s.front() == '+' && s[1] != '-' && s[1] != '+')
                s.remove_prefix(1);
            return s;
        }

        [[nodiscard]] std::optional<std::int64_t> parse_i64(std::string_view s) noexcept {
            s = strip_plus(s);
            if (s.empty())
                return std::nullopt;
            const char *const end = s.data() + s.size();
            std::int64_t      out = 0;
            const auto [ptr, ec]  = std::from_chars(s.data(), end, out);
            if (ec != std::errc {} || ptr != end)
                return std::nullopt;
            return out;
        }

        [[nodiscard]] std::optional<double> parse_f64(std::string_view s) noexcept {
            s = strip_plus(s);
            if (s.empty())
                return std::nullopt;
            const char *const end = s.data() + s.size();
            double            out = 0.0;
#if defined(__cpp_lib_to_chars)
            const auto [ptr, ec] = std::from_chars(s.data(), end, out);
#else
            const auto [ptr, ec] = fast_float::from_chars(s.data(), end, out);
#endif
            if (ec != std::errc {} || ptr != end || !std::isfinite(out))
                return std::nullopt;
            return out;
        }

        // An integral double in int64 range ("12.0", 1e3), otherwise nothing.
        [[nodiscard]] std::optional<FieldValue> int_from_double(double d) noexcept {
            constexpr double kLimit = 9223372036854775808.0; // 2^63
            if (!(d >= -kLimit && d < kLimit) || d != std::trunc(d))
                return std::nullopt;
            return FieldValue {static_cast<std::int64_t>(d)};
        }

        [[nodiscard]] std::optional<FieldValue> int_from_text(std::string_view s) noexcept {
            if (const auto i = parse_i64(s))
                return FieldValue {*i};
            if (const auto d = parse_f64(s))
                return int_from_double(*d);
            return std::nullopt;
        }

        [[nodiscard]] std::optional<FieldValue> cast_value(const rapidjson::Value &v, FieldType type) {
            switch (type) {
                case FieldType::String:
                    if (v.IsString())
                        return FieldValue {std::string(v.GetString(), v.GetStringLength())};
                    if (v.IsInt64())
                        return FieldValue {std::to_string(v.GetInt64())};
                    if (v.IsBool())
                        return FieldValue {std::string(v.GetBool() ? "true" : "false")};
                    return std::nullopt;

                case FieldType::Int:
                    if (v.IsInt64())
                        return FieldValue {v.GetInt64()};
                    if (v.IsDouble())
                        return int_from_double(v.GetDouble());
                    if (v.IsString())
                        return int_from_text(std::string_view(v.GetString(), v.GetStringLength()));
                    return std::nullopt;

                case FieldType::Float:
                    if (v.IsDouble())
                        return std::isfinite(v.GetDouble())
                                   ? std::optional<FieldValue> {v.GetDouble()}
                                   : std::nullopt;
                    if (v.IsInt64())
                        return FieldValue {static_cast<double>(v.GetInt64())};
                    if (v.IsString())
                        if (const auto d =
                                parse_f64(std::string_view(v.GetString(), v.GetStringLength())))
                            return FieldValue {*d};
                    return std::nullopt;
            }
            return std::nullopt;
        }

        [[nodiscard]] std::int64_t now_seconds() noexcept {
            return static_cast<std::int64_t>(std::time(nullptr));
        }

    } // namespace

    class TickReconstructor {
    public:
        std::optional<Tick> on_message(const std::string &broker_name,
                                       const char        *data,
                                       std::size_t        size) {
            rapidjson::Document doc;
            doc.Parse(data, size);
            if (doc.HasParseError() || !doc.IsObject())
                return std::nullopt;

            const auto t_it = doc.FindMember("t");
            if (t_it == doc.MemberEnd() || !t_it->value.IsString())
                return std::nullopt;
            const std::string tag(t_it->value.GetString(), t_it->value.GetStringLength());

            const Schema &schema = schema_for_tag(tag);
            if (schema.empty())
                return std::nullopt;

            const auto get_str = [&](const char *name) -> std::string {
                const auto it = doc.FindMember(name);
                if (it != doc.MemberEnd() && it->value.IsString())
                    return std::string(it->value.GetString(), it->value.GetStringLength());
                return {};
            };
            const std::string e      = get_str("e");
            const std::string tk     = get_str("tk");
            const std::string key    = e + "|" + tk;
            const Family      family = family_for_tag(tag);

            auto slot_it = table_.find(key);
            if (slot_it == table_.end() || slot_it->second.family != family) {
                slot_it = table_.insert_or_assign(key, Slot {family, Tick {}}).first;
            }

            Tick                        scratch = slot_it->second.tick;
            std::optional<std::int64_t> ft_msg;
            for (auto m = doc.MemberBegin(); m != doc.MemberEnd(); ++m) {
                const std::string field(m->name.GetString(), m->name.GetStringLength());
                if (field == "t")
                    continue;
                const auto sch_it = schema.find(field);
                if (sch_it == schema.end())
                    continue;
                const auto value = cast_value(m->value, sch_it->second);
                if (field == "ft") {
                    if (value)
                        if (const auto *i = std::get_if<std::int64_t>(&*value))
                            ft_msg = *i;
                    continue;
                }
                if (value)
                    scratch[field] = *value;
                else
                    scratch.erase(field);
            }
            scratch["t"]      = tag;
            scratch["broker"] = broker_name;

            // Random mcx disaster :: no incoming ft -->
            const std::int64_t ft_in = ft_msg ? *ft_msg : now_seconds();

            const auto guard_it = ft_guard_.find(key);
            if (guard_it != ft_guard_.end() && ft_in < guard_it->second) {
                return std::nullopt;
            }

            scratch["ft"]        = ft_in;
            ft_guard_[key]       = ft_in;
            slot_it->second.tick = scratch;
            return scratch;
        }

        void forget(const std::string &instrument) {
            table_.erase(instrument);
            ft_guard_.erase(instrument);
        }

        [[nodiscard]] std::optional<Tick> last_snapshot(const std::string &exchange,
                                                        const std::string &token) const {
            const auto it = table_.find(exchange + "|" + token);
            if (it == table_.end())
                return std::nullopt;
            return it->second.tick;
        }

    private:
        struct Slot {
            Family family;
            Tick   tick;
        };

        std::unordered_map<std::string, Slot>         table_;
        std::unordered_map<std::string, std::int64_t> ft_guard_;
    };

} // namespace fsticker::merge
