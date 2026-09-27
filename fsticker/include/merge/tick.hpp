// Copyright (c) 2026 Tapanhaz
// Part of fsticker :: https://github.com/Tapanhaz/fsticker
// Licensed under the Apache License, Version 2.0
// Created: 2026-09-25 10:41 IST

#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <variant>

namespace fsticker::merge {

    enum class Family : std::uint8_t { Touchline, Depth };

    enum class FieldType : std::uint8_t { String, Int, Float };

    using FieldValue = std::variant<std::monostate, std::string, std::int64_t, double>;
    using Tick       = std::map<std::string, FieldValue>;

    [[nodiscard]] inline Family family_for_tag(const std::string &tag) {
        return (tag == "tf" || tag == "tk") ? Family::Touchline : Family::Depth;
    }

    inline const std::set<std::string> kDiffExclude {"t", "e", "tk", "broker", "ft", "ts"};

    [[nodiscard]] inline std::set<std::string> &extra_diff_exclude() {
        static std::set<std::string> fields {"ml"};
        return fields;
    }

    [[nodiscard]] inline bool is_diff_excluded(const std::string &field) {
        return kDiffExclude.count(field) != 0 || extra_diff_exclude().count(field) != 0;
    }

    [[nodiscard]] inline std::string get_string(const Tick        &t,
                                                const std::string &field,
                                                std::string        fallback = {}) {
        auto it = t.find(field);
        if (it == t.end())
            return fallback;
        if (const auto *s = std::get_if<std::string>(&it->second))
            return *s;
        return fallback;
    }

    [[nodiscard]] inline std::optional<std::int64_t> get_int(const Tick &t, const std::string &field) {
        const auto it = t.find(field);
        if (it == t.end())
            return std::nullopt;
        if (const auto *v = std::get_if<std::int64_t>(&it->second))
            return *v;
        return std::nullopt;
    }

    [[nodiscard]] inline std::optional<double> get_double(const Tick &t, const std::string &field) {
        auto it = t.find(field);
        if (it == t.end())
            return std::nullopt;
        if (const auto *v = std::get_if<double>(&it->second))
            return *v;
        return std::nullopt;
    }


} // namespace fsticker::merge
