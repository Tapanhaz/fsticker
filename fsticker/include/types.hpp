// Copyright (c) 2026 Tapanhaz
// Part of fsticker :: https://github.com/Tapanhaz/fsticker
// Licensed under the Apache License, Version 2.0
// Created: 2026-09-25 10:41 IST

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <string>
#include <string_view>

namespace fsticker {

    enum class FrameType : std::uint8_t {
        Text,
        Binary,
        Ping,
        Pong,

        Close
    };

    [[nodiscard]] constexpr std::string_view to_string(FrameType t) noexcept {
        switch (t) {
            case FrameType::Text:
                return "TEXT";
            case FrameType::Binary:
                return "BINARY";
            case FrameType::Ping:
                return "PING";
            case FrameType::Pong:
                return "PONG";
            case FrameType::Close:
                return "CLOSE";
        }
        return "UNKNOWN";
    }

    enum class AccessType : std::uint8_t { API, WEB, MOB };

    [[nodiscard]] constexpr std::string_view to_string(AccessType t) noexcept {
        switch (t) {
            case AccessType::API:
                return "API";
            case AccessType::WEB:
                return "WEB";
            case AccessType::MOB:
                return "MOB";
        }
        return "API";
    }

    [[nodiscard]] inline AccessType access_type_from_string(std::string_view s) {
        if (s == "WEB")
            return AccessType::WEB;
        if (s == "MOB")
            return AccessType::MOB;
        return AccessType::API;
    }

    enum class FeedType : std::uint8_t { Touchline, SnapQuote };

    enum LogLevel { kDebug = 0, kInfo = 1, kWarn = 2, kError = 3 };

    using RawFrameCallback = std::function<void(FrameType type, const char *data, std::size_t size)>;
    using DataCallback    = std::function<void(const char *data, std::size_t size)>;
    using ErrorCallback   = std::function<void(const char *data, std::size_t size)>;
    using OpenCallback    = std::function<void(const char *data, std::size_t size)>;
    using CloseCallback   = std::function<void(bool graceful)>;
    using StalledCallback = std::function<void(std::uint32_t consecutive_failures)>;
    using LogCallback     = std::function<void(int level, std::string_view message)>;

    inline void emit_log(int level, LogLevel min_level, const LogCallback &cb, std::string_view msg) {
        if (level < static_cast<int>(min_level))
            return;
        if (cb) {
            cb(level, msg);
            return;
        }
        static constexpr const char *kNames[] = {"DEBUG", "INFO", "WARN", "ERROR"};
        const char                  *name     = (level >= 0 && level < 4) ? kNames[level] : "LOG";
        std::fprintf(stderr, "[fsticker][%s] %.*s\n", name, static_cast<int>(msg.size()), msg.data());
    }

    struct Callbacks {
        DataCallback    subscribe_callback;
        DataCallback    order_update_callback;
        ErrorCallback   error_callback;
        OpenCallback    open_callback;
        CloseCallback   close_callback;
        StalledCallback stalled_callback;
        DataCallback    unsubscribe_callback;
        DataCallback    alert_callback;

        RawFrameCallback raw_frame_callback;

        LogCallback log_callback;
    };

    struct Credentials {
        std::string ws_endpoint;
        std::string user_id;
        std::string token;
        AccessType  access_type       = AccessType::API;
        bool        verify_ssl        = false;
        bool        enable_ip_pinning = false;
    };

} // namespace fsticker
