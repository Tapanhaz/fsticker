// Copyright (c) 2026 Tapanhaz
// Part of fsticker :: https://github.com/Tapanhaz/fsticker
// Licensed under the Apache License, Version 2.0
// Created: 2026-09-25 10:41 IST

#pragma once

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <random>

namespace fsticker {

    class Backoff {
    public:
        struct Params {
            std::chrono::duration<double> min_backoff {0.25};
            std::chrono::duration<double> max_backoff {5.0};
            double                        multiplier      = 2.0;
            double                        jitter_fraction = 0.3;
        };

        explicit Backoff(Params params) : params_(params),
                                          rng_(std::random_device {}()) {}

        [[nodiscard]] std::chrono::duration<double> next_delay() {
            ++consecutive_failures_;
            const double n   = static_cast<double>(consecutive_failures_);
            const double raw = params_.min_backoff.count() * std::pow(params_.multiplier, n - 1.0);
            const double capped = std::min(params_.max_backoff.count(), raw);
            const double jitter = capped * params_.jitter_fraction;
            std::uniform_real_distribution<double> dist(-jitter, jitter);
            const double                           delayed = std::max(0.0, capped + dist(rng_));
            return std::chrono::duration<double>(delayed);
        }

        void reset() noexcept { consecutive_failures_ = 0; }

        [[nodiscard]] std::uint32_t consecutive_failures() const noexcept {
            return consecutive_failures_;
        }

    private:
        Params        params_;
        std::mt19937  rng_;
        std::uint32_t consecutive_failures_ = 0;
    };

} // namespace fsticker
