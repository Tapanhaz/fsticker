// Copyright (c) 2026 Tapanhaz
// Part of fsticker :: https://github.com/Tapanhaz/fsticker
// Licensed under the Apache License, Version 2.0
// Created: 2026-09-25 10:41 IST

#pragma once

#include "async_queue_bridge.hpp"
#include "merge/candle_engine.hpp"

namespace fsticker::pybridge {
    using AsyncCandleBridge = AsyncQueueBridge<std::vector<fsticker::merge::Candle>>;
} // namespace fsticker::pybridge