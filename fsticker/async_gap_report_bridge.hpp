// Copyright (c) 2026 Tapanhaz
// Part of fsticker :: https://github.com/Tapanhaz/fsticker
// Licensed under the Apache License, Version 2.0

#pragma once

#include "async_queue_bridge.hpp"
#include "merge/sf_feed.hpp"

namespace fsticker::pybridge {
    using AsyncGapReportBridge = AsyncQueueBridge<fsticker::merge::CandleGapReport>;
} // namespace fsticker::pybridge