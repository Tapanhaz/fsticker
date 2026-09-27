// Copyright (c) 2026 Tapanhaz
// Part of fsticker :: https://github.com/Tapanhaz/fsticker
// Licensed under the Apache License, Version 2.0
// Created: 2026-09-25 10:41 IST

#pragma once

#include "async_queue_bridge.hpp"
#include "merge/tick.hpp"

namespace fsticker::pybridge {

    using AsyncTickBridge = AsyncQueueBridge<fsticker::merge::Tick>;

} // namespace fsticker::pybridge
