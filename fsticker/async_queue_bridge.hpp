// Copyright (c) 2026 Tapanhaz
// Part of fsticker :: https://github.com/Tapanhaz/fsticker
// Licensed under the Apache License, Version 2.0
// Created: 2026-09-25 10:41 IST

#pragma once

#include <atomic>
#include <cstddef>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#else
#include <sys/socket.h>
#endif

namespace fsticker::pybridge {

    template <typename T> class AsyncQueueBridge {
    public:
        AsyncQueueBridge(int         notify_fd,
                         std::size_t capacity,
                         bool        overwrite = true) : notify_fd_(notify_fd),
                                                  buf_(capacity),
                                                  overwrite_(overwrite) {
        }

                bool push(T item) {
            bool accepted;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                accepted = push_locked(std::move(item));
            }
            notify();
            return accepted;
        }

        void begin_wait() {
            waiting_.store(1, std::memory_order_release);
        }

        void end_wait() {
            waiting_.store(0, std::memory_order_release);
        }

        [[nodiscard]] std::optional<T> try_pop() {
            std::lock_guard<std::mutex> lock(mutex_);
            if (size_ == 0)
                return std::nullopt;
            T item = std::move(buf_[head_]);
            head_  = (head_ + 1) % buf_.size();
            --size_;
            return item;
        }

        [[nodiscard]] std::size_t dropped() const noexcept {
            return dropped_.load(std::memory_order_relaxed);
        }

    private:
        bool push_locked(T item) {
            const std::size_t cap = buf_.size();
            if (size_ == cap) {
                if (!overwrite_) {
                    dropped_.fetch_add(1, std::memory_order_relaxed);
                    return false;
                }
                head_ = (head_ + 1) % cap; // drop oldest
                --size_;
                dropped_.fetch_add(1, std::memory_order_relaxed);
            }
            buf_[(head_ + size_) % cap] = std::move(item);
            ++size_;
            return true;
        }

        void notify() {
            unsigned char expected = 1;
            if (waiting_.compare_exchange_strong(expected, 0, std::memory_order_acq_rel)) {
                const char byte = 1;
                ::send(notify_fd_, &byte, 1, 0);
            }
        }

        int                        notify_fd_;
        std::atomic<unsigned char> waiting_ {0};
        std::atomic<std::size_t>   dropped_ {0};
        std::mutex                 mutex_;
        std::vector<T>             buf_;
        std::size_t                head_ = 0;
        std::size_t                size_ = 0;
        bool                       overwrite_;
    };

} // namespace fsticker::pybridge