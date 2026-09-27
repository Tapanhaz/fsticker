// Copyright (c) 2026 Tapanhaz
// Part of fsticker :: https://github.com/Tapanhaz/fsticker
// Licensed under the Apache License, Version 2.0
// Created: 2026-09-25 10:41 IST

#pragma once

#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <new>
#include <system_error>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#else
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <unistd.h>
#endif

namespace fsticker::detail {

    inline std::atomic<int>            g_caught {0};
    inline std::atomic<bool>           g_scope_active {false};
    inline std::atomic<std::uintptr_t> g_chain_fn[2] {0, 0};
    static_assert(std::atomic<int>::is_always_lock_free);
    static_assert(std::atomic<std::uintptr_t>::is_always_lock_free);

    [[nodiscard]] constexpr int signal_index(int sig) noexcept {
        return sig == SIGINT ? 0 : 1;
    }

#ifdef _WIN32
    inline std::atomic<void *>      g_signal_event {nullptr};
    inline constexpr std::uintptr_t kChainPending = 1;
#else
    inline std::atomic<int>  g_signal_wfd {-1};
    inline std::atomic<bool> g_chain_siginfo[2] {false, false};

    inline bool make_pipe(int (&fds)[2]) noexcept {
#if defined(__linux__)
        return ::pipe2(fds, O_NONBLOCK | O_CLOEXEC) == 0;
#else
        if (::pipe(fds) != 0)
            return false;
        for (int fd : fds) {
            ::fcntl(fd, F_SETFD, FD_CLOEXEC);
            ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL) | O_NONBLOCK);
        }
        return true;
#endif
    }

    inline void drain_fd(int fd) noexcept {
        char buf[64];
        while (::read(fd, buf, sizeof buf) > 0) {}
    }
#endif

    class SignalChannel {
    public:
        [[nodiscard]] static SignalChannel *get() noexcept {
            static SignalChannel *const inst = create();
            return inst;
        }

#ifdef _WIN32
        [[nodiscard]] void *event() const noexcept {
            return ev_;
        }
#else
        [[nodiscard]] int read_fd() const noexcept {
            return fds_[0];
        }

        void drain() noexcept {
            drain_fd(fds_[0]);
        }
#endif

    private:
        SignalChannel() noexcept {
#ifdef _WIN32
            ev_ = ::CreateEventW(nullptr, FALSE, FALSE, nullptr);
            ok_ = ev_ != nullptr;
            if (ok_)
                g_signal_event.store(ev_, std::memory_order_release);
#else
            ok_ = make_pipe(fds_);
            if (ok_)
                g_signal_wfd.store(fds_[1], std::memory_order_release);
#endif
        }

        static SignalChannel *create() noexcept {
            auto *c = new (std::nothrow) SignalChannel;
            if (c && !c->ok_) {
                delete c;
                return nullptr;
            }
            return c;
        }

#ifdef _WIN32
        void *ev_ = nullptr;
#else
        int fds_[2] {-1, -1};
#endif
        bool ok_ = false;
    };

} // namespace fsticker::detail

#ifdef _WIN32
extern "C" inline void fsticker_signal_handler(int sig) {
    using namespace fsticker::detail;
    std::signal(sig, fsticker_signal_handler);
    const int      i = signal_index(sig);
    std::uintptr_t fn;
    while ((fn = g_chain_fn[i].load(std::memory_order_acquire)) == kChainPending)
        ::SwitchToThread();
    if (fn) {
        reinterpret_cast<void (*)(int)>(fn)(sig);
    } else {
        int expected = 0;
        g_caught.compare_exchange_strong(expected, sig, std::memory_order_relaxed);
    }
    if (void *ev = g_signal_event.load(std::memory_order_acquire))
        ::SetEvent(ev);
}
#else


extern "C" inline void fsticker_signal_handler(int sig, siginfo_t *info, void *ctx) {
    using namespace fsticker::detail;
    const int saved_errno = errno;
    const int i           = signal_index(sig);
    if (const std::uintptr_t fn = g_chain_fn[i].load(std::memory_order_acquire)) {
        if (g_chain_siginfo[i].load(std::memory_order_relaxed))
            reinterpret_cast<void (*)(int, siginfo_t *, void *)>(fn)(sig, info, ctx);
        else
            reinterpret_cast<void (*)(int)>(fn)(sig);
    } else {
        int expected = 0;
        g_caught.compare_exchange_strong(expected, sig, std::memory_order_relaxed);
    }
    const int fd = g_signal_wfd.load(std::memory_order_acquire);
    if (fd >= 0) {
        const char                  b = 1;
        [[maybe_unused]] const auto r = ::write(fd, &b, 1); // EAGAIN
    }
    errno = saved_errno;
}
#endif

namespace fsticker {

    class Waker {
    public:
#ifdef _WIN32
        Waker() : ev_(::CreateEventW(nullptr, FALSE, FALSE, nullptr)) {
            if (!ev_)
                throw std::system_error(static_cast<int>(::GetLastError()), std::system_category(),
                                        "fsticker: CreateEvent");
        }

        ~Waker() {
            ::CloseHandle(ev_);
        }

        void notify() noexcept {
            ::SetEvent(ev_);
        }

        void wait(int timeout_ms, bool watch_signals) noexcept {
            HANDLE h[2] = {ev_, nullptr};
            DWORD  n    = 1;
            if (watch_signals)
                if (auto *sc = detail::SignalChannel::get()) {
                    h[1] = sc->event();
                    n    = 2;
                }
            ::WaitForMultipleObjects(n, h, FALSE,
                                     timeout_ms < 0 ? INFINITE : static_cast<DWORD>(timeout_ms));
        }

    private:
        HANDLE ev_;
#else
        Waker() {
            if (!detail::make_pipe(fds_))
                throw std::system_error(errno, std::generic_category(), "fsticker: pipe");
        }

        ~Waker() {
            ::close(fds_[0]);
            ::close(fds_[1]);
        }

        void notify() noexcept {
            const int                   saved = errno;
            const char                  b     = 1;
            [[maybe_unused]] const auto r     = ::write(fds_[1], &b, 1);
            errno                             = saved;
        }

        void wait(int timeout_ms, bool watch_signals) noexcept {
            struct pollfd p[2] = {
                {fds_[0], POLLIN, 0},
                {     -1, POLLIN, 0}
            };
            nfds_t n  = 1;
            auto  *sc = watch_signals ? detail::SignalChannel::get() : nullptr;
            if (sc) {
                p[1].fd = sc->read_fd();
                n       = 2;
            }
            ::poll(p, n, timeout_ms);
            detail::drain_fd(fds_[0]);
            if (sc)
                sc->drain();
        }

    private:
        int fds_[2] {-1, -1};
#endif

    public:
        Waker(const Waker &)            = delete;
        Waker &operator=(const Waker &) = delete;
    };

    enum class SignalMode { None, Own, Chain };

    class SignalScope {
    public:
        explicit SignalScope(SignalMode mode) noexcept : mode_(mode) {
            if (mode == SignalMode::None)
                return;

            bool expected = false;

            if (!detail::g_scope_active.compare_exchange_strong(expected, true))
                return;
            if (!detail::SignalChannel::get()) {
                detail::g_scope_active.store(false);
                return;
            }
            owns_ = true;
            if (mode == SignalMode::Own)
                detail::g_caught.store(0, std::memory_order_relaxed);
            for (int i = 0; i < 2; ++i)
                install(i);
        }

        ~SignalScope() {
            if (!owns_)
                return;
            for (int i = 0; i < 2; ++i)
                if (installed_[i])
                    uninstall(i);
            detail::g_scope_active.store(false);
        }

        SignalScope(const SignalScope &)            = delete;
        SignalScope &operator=(const SignalScope &) = delete;

        [[nodiscard]] bool active() const noexcept {
            return owns_;
        }

        [[nodiscard]] static int caught() noexcept {
            return detail::g_caught.load(std::memory_order_relaxed);
        }

    private:
        void install(int i) noexcept {
            using namespace detail;
            const int sig = kSigs[i];
#ifdef _WIN32
            if (mode_ == SignalMode::Chain)
                g_chain_fn[i].store(kChainPending, std::memory_order_release);
            void (*prev)(int) = std::signal(sig, fsticker_signal_handler);
            if (prev == SIG_ERR) {
                g_chain_fn[i].store(0, std::memory_order_release);
                return;
            }
            if (prev == SIG_IGN || (mode_ == SignalMode::Chain && prev == SIG_DFL)) {
                std::signal(sig, prev);
                g_chain_fn[i].store(0, std::memory_order_release);
                return;
            }
            prev_[i] = prev;
            if (mode_ == SignalMode::Chain)
                g_chain_fn[i].store(reinterpret_cast<std::uintptr_t>(prev),
                                    std::memory_order_release);
#else
            struct sigaction cur {};
            if (::sigaction(sig, nullptr, &cur) != 0)
                return;
            const bool siginfo = (cur.sa_flags & SA_SIGINFO) != 0;
            if (!siginfo && cur.sa_handler == SIG_IGN)
                return; // nohup
            const std::uintptr_t fn = siginfo ? reinterpret_cast<std::uintptr_t>(cur.sa_sigaction)
                                              : reinterpret_cast<std::uintptr_t>(cur.sa_handler);
            const bool is_fn = siginfo ? cur.sa_sigaction != nullptr : cur.sa_handler != SIG_DFL;
            if (mode_ == SignalMode::Chain) {
                if (!is_fn)
                    return;
                g_chain_siginfo[i].store(siginfo, std::memory_order_relaxed);
                g_chain_fn[i].store(fn, std::memory_order_release);
            }
            struct sigaction sa {};
            sa.sa_sigaction = fsticker_signal_handler;
            sigemptyset(&sa.sa_mask);
            sa.sa_flags =
                SA_SIGINFO | (mode_ == SignalMode::Own ? SA_RESTART : (cur.sa_flags & SA_RESTART));
            if (::sigaction(sig, &sa, &prev_[i]) != 0) {
                g_chain_fn[i].store(0, std::memory_order_release);
                return;
            }
#endif
            installed_[i] = true;
        }

        void uninstall(int i) noexcept {
#ifdef _WIN32
            std::signal(kSigs[i], prev_[i]);
#else
            ::sigaction(kSigs[i], &prev_[i], nullptr);
#endif
            detail::g_chain_fn[i].store(0, std::memory_order_release);
            installed_[i] = false;
        }

        static constexpr int kSigs[2] = {SIGINT, SIGTERM};
#ifdef _WIN32
        void (*prev_[2])(int) {};
#else
        struct sigaction prev_[2] {};
#endif
        bool       installed_[2] {};
        bool       owns_ = false;
        SignalMode mode_;
    };

} // namespace fsticker