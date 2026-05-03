#pragma once
// ============================================================================
// spsc_queue.hpp  —  Single-Producer Single-Consumer Lock-Free Ring Buffer
// ============================================================================
//
// Design goals (all relevant to HFT / CppCon audience):
//
//  1. ZERO locks — no mutex, no condition variable, no futex.
//     Producer and consumer never contend: each owns exactly one pointer.
//
//  2. Cache-line isolation — producer's head and consumer's tail live on
//     separate 64-byte cache lines. Without this, every write by the producer
//     invalidates the consumer's cache line (false sharing), adding ~100-200ns
//     of cross-core latency on a typical Xeon.
//
//  3. Acquire/release memory ordering — the minimum fencing required for
//     correctness on x86-64 and ARM. We deliberately avoid seq_cst, which
//     inserts a full memory fence (MFENCE on x86) on every operation.
//
//  4. Power-of-two capacity — allows index masking with bitwise AND instead
//     of modulo. Modulo requires an integer division; AND is one cycle.
//
//  5. Padding to capacity — the slot array is sized to exactly Capacity
//     elements. No dynamic allocation after construction. The whole queue
//     fits in a contiguous region that the OS can back with huge pages.
//
//  6. Tryput / tryget semantics — non-blocking. Callers decide what to do
//     when the queue is full/empty (spin, yield, back off). This keeps the
//     queue policy-free and composable.
//
// Usage:
//   spsc::Queue<OrderEvent, 4096> q;
//   // Producer thread:
//   while (!q.try_push(event)) { _mm_pause(); }
//   // Consumer thread:
//   OrderEvent e;
//   while (!q.try_pop(e)) { _mm_pause(); }
//
// Capacity must be a power of two. Static assert enforces this.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cassert>
#include <new>       // std::hardware_destructive_interference_size
#include <optional>
#include <type_traits>

namespace spsc {

// Cache line size. C++17 provides this as a hint; we use 64 as the x86 default.
#ifdef __cpp_lib_hardware_interference_size
    inline constexpr std::size_t kCacheLine =
        std::hardware_destructive_interference_size;
#else
    inline constexpr std::size_t kCacheLine = 64;
#endif

// ── Slot ─────────────────────────────────────────────────────────────────────
// Each slot in the ring buffer. The sequence_ field drives the handshake:
//   - sequence_ == index           → slot is ready to be written (empty)
//   - sequence_ == index + 1       → slot has been written, ready to read
//   - sequence_ == index + Capacity → slot has been read, wrapped around
//
// This sequence scheme avoids a separate "full/empty" flag and works
// correctly even when head/tail overflow (they're unsigned and wrap cleanly).
template<typename T>
struct Slot {
    alignas(kCacheLine) std::atomic<std::size_t> sequence_;
    T data_;

    Slot() = default;
    // Non-copyable, non-movable — slots live in a fixed array
    Slot(const Slot&) = delete;
    Slot& operator=(const Slot&) = delete;
};


// ── Queue ─────────────────────────────────────────────────────────────────────
template<typename T, std::size_t Capacity>
class Queue {
    static_assert(Capacity >= 2,               "Capacity must be >= 2");
    static_assert((Capacity & (Capacity-1)) == 0,
                  "Capacity must be a power of two (use 1024, 2048, 4096...)");
    static_assert(std::is_nothrow_move_constructible_v<T> ||
                  std::is_nothrow_copy_constructible_v<T>,
                  "T must be nothrow move- or copy-constructible");

public:
    using value_type = T;
    static constexpr std::size_t capacity = Capacity;

    Queue() noexcept {
        for (std::size_t i = 0; i < Capacity; ++i)
            slots_[i].sequence_.store(i, std::memory_order_relaxed);
        head_.store(0, std::memory_order_relaxed);
        tail_.store(0, std::memory_order_relaxed);
    }

    // Non-copyable — the queue is a unique resource
    Queue(const Queue&)            = delete;
    Queue& operator=(const Queue&) = delete;

    // ── try_push ─────────────────────────────────────────────────────────────
    // Called ONLY by the producer thread.
    // Returns true if the item was enqueued, false if the queue is full.
    //
    // Memory ordering:
    //   - head_ load: relaxed  — only the producer touches head_
    //   - sequence_ load: acquire — synchronise with the consumer's release
    //   - sequence_ store: release — publish the written slot to the consumer
    template<typename U>
    [[nodiscard]] bool try_push(U&& val) noexcept {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        Slot<T>& slot = slots_[head & kMask];

        // Has the consumer finished with this slot?
        const std::size_t seq = slot.sequence_.load(std::memory_order_acquire);
        const std::intptr_t diff = static_cast<std::intptr_t>(seq)
                                 - static_cast<std::intptr_t>(head);
        if (diff < 0) return false;  // queue is full
        if (diff > 0) {
            // Another producer got here first — impossible in SPSC, but
            // the check makes the logic self-documenting.
            return false;
        }

        // Claim the slot
        head_.store(head + 1, std::memory_order_relaxed);

        // Write the data
        if constexpr (std::is_move_constructible_v<T>)
            new (&slot.data_) T(std::forward<U>(val));
        else
            new (&slot.data_) T(val);

        // Publish: tell the consumer this slot is ready
        slot.sequence_.store(head + 1, std::memory_order_release);
        return true;
    }

    // ── try_pop ──────────────────────────────────────────────────────────────
    // Called ONLY by the consumer thread.
    // Returns true and fills `val` if an item was dequeued; false if empty.
    //
    // Memory ordering:
    //   - tail_ load: relaxed  — only the consumer touches tail_
    //   - sequence_ load: acquire — synchronise with the producer's release
    //   - sequence_ store: release — hand the slot back to the producer
    [[nodiscard]] bool try_pop(T& val) noexcept {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        Slot<T>& slot = slots_[tail & kMask];

        // Has the producer written to this slot?
        const std::size_t seq = slot.sequence_.load(std::memory_order_acquire);
        const std::intptr_t diff = static_cast<std::intptr_t>(seq)
                                 - static_cast<std::intptr_t>(tail + 1);
        if (diff < 0) return false;  // queue is empty

        // Consume the data
        tail_.store(tail + 1, std::memory_order_relaxed);
        val = std::move(slot.data_);
        slot.data_.~T();

        // Release: hand the slot back to the producer
        slot.sequence_.store(tail + Capacity, std::memory_order_release);
        return true;
    }

    // ── try_pop (optional variant) ────────────────────────────────────────────
    [[nodiscard]] std::optional<T> try_pop() noexcept {
        T val;
        if (try_pop(val)) return val;
        return std::nullopt;
    }

    // ── Approximate size ─────────────────────────────────────────────────────
    // Not exact — head/tail are read without synchronisation between them.
    // Safe to call from either thread for monitoring/stats; not for logic.
    std::size_t size_approx() const noexcept {
        const std::size_t h = head_.load(std::memory_order_acquire);
        const std::size_t t = tail_.load(std::memory_order_acquire);
        return (h >= t) ? (h - t) : (Capacity - (t - h));
    }

    bool empty_approx() const noexcept { return size_approx() == 0; }
    bool full_approx()  const noexcept { return size_approx() >= Capacity - 1; }

private:
    static constexpr std::size_t kMask = Capacity - 1;

    // ── Cache-line separation ─────────────────────────────────────────────────
    // head_ is written exclusively by the producer.
    // tail_ is written exclusively by the consumer.
    // If they shared a cache line, every store by one thread would force
    // the other thread to re-fetch the line (MESI protocol Invalid state),
    // adding ~100-200ns of inter-core latency per operation.
    alignas(kCacheLine) std::atomic<std::size_t> head_{0};
    alignas(kCacheLine) std::atomic<std::size_t> tail_{0};

    // Slot array — contiguous, no heap allocation after construction
    alignas(kCacheLine) Slot<T> slots_[Capacity];
};


// ── SpinPush / SpinPop helpers ────────────────────────────────────────────────
// Spin with _mm_pause (PAUSE instruction) to reduce power and bus traffic
// while waiting. PAUSE tells the CPU this is a spin-wait loop, allowing it
// to avoid memory-order violation penalties on TSX and improve SMT efficiency.

template<typename Q, typename T>
inline void spin_push(Q& q, T&& val) noexcept {
    while (!q.try_push(std::forward<T>(val))) {
#if defined(__x86_64__) || defined(_M_X64)
        __builtin_ia32_pause();
#else
        std::atomic_thread_fence(std::memory_order_seq_cst);
#endif
    }
}

template<typename Q, typename T>
inline void spin_pop(Q& q, T& val) noexcept {
    while (!q.try_pop(val)) {
#if defined(__x86_64__) || defined(_M_X64)
        __builtin_ia32_pause();
#else
        std::atomic_thread_fence(std::memory_order_seq_cst);
#endif
    }
}

} // namespace spsc
