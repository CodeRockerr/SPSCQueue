#pragma once
// ============================================================================
// market_feed.hpp  —  SPSC-backed Market Data Feed Pipeline
// ============================================================================
//
// Connects the SPSC queue to the FastBook matching engine:
//
//   [NIC / mmap ring] → FeedParser → SPSC Queue → MatchingEngine
//
// The SPSC queue decouples the feed parser (producer, typically pinned to
// core 0) from the matching engine (consumer, pinned to core 1).
// Zero locks cross the boundary — the queue IS the synchronisation.
//
// In production HFT systems this pattern is called a "disruptor" or
// "ring buffer pipeline". This is the simplified single-consumer version.

#include "spsc_queue.hpp"
#include <cstdint>
#include <cstring>

namespace spsc {

// ── OrderEvent ───────────────────────────────────────────────────────────────
// The message type flowing through the queue.
// Kept to 64 bytes (one cache line) so each slot in the SPSC queue
// holds exactly one event without straddling cache lines.
enum class EventType : uint8_t {
    ADD    = 0,
    CANCEL = 1,
    FILL   = 2,
};

enum class Side : uint8_t { BUY = 0, SELL = 1 };

struct alignas(64) OrderEvent {
    EventType type;
    Side      side;
    uint8_t   _pad0[2];
    uint32_t  qty;
    int64_t   price;      // ticks (price * 100)
    uint64_t  order_id;
    uint64_t  timestamp;  // nanoseconds since epoch (from rdtsc)
    uint8_t   _pad1[32];  // pad to 64 bytes
};
static_assert(sizeof(OrderEvent) == 64, "OrderEvent must be one cache line");


// ── FeedQueue ────────────────────────────────────────────────────────────────
// Concrete queue type for the market data pipeline.
// 4096 slots = 4096 * 64 bytes = 256 KB — fits in L2 cache on most CPUs.
using FeedQueue = Queue<OrderEvent, 4096>;


// ── FeedProducer ─────────────────────────────────────────────────────────────
// Simulates a market data feed parser running on the producer thread.
// In production: replace the generate() body with a mmap'd NIC ring buffer read.
class FeedProducer {
public:
    explicit FeedProducer(FeedQueue& q) : q_(q) {}

    // Push an ADD event. Returns false if queue is full (caller backs off).
    [[nodiscard]] bool push_add(Side side, int64_t price, uint32_t qty,
                                uint64_t order_id, uint64_t ts) noexcept {
        OrderEvent ev{};
        ev.type     = EventType::ADD;
        ev.side     = side;
        ev.price    = price;
        ev.qty      = qty;
        ev.order_id = order_id;
        ev.timestamp = ts;
        return q_.try_push(ev);
    }

    [[nodiscard]] bool push_cancel(uint64_t order_id, uint64_t ts) noexcept {
        OrderEvent ev{};
        ev.type      = EventType::CANCEL;
        ev.order_id  = order_id;
        ev.timestamp = ts;
        return q_.try_push(ev);
    }

    std::size_t queue_depth() const { return q_.size_approx(); }

private:
    FeedQueue& q_;
};


// ── FeedConsumer ─────────────────────────────────────────────────────────────
// Runs on the consumer (matching engine) thread.
// Drains events from the queue and dispatches them.
class FeedConsumer {
public:
    using AddHandler    = void(*)(const OrderEvent&, void*);
    using CancelHandler = void(*)(const OrderEvent&, void*);

    explicit FeedConsumer(FeedQueue& q) : q_(q) {}

    void set_add_handler   (AddHandler    h, void* ctx) { on_add_    = h; add_ctx_    = ctx; }
    void set_cancel_handler(CancelHandler h, void* ctx) { on_cancel_ = h; cancel_ctx_ = ctx; }

    // Drain up to `max_events` from the queue. Returns number processed.
    // Call this in a tight loop on the consumer thread.
    std::size_t drain(std::size_t max_events = 256) noexcept {
        std::size_t count = 0;
        OrderEvent ev;
        while (count < max_events && q_.try_pop(ev)) {
            switch (ev.type) {
                case EventType::ADD:
                    ++adds_;
                    if (on_add_) on_add_(ev, add_ctx_);
                    break;
                case EventType::CANCEL:
                    ++cancels_;
                    if (on_cancel_) on_cancel_(ev, cancel_ctx_);
                    break;
                default:
                    ++drops_;
                    break;
            }
            ++count;
        }
        processed_ += count;
        return count;
    }

    uint64_t processed() const { return processed_; }
    uint64_t adds()      const { return adds_; }
    uint64_t cancels()   const { return cancels_; }
    uint64_t drops()     const { return drops_; }

private:
    FeedQueue& q_;
    AddHandler    on_add_    = nullptr;
    CancelHandler on_cancel_ = nullptr;
    void*         add_ctx_    = nullptr;
    void*         cancel_ctx_ = nullptr;
    uint64_t processed_ = 0, adds_ = 0, cancels_ = 0, drops_ = 0;
};

} // namespace spsc
