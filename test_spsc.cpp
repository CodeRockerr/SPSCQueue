#include <cstdarg>
#include "spsc_queue.hpp"
#include "market_feed.hpp"
#include <unistd.h>
#include <cstring>
#include <cstdio>
#include <thread>
#include <atomic>
#include <vector>
#include <numeric>
#include <stdexcept>
#include <string>

// ── Output via write() to avoid stdio buffering hang ─────────────────────────
static void say(const char* s) { ::write(1, s, strlen(s)); ::write(1,"\n",1); }
static void sayf(const char* fmt, ...) {
    char buf[256]; va_list ap; va_start(ap,fmt);
    int n = vsnprintf(buf,sizeof(buf),fmt,ap); va_end(ap);
    ::write(1,buf,n);
}

static int g_pass = 0, g_fail = 0;

#define TEST(name) static void test_##name()
#define RUN(name) do { \
    say("  " #name); \
    try { test_##name(); say("    -> PASS"); ++g_pass; } \
    catch(const std::exception& e){ sayf("    -> FAIL: %s\n",e.what()); ++g_fail; } \
} while(0)
#define EXPECT(c) do { if(!(c)) throw std::runtime_error("EXPECT(" #c ") line " + std::to_string(__LINE__)); } while(0)


// ── Basic correctness ─────────────────────────────────────────────────────────

TEST(capacity_is_power_of_two) {
    spsc::Queue<int,   16> q16;
    spsc::Queue<int,  256> q256;
    spsc::Queue<int, 4096> q4096;
    EXPECT(q16.capacity   ==   16);
    EXPECT(q256.capacity  ==  256);
    EXPECT(q4096.capacity == 4096);
}

TEST(empty_on_construction) {
    spsc::Queue<int, 8> q;
    EXPECT(q.empty_approx());
    EXPECT(q.size_approx() == 0);
}

TEST(try_pop_empty_returns_false) {
    spsc::Queue<int, 8> q;
    int v = 0;
    EXPECT(!q.try_pop(v));
}

TEST(single_push_pop) {
    spsc::Queue<int, 8> q;
    EXPECT(q.try_push(42));
    int v = 0;
    EXPECT(q.try_pop(v));
    EXPECT(v == 42);
    EXPECT(q.empty_approx());
}

TEST(fifo_ordering) {
    spsc::Queue<int, 8> q;
    for (int i = 0; i < 5; ++i) EXPECT(q.try_push(i));
    for (int i = 0; i < 5; ++i) {
        int v = -1;
        EXPECT(q.try_pop(v));
        EXPECT(v == i);
    }
}

TEST(full_queue_rejects_push) {
    spsc::Queue<int, 4> q;
    // Sequence-based design: all Capacity slots are usable
    EXPECT(q.try_push(1));
    EXPECT(q.try_push(2));
    EXPECT(q.try_push(3));
    EXPECT(q.try_push(4));
    EXPECT(!q.try_push(5));  // now truly full
}

TEST(wraparound_correctness) {
    spsc::Queue<int, 4> q;
    // Fill, drain, fill again — exercises the index wrap
    for (int round = 0; round < 8; ++round) {
        for (int i = 0; i < 3; ++i) EXPECT(q.try_push(round * 10 + i));
        for (int i = 0; i < 3; ++i) {
            int v; EXPECT(q.try_pop(v));
            EXPECT(v == round * 10 + i);
        }
    }
}

TEST(optional_pop_variant) {
    spsc::Queue<int, 8> q;
    q.try_push(99);
    auto opt = q.try_pop();
    EXPECT(opt.has_value());
    EXPECT(*opt == 99);
    EXPECT(!q.try_pop().has_value());
}

TEST(move_semantics) {
    spsc::Queue<std::vector<int>, 8> q;
    std::vector<int> v = {1, 2, 3, 4, 5};
    EXPECT(q.try_push(std::move(v)));
    EXPECT(v.empty()); // moved-from
    std::vector<int> out;
    EXPECT(q.try_pop(out));
    EXPECT(out.size() == 5);
    EXPECT(out[0] == 1 && out[4] == 5);
}

TEST(cache_line_separation) {
    spsc::Queue<int, 16> q;
    // head_ and tail_ must be on different cache lines
    const uint8_t* base = reinterpret_cast<const uint8_t*>(&q);
    // We can't directly access private members, but we verify the sizeof
    // is at least 3 cache lines (head + tail + at least one slot)
    EXPECT(sizeof(q) >= 3 * spsc::kCacheLine);
}


// ── Struct types ──────────────────────────────────────────────────────────────

TEST(order_event_is_cache_line_sized) {
    EXPECT(sizeof(spsc::OrderEvent) == 64);
    EXPECT(alignof(spsc::OrderEvent) == 64);
}

TEST(push_pop_order_event) {
    spsc::FeedQueue q;
    spsc::OrderEvent ev{};
    ev.type     = spsc::EventType::ADD;
    ev.side     = spsc::Side::BUY;
    ev.price    = 17542;
    ev.qty      = 500;
    ev.order_id = 77;

    EXPECT(q.try_push(ev));
    spsc::OrderEvent out{};
    EXPECT(q.try_pop(out));
    EXPECT(out.type     == spsc::EventType::ADD);
    EXPECT(out.price    == 17542);
    EXPECT(out.qty      == 500);
    EXPECT(out.order_id == 77);
}


// ── Multi-threaded correctness ────────────────────────────────────────────────

TEST(threaded_producer_consumer_1M) {
    // Producer sends 1,000,000 sequential integers.
    // Consumer checks they arrive in order with no gaps.
    spsc::Queue<uint64_t, 4096> q;
    constexpr uint64_t N = 1'000'000;
    std::atomic<bool> done{false};
    std::atomic<uint64_t> errors{0};

    std::thread producer([&]{
        for (uint64_t i = 0; i < N; ++i)
            spsc::spin_push(q, i);
    });

    std::thread consumer([&]{
        uint64_t expected = 0;
        while (expected < N) {
            uint64_t val;
            if (q.try_pop(val)) {
                if (val != expected++) ++errors;
            } else {
#if defined(__x86_64__)
                __builtin_ia32_pause();
#endif
            }
        }
    });

    producer.join();
    consumer.join();
    EXPECT(errors.load() == 0);
}

TEST(threaded_no_data_loss_100k_structs) {
    spsc::FeedQueue q;
    constexpr int N = 100'000;
    std::atomic<uint64_t> sum_sent{0}, sum_recv{0};

    std::thread prod([&]{
        for (int i = 0; i < N; ++i) {
            spsc::OrderEvent ev{};
            ev.order_id = static_cast<uint64_t>(i);
            ev.price    = 17500 + (i % 100);
            ev.qty      = static_cast<uint32_t>(i % 1000 + 1);
            sum_sent += ev.order_id;
            spsc::spin_push(q, ev);
        }
    });

    std::thread cons([&]{
        int received = 0;
        while (received < N) {
            spsc::OrderEvent ev;
            if (q.try_pop(ev)) {
                sum_recv += ev.order_id;
                ++received;
            }
        }
    });

    prod.join();
    cons.join();
    EXPECT(sum_sent.load() == sum_recv.load());
}


// ── FeedProducer / FeedConsumer integration ───────────────────────────────────

TEST(feed_pipeline_add_events) {
    spsc::FeedQueue q;
    spsc::FeedProducer prod(q);
    spsc::FeedConsumer cons(q);

    std::atomic<int> add_count{0};
    cons.set_add_handler([](const spsc::OrderEvent&, void* ctx){
        ++(*static_cast<std::atomic<int>*>(ctx));
    }, &add_count);

    for (int i = 0; i < 10; ++i)
        EXPECT(prod.push_add(spsc::Side::BUY, 17500+i, 100, i, 0));

    cons.drain(20);
    EXPECT(add_count.load() == 10);
    EXPECT(cons.processed() == 10);
    EXPECT(cons.adds()      == 10);
}

TEST(feed_pipeline_cancel_events) {
    spsc::FeedQueue q;
    spsc::FeedProducer prod(q);
    spsc::FeedConsumer cons(q);

    std::atomic<int> cancel_count{0};
    cons.set_cancel_handler([](const spsc::OrderEvent&, void* ctx){
        ++(*static_cast<std::atomic<int>*>(ctx));
    }, &cancel_count);

    for (int i = 0; i < 5; ++i)
        EXPECT(prod.push_cancel(i, 0));

    cons.drain();
    EXPECT(cancel_count.load() == 5);
    EXPECT(cons.cancels() == 5);
}

// ─────────────────────────────────────────────────────────────────────────────
int main() {
    say("\n==============================================");
    say("        SPSCQueue -- Unit Test Suite");
    say("        C++20 | Lock-free | Cache-isolated");
    say("==============================================\n");

    say("[ Basic correctness ]");
    RUN(capacity_is_power_of_two);
    RUN(empty_on_construction);
    RUN(try_pop_empty_returns_false);
    RUN(single_push_pop);
    RUN(fifo_ordering);
    RUN(full_queue_rejects_push);
    RUN(wraparound_correctness);
    RUN(optional_pop_variant);
    RUN(move_semantics);
    RUN(cache_line_separation);

    say("\n[ OrderEvent / FeedQueue ]");
    RUN(order_event_is_cache_line_sized);
    RUN(push_pop_order_event);

    say("\n[ Multi-threaded ]");
    RUN(threaded_producer_consumer_1M);
    RUN(threaded_no_data_loss_100k_structs);

    say("\n[ Feed pipeline ]");
    RUN(feed_pipeline_add_events);
    RUN(feed_pipeline_cancel_events);

    say("\n----------------------------------------------");
    char buf[64];
    int n = snprintf(buf,sizeof(buf),"  Results: %d passed, %d failed\n",g_pass,g_fail);
    ::write(1,buf,n);
    say("----------------------------------------------\n");
    return g_fail > 0 ? 1 : 0;
}
