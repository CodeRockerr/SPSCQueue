#include "spsc_queue.hpp"
#include "market_feed.hpp"
#include <unistd.h>
#include <cstdarg>
#include <cstring>
#include <cstdio>
#include <thread>
#include <atomic>
#include <vector>
#include <algorithm>
#include <numeric>
#include <chrono>

#if defined(__x86_64__)
  static inline uint64_t rdtsc() {
      uint32_t lo, hi;
      __asm__ volatile("rdtsc" : "=a"(lo), "=d"(hi));
      return (uint64_t(hi) << 32) | lo;
  }
  static inline void cpu_pause() { __builtin_ia32_pause(); }
#else
  static inline uint64_t rdtsc() {
      return uint64_t(std::chrono::high_resolution_clock::now()
                      .time_since_epoch().count());
  }
  static inline void cpu_pause() {}
#endif

static void wout(const char* s) { ::write(1, s, strlen(s)); }
static void woutf(const char* fmt, ...) {
    char buf[256]; va_list ap; va_start(ap,fmt);
    int n = vsnprintf(buf,sizeof(buf),fmt,ap); va_end(ap);
    ::write(1,buf,n);
}
static void sep(char c='-', int n=52) {
    char line[64]; memset(line,c,n); line[n]='\n'; ::write(1,line,n+1);
}

// ── TSC calibration ───────────────────────────────────────────────────────────
static double calibrate_tsc() {
    using Clock = std::chrono::high_resolution_clock;
    double samples[5];
    for (int i = 0; i < 5; ++i) {
        auto t0 = Clock::now();
        uint64_t r0 = rdtsc();
        while (std::chrono::duration_cast<std::chrono::milliseconds>(
                   Clock::now()-t0).count() < 10) {}
        uint64_t r1 = rdtsc();
        auto t1 = Clock::now();
        double ns = double(std::chrono::duration_cast<
                           std::chrono::nanoseconds>(t1-t0).count());
        samples[i] = double(r1-r0)/ns;
    }
    std::sort(samples,samples+5);
    return samples[2];
}

// ── Percentile helper ─────────────────────────────────────────────────────────
static void print_latency(const char* label, std::vector<double>& s) {
    std::sort(s.begin(), s.end());
    double mn  = s.front();
    double med = s[s.size()/2];
    double p90 = s[s.size()*90/100];
    double p99 = s[s.size()*99/100];
    double p999= s[s.size()*999/1000];
    double mx  = s.back();
    woutf("\n  %s\n", label);
    sep(46);
    woutf("  %-12s %9.1f ns\n","Min",   mn);
    woutf("  %-12s %9.1f ns\n","Median",med);
    woutf("  %-12s %9.1f ns\n","p90",   p90);
    woutf("  %-12s %9.1f ns\n","p99",   p99);
    woutf("  %-12s %9.1f ns\n","p99.9", p999);
    woutf("  %-12s %9.1f ns\n","Max",   mx);
    woutf("  %-12s %9zu\n",    "Samples",s.size());
}

// ── Bench 1: Single-threaded push+pop roundtrip ───────────────────────────────
void bench_roundtrip(double tpns) {
    spsc::Queue<uint64_t, 4096> q;
    constexpr int N = 500'000;
    std::vector<double> lat;
    lat.reserve(N);

    for (int i = 0; i < N; ++i) {
        uint64_t t0 = rdtsc();
        q.try_push(uint64_t(i));
        uint64_t val;
        q.try_pop(val);
        uint64_t t1 = rdtsc();
        lat.push_back(double(t1-t0)/tpns);
    }
    print_latency("Push+Pop roundtrip latency (single-thread)", lat);
}

// ── Bench 2: Multi-threaded throughput ───────────────────────────────────────
void bench_throughput() {
    spsc::FeedQueue q;
    constexpr uint64_t N = 2'000'000;
    std::atomic<bool> start{false};
    uint64_t prod_ns = 0, cons_ns = 0;

    std::thread prod([&]{
        while (!start.load()) cpu_pause();
        using Clock = std::chrono::high_resolution_clock;
        auto t0 = Clock::now();
        spsc::OrderEvent ev{};
        ev.type = spsc::EventType::ADD;
        ev.price = 17542;
        ev.qty   = 100;
        for (uint64_t i = 0; i < N; ++i) {
            ev.order_id = i;
            spsc::spin_push(q, ev);
        }
        prod_ns = uint64_t(std::chrono::duration_cast<
                           std::chrono::nanoseconds>(Clock::now()-t0).count());
    });

    std::thread cons([&]{
        while (!start.load()) cpu_pause();
        using Clock = std::chrono::high_resolution_clock;
        uint64_t received = 0;
        auto t0 = Clock::now();
        spsc::OrderEvent ev;
        while (received < N) {
            if (q.try_pop(ev)) ++received;
            else cpu_pause();
        }
        cons_ns = uint64_t(std::chrono::duration_cast<
                           std::chrono::nanoseconds>(Clock::now()-t0).count());
    });

    start.store(true);
    prod.join();
    cons.join();

    double prod_mps = double(N) / double(prod_ns) * 1000.0;
    double cons_mps = double(N) / double(cons_ns) * 1000.0;

    woutf("\n  Multi-threaded throughput\n");
    sep(46);
    woutf("  %-22s %10lu\n",   "Messages sent",      N);
    woutf("  %-22s %10.1f ns\n","Producer wall time", double(prod_ns)/N);
    woutf("  %-22s %10.1f ns\n","Consumer wall time", double(cons_ns)/N);
    woutf("  %-22s %10.2f M/s\n","Producer M-msg/s",  prod_mps);
    woutf("  %-22s %10.2f M/s\n","Consumer M-msg/s",  cons_mps);
}

// ── Bench 3: False sharing baseline comparison ────────────────────────────────
// Show the cost of NOT separating head/tail onto separate cache lines.
// We simulate it with two atomics on the same cache line.
void bench_false_sharing_demo(double tpns) {
    // Same-line atomics (bad — like a naive ring buffer)
    struct BadPair {
        std::atomic<uint64_t> a{0};
        std::atomic<uint64_t> b{0}; // shares cache line with a
    } bad;

    // Separate-line atomics (good — like our SPSC queue)
    struct GoodPair {
        alignas(64) std::atomic<uint64_t> a{0};
        alignas(64) std::atomic<uint64_t> b{0};
    } good;

    constexpr int N = 200'000;
    std::vector<double> bad_lat, good_lat;
    bad_lat.reserve(N); good_lat.reserve(N);

    // Bad: two threads ping-pong on the same cache line
    {
        std::atomic<bool> go{false};
        std::thread t([&]{
            while (!go) cpu_pause();
            for (int i = 0; i < N; ++i) bad.b.fetch_add(1, std::memory_order_relaxed);
        });
        go = true;
        for (int i = 0; i < N; ++i) {
            uint64_t t0 = rdtsc();
            bad.a.fetch_add(1, std::memory_order_relaxed);
            bad_lat.push_back(double(rdtsc()-t0)/tpns);
        }
        t.join();
    }

    // Good: two threads on separate cache lines
    {
        std::atomic<bool> go{false};
        std::thread t([&]{
            while (!go) cpu_pause();
            for (int i = 0; i < N; ++i) good.b.fetch_add(1, std::memory_order_relaxed);
        });
        go = true;
        for (int i = 0; i < N; ++i) {
            uint64_t t0 = rdtsc();
            good.a.fetch_add(1, std::memory_order_relaxed);
            good_lat.push_back(double(rdtsc()-t0)/tpns);
        }
        t.join();
    }

    print_latency("Shared cache line (false sharing -- naive queue)", bad_lat);
    print_latency("Separate cache lines (our SPSC design)", good_lat);

    std::sort(bad_lat.begin(),  bad_lat.end());
    std::sort(good_lat.begin(), good_lat.end());
    double speedup = bad_lat[bad_lat.size()/2] / good_lat[good_lat.size()/2];
    woutf("\n  Cache-line separation speedup (median): %.1fx\n", speedup);
}

// ─────────────────────────────────────────────────────────────────────────────
int main() {
    sep('=', 52);
    wout("  SPSCQueue -- HFT Benchmark\n");
    wout("  C++20 | Lock-free | acquire/release | rdtsc\n");
    sep('=', 52);

    wout("\nCalibrating TSC (5 x 10ms)...\n");
    double tpns = calibrate_tsc();
    woutf("TSC rate: %.3f ticks/ns\n", tpns);

    woutf("\n[1] SINGLE-THREAD ROUNDTRIP  500k iterations\n");
    bench_roundtrip(tpns);

    woutf("\n[2] MULTI-THREAD THROUGHPUT  2M messages\n");
    bench_throughput();

    woutf("\n[3] FALSE SHARING DEMO  200k iterations\n");
    bench_false_sharing_demo(tpns);

    wout("\n");
    sep('=', 52);
    wout("  Done.\n");
    sep('=', 52);
    wout("\n");
    return 0;
}
