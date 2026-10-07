# concurrency-toolkit

A thread-safe TTL cache and two rate limiters, written in plain **Java 21** on
`java.util.concurrent` with **no runtime dependencies**.

No Spring, no framework, nothing to configure. The point of this project is the
concurrency itself: how the synchronisation is chosen, why it is correct, and what it
costs under contention.

---

## What is in it

| Type | Purpose |
|---|---|
| `StripedTtlCache<K,V>` | Bounded cache with per-entry TTL, LRU eviction and single-flight loading |
| `TokenBucketRateLimiter` | Lock-free token bucket with a burst allowance |
| `SlidingWindowRateLimiter` | Bucketed sliding window with O(1) admission |
| `KeyedRateLimiter<K>` | Independent limit per user / API key / IP, without leaking memory |
| `Ticker` | Injectable monotonic time, so every timing test is deterministic |

---

## The design decisions, and why

### Striping, because an LRU cache writes on every read

An LRU cache mutates shared state on a **hit**, not just on a write: the entry has to move
to the front of the recency list. So the obvious implementation — one lock around a
`LinkedHashMap` — serialises reads too, and throughput stops improving past a single core.

`StripedTtlCache` splits the key space across `2^n` segments, each with its own lock, map
and recency list. Threads touching different segments never contend. Measured on this
machine (12 hardware threads, 90% reads, identical code with only the segment count
changing):

```
configuration                   ops/sec       relative
concurrencyLevel=1            4,103,561          1.00x
concurrencyLevel=2            4,781,394          1.17x
concurrencyLevel=4            5,265,858          1.28x
concurrencyLevel=8            8,746,216          2.13x
concurrencyLevel=16          12,972,375          3.16x
concurrencyLevel=32          18,534,981          4.52x
```

**These numbers are indicative, not rigorous.** They come from a hand-rolled harness in
one JVM with no forking and no statistics; see the caveats in `Benchmarks`. They are
included because striping is a design claim and a claim about contention deserves a
number — but reach for JMH before quoting any of it.

The cost of striping is that eviction is per-segment, so recency is *approximately* global:
a full segment evicts its own coldest entry even when a colder one exists elsewhere. Guava's
cache and pre-Java-8 `ConcurrentHashMap` make the same trade.

### An intrusive linked list, because O(1) recency needs it

Each cache entry **is** its own list node, holding its own `prev`/`next`. Moving an entry to
the front on a hit therefore costs O(1) with no search and no allocation — the map value is
already the node to unlink. A list kept separately from the map would have to be scanned to
find the entry, making every hit O(n). Sentinel head and tail nodes remove every null check
from the linking code.

### CAS for the token bucket, a lock for the sliding window

These look like similar problems and need opposite answers, which is the interesting part.

**The token bucket's entire state is two longs** — tokens and a timestamp — so it fits in one
immutable record behind an `AtomicReference` and updates with a compare-and-set retry loop.
A lock here would serialise every call on a few nanoseconds of arithmetic, making the
limiter its own bottleneck. The loop cannot spin forever: a lost CAS means another thread
committed, so progress was made.

**The sliding window has to age out buckets, read a total and increment, as one unit.** That
is several words of state, which a single CAS cannot cover, so it takes a short lock and
keeps the critical section to a few instructions.

The right synchronisation follows from the shape of the state, not from a preference for
lock-free code.

### Refill without drift

The bucket refills in whole tokens from integer division, and advances its timestamp by
exactly the time those tokens cost — **not** to `now`. Jumping to `now` would discard the
leftover fraction on every call, so a limiter polled more often than its refill interval
would never gain a token at all. There is a test for precisely that: ten 10ms polls against
a 100ms-per-token bucket must grant exactly one permit, and would grant zero with the bug.

### Monotonic time only

Everything measures elapsed time with `System.nanoTime()` and never `currentTimeMillis()`.
Wall-clock time can jump backwards when NTP corrects it, which would un-expire a cache
entry or hand a limiter a free refill. `nanoTime` is also allowed to be negative and to
wrap, so durations are compared by **subtraction**, and bucket indices use `floorDiv`/
`floorMod` rather than `/` and `%` — truncation toward zero would make two adjacent negative
instants share a bucket. The test ticker deliberately starts negative to exercise this.

### Single-flight loading, because expiry causes stampedes

`cache.get(key, loader)` runs the loader **at most once per key** across all threads;
concurrent callers for a missing key wait and share the result. Without it, a popular key
expiring under load means every in-flight request recomputes the same value — N simultaneous
database queries for one row. The loader runs outside every cache lock, so a slow load never
blocks unrelated keys.

Getting this right took two attempts. The first version claimed the load with an atomic
`putIfAbsent` on an in-flight map, which looks sufficient and is not: a thread whose cache
miss happened *before* the winner stored its value, but whose claim happened *after* the
winner removed its marker, sees no marker and loads the key a second time. The second value
then replaces the first, discarding any state it carried — for `KeyedRateLimiter` that meant
a key silently getting a second full allowance.

The fix is a re-check of the cache after winning the claim. It is sufficient because the
winner stores before removing its marker, and a claim can only succeed once the marker is
gone, so the store is guaranteed visible.

This was caught by CI, not locally. Pinned to four cores to match a CI runner, the stress
suite failed **7 times in 15** without the re-check and **0 in 15** with it.

### The keyed limiter's memory leak, avoided

The natural implementation of a per-key limiter is `ConcurrentHashMap<K, RateLimiter>` with
`computeIfAbsent`. It works, and it leaks: every key ever seen is retained forever. Keyed by
client IP on a public endpoint, that map is unbounded allocation driven by strangers — a
denial-of-service vector rather than a defence against one.

So `KeyedRateLimiter` stores its limiters in the TTL cache, bounding both how many are kept
and how long an idle one survives. The honest cost: an evicted limiter loses its state, so a
key can return with a fresh burst. For idle keys that is intended; under real pressure the
cache may also drop a limiter still in use, which is why `maximumKeys` should sit above the
expected active set.

---

## Using it

```java
TtlCache<String, User> cache = StripedTtlCache.<String, User>builder()
        .maximumSize(10_000)
        .defaultTtl(Duration.ofMinutes(5))
        .concurrencyLevel(16)
        .build();

User user = cache.get(userId, id -> userRepository.load(id));   // loads once per key

RateLimiter limiter = TokenBucketRateLimiter.perSecond(100, 200);  // 100/s, burst 200
if (!limiter.tryAcquire()) {
    return tooManyRequests();
}

KeyedRateLimiter<String> perClient = KeyedRateLimiter.create(
        50_000, Duration.ofMinutes(10), Ticker.system(),
        key -> TokenBucketRateLimiter.perSecond(10, 20));

if (!perClient.tryAcquire(clientIp)) {
    return tooManyRequests();
}
```

## Building

Needs a JDK 21+. The Maven wrapper fetches Maven itself.

```bash
./mvnw test                 # 44 tests
./mvnw package              # builds the jar
java -cp target/classes io.github.guyfromtv.concurrency.bench.Benchmarks
```

There is no Dockerfile. This is a library, not a service — there is nothing to run as a
container, and shipping one would be cargo cult.

## Tests

**44 tests, and not one of them sleeps.** Timing behaviour is driven by a `ManualTicker` the
tests advance by hand, so nothing can fail because a CI runner was briefly busy. Sleep-based
timing tests are the classic source of flaky suites.

| Suite | Covers |
|---|---|
| `StripedTtlCacheTest` | get/put/remove/clear, null rejection, TTL and per-entry TTL, LRU victim selection, size bound, loader semantics, failure handling, statistics |
| `TokenBucketRateLimiterTest` | burst, refill rate, capacity cap, drift guard, atomic multi-permit, validation, negative `nanoTime` |
| `SlidingWindowRateLimiterTest` | limit enforcement, bucket-by-bucket recovery, window reset, year-long idle gap, single-bucket degeneration, `floorDiv` correctness |
| `ConcurrencyStressTest` | 32 threads asserting invariants: no over-granting, exact multi-permit totals, one loader run per key under a stampede, bounded size under concurrent writes, read/write consistency, per-key independence, no key leak |

The concurrency tests assert **invariants, never timings** — "a limiter must never exceed its
limit" holds however the scheduler interleaves, whereas "finished fast enough" is a flaky
test waiting to happen.

Each one was verified by breaking the thing it guards:

| Sabotage | Result |
|---|---|
| CAS replaced with a plain `set` in the token bucket | 103 permits granted instead of 100; 1449 instead of 994 |
| `putIfAbsent` replaced with check-then-act in the loader | loader ran twice for a key instead of once |

A concurrency test that passes with broken synchronisation is worthless, so these were
confirmed to fail before being trusted.

## Stack

Java 21 · `java.util.concurrent` · Maven · JUnit 5 · AssertJ · GitHub Actions ·
zero runtime dependencies
