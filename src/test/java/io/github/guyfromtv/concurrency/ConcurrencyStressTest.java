package io.github.guyfromtv.concurrency;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.guyfromtv.concurrency.cache.StripedTtlCache;
import io.github.guyfromtv.concurrency.ratelimit.KeyedRateLimiter;
import io.github.guyfromtv.concurrency.ratelimit.SlidingWindowRateLimiter;
import io.github.guyfromtv.concurrency.ratelimit.TokenBucketRateLimiter;
import io.github.guyfromtv.concurrency.support.ManualTicker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Multi-threaded tests that assert <em>invariants</em>, never timings.
 *
 * <p>"Roughly the right number of permits" or "finished fast enough" would be a flaky
 * test. Each assertion here is something that must hold exactly, however the scheduler
 * interleaves the threads -- a limiter must never grant more than its limit, a cache must
 * never exceed its bound, a loader must run once per key. Those are the properties a
 * correctness bug actually violates.
 *
 * <p>Threads are released together from a latch so they genuinely collide rather than
 * trickling in as they are scheduled.
 */
class ConcurrencyStressTest {

    private static final int THREADS = 32;

    @Test
    @DisplayName("A token bucket never grants more permits than it holds")
    void tokenBucketNeverOverGrants() throws Exception {
        // Time is frozen, so the bucket cannot refill during the run and the correct total
        // is exactly the capacity. Any over-grant is a lost update in the CAS loop.
        ManualTicker frozen = new ManualTicker();
        var limiter = new TokenBucketRateLimiter(100, 1, Duration.ofHours(1), frozen);

        LongAdder granted = new LongAdder();
        runConcurrently(THREADS, () -> {
            for (int i = 0; i < 1_000; i++) {
                if (limiter.tryAcquire()) {
                    granted.increment();
                }
            }
            return null;
        });

        assertThat(granted.sum())
                .as("32 threads x 1000 attempts against a 100-permit bucket")
                .isEqualTo(100);
        assertThat(limiter.availablePermits()).isZero();
    }

    @Test
    @DisplayName("A sliding window never grants more permits than its limit")
    void slidingWindowNeverOverGrants() throws Exception {
        ManualTicker frozen = new ManualTicker();
        var limiter = new SlidingWindowRateLimiter(250, Duration.ofHours(1), 10, frozen);

        LongAdder granted = new LongAdder();
        runConcurrently(THREADS, () -> {
            for (int i = 0; i < 500; i++) {
                if (limiter.tryAcquire()) {
                    granted.increment();
                }
            }
            return null;
        });

        assertThat(granted.sum()).isEqualTo(250);
        assertThat(limiter.usedPermits()).isEqualTo(250);
    }

    @Test
    @DisplayName("Multi-permit grants stay exact under contention")
    void multiPermitGrantsAreExact() throws Exception {
        ManualTicker frozen = new ManualTicker();
        var limiter = new TokenBucketRateLimiter(1_000, 1, Duration.ofHours(1), frozen);

        LongAdder permitsTaken = new LongAdder();
        runConcurrently(THREADS, () -> {
            for (int i = 0; i < 200; i++) {
                if (limiter.tryAcquire(7)) {
                    permitsTaken.add(7);
                }
            }
            return null;
        });

        // 1000 is not divisible by 7, so the most that can be taken is 142 * 7 = 994.
        assertThat(permitsTaken.sum()).isEqualTo(994);
        assertThat(limiter.availablePermits()).isEqualTo(6);
    }

    @Test
    @DisplayName("A cache loader runs exactly once per key under a stampede")
    void loaderRunsOncePerKeyUnderContention() throws Exception {
        // The stampede scenario: many threads miss the same cold key at the same moment.
        // Without single-flight each would run the loader, which in production means N
        // simultaneous database queries for one value.
        var cache = StripedTtlCache.<String, String>builder()
                .maximumSize(512)
                .defaultTtl(Duration.ofHours(1))
                .build();

        ConcurrentHashMap<String, AtomicInteger> loaderCalls = new ConcurrentHashMap<>();
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            keys.add("key-" + i);
        }

        runConcurrently(THREADS, () -> {
            for (String key : keys) {
                String value = cache.get(key, k -> {
                    loaderCalls.computeIfAbsent(k, ignored -> new AtomicInteger()).incrementAndGet();
                    return "value-for-" + k;
                });
                assertThat(value).isEqualTo("value-for-" + key);
            }
            return null;
        });

        assertThat(loaderCalls).hasSize(keys.size());
        assertThat(loaderCalls.values()).allSatisfy(calls -> assertThat(calls.get())
                .as("each loader must run exactly once despite %d threads", THREADS)
                .isEqualTo(1));
    }

    @Test
    @DisplayName("A cache never exceeds its size bound under concurrent writes")
    void cacheStaysBoundedUnderConcurrentWrites() throws Exception {
        int maximumSize = 256;
        var cache = StripedTtlCache.<Integer, String>builder()
                .maximumSize(maximumSize)
                .concurrencyLevel(8)
                .defaultTtl(Duration.ofHours(1))
                .build();

        runConcurrently(THREADS, () -> {
            for (int i = 0; i < 2_000; i++) {
                // Distinct keys across threads, far more than the cache can hold, so
                // eviction runs constantly while reads interleave with it.
                cache.put(Thread.currentThread().hashCode() * 31 + i, "v");
                cache.get(i);
            }
            return null;
        });

        // Per-segment capacity rounds up, so the ceiling is the rounded total rather than
        // the requested figure; what must never happen is unbounded growth.
        assertThat(cache.size()).isLessThanOrEqualTo(maximumSize + cache.segmentCount());
    }

    @Test
    @DisplayName("Concurrent readers and writers on shared keys lose no updates")
    void concurrentReadWriteStaysConsistent() throws Exception {
        var cache = StripedTtlCache.<String, String>builder()
                .maximumSize(1_024)
                .defaultTtl(Duration.ofHours(1))
                .build();

        // Every value written for a key is identical, so any read must either miss or
        // return exactly that value. A torn or stale-node read would show up as a
        // mismatch, and a broken linked list would throw.
        runConcurrently(THREADS, () -> {
            for (int i = 0; i < 2_000; i++) {
                String key = "shared-" + (i % 32);
                cache.put(key, "canonical-" + (i % 32));
                cache.get(key).ifPresent(value -> assertThat(value).startsWith("canonical-"));
                if (i % 7 == 0) {
                    cache.remove(key);
                }
            }
            return null;
        });

        assertThat(cache.size()).isLessThanOrEqualTo(32 + cache.segmentCount());
    }

    @Test
    @DisplayName("A keyed limiter keeps keys independent and bounded")
    void keyedLimiterIsPerKeyAndBounded() throws Exception {
        ManualTicker frozen = new ManualTicker();
        int maximumKeys = 64;
        KeyedRateLimiter<String> limiter = KeyedRateLimiter.create(
                maximumKeys,
                Duration.ofHours(1),
                frozen,
                key -> new TokenBucketRateLimiter(5, 1, Duration.ofHours(1), frozen));

        // Only 8 distinct keys, well inside the bound, so no limiter is evicted and each
        // key's allowance is exact.
        ConcurrentHashMap<String, LongAdder> grantsPerKey = new ConcurrentHashMap<>();
        runConcurrently(THREADS, () -> {
            for (int i = 0; i < 200; i++) {
                String key = "client-" + (i % 8);
                if (limiter.tryAcquire(key)) {
                    grantsPerKey.computeIfAbsent(key, ignored -> new LongAdder()).increment();
                }
            }
            return null;
        });

        assertThat(grantsPerKey).hasSize(8);
        assertThat(grantsPerKey.values())
                .allSatisfy(grants -> assertThat(grants.sum())
                        .as("each key gets its own 5 permits, no more")
                        .isEqualTo(5));
        assertThat(limiter.trackedKeys()).isEqualTo(8);
    }

    @Test
    @DisplayName("A keyed limiter does not grow without bound as keys churn")
    void keyedLimiterDoesNotLeak() throws Exception {
        ManualTicker frozen = new ManualTicker();
        int maximumKeys = 32;
        KeyedRateLimiter<String> limiter = KeyedRateLimiter.create(
                maximumKeys, Duration.ofMinutes(1), frozen, key -> TokenBucketRateLimiter.perSecond(10, 10));

        // The attack shape: a flood of keys never seen before, as a public endpoint keyed
        // by client address would see. The tracked set must stay bounded.
        runConcurrently(THREADS, () -> {
            for (int i = 0; i < 1_000; i++) {
                limiter.tryAcquire("ephemeral-" + Thread.currentThread().threadId() + "-" + i);
            }
            return null;
        });

        assertThat(limiter.trackedKeys()).isLessThanOrEqualTo(maximumKeys + 16);
    }

    /** Runs {@code task} on {@code threads} threads released simultaneously. */
    private static void runConcurrently(int threads, Callable<Void> task) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGun = new CountDownLatch(1);
        List<Future<Void>> futures = new ArrayList<>();

        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    startGun.await();
                    return task.call();
                }));
            }
            startGun.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(120, TimeUnit.SECONDS))
                    .as("a hang here means a deadlock or livelock")
                    .isTrue();

            // Surfaces any assertion failure or exception from inside a worker; without
            // this the test would pass while every thread had thrown.
            for (Future<Void> future : futures) {
                future.get();
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
