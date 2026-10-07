package io.github.guyfromtv.concurrency.bench;

import io.github.guyfromtv.concurrency.cache.StripedTtlCache;
import io.github.guyfromtv.concurrency.cache.TtlCache;
import io.github.guyfromtv.concurrency.ratelimit.RateLimiter;
import io.github.guyfromtv.concurrency.ratelimit.TokenBucketRateLimiter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A throughput harness, run with {@code java -cp target/classes
 * io.github.guyfromtv.concurrency.bench.Benchmarks}.
 *
 * <h2>What this is not</h2>
 *
 * <p>It is not JMH, and its numbers are indicative rather than publishable. A hand-rolled
 * harness cannot defend against the things JMH exists to handle: the JIT may eliminate
 * work whose result is unused, compilation happens partway through the measurement, and a
 * single JVM invocation cannot separate run-to-run variance from a real difference.
 *
 * <p>It is included because the striping in {@link StripedTtlCache} is a design claim, and
 * a claim about contention should come with a number rather than an assertion. The
 * mitigations here are a warm-up phase before timing and accumulating a checksum from
 * every result so the work cannot be optimised away. Treat the output as a direction, not
 * a measurement, and reach for JMH before quoting any of it.
 */
public final class Benchmarks {

    private static final Duration WARMUP = Duration.ofSeconds(2);
    private static final Duration MEASURE = Duration.ofSeconds(3);
    private static final int KEY_SPACE = 10_000;

    private Benchmarks() {}

    public static void main(String[] args) throws Exception {
        int threads = Runtime.getRuntime().availableProcessors();
        System.out.printf("Hardware threads: %d%n", threads);
        System.out.printf("Warm-up %ds, measure %ds per run. Indicative only -- not JMH.%n%n",
                WARMUP.toSeconds(), MEASURE.toSeconds());

        System.out.println("== TtlCache: 90% reads, 10% writes ==");
        System.out.printf("%-24s %14s %14s%n", "configuration", "ops/sec", "relative");
        double baseline = 0;
        for (int concurrency : new int[] {1, 2, 4, 8, 16, 32}) {
            double opsPerSecond = benchmarkCache(threads, concurrency);
            if (concurrency == 1) {
                baseline = opsPerSecond;
            }
            System.out.printf(
                    "%-24s %,14.0f %13.2fx%n",
                    "concurrencyLevel=" + concurrency, opsPerSecond, opsPerSecond / baseline);
        }

        System.out.println();
        System.out.println("== TokenBucketRateLimiter: lock-free CAS ==");
        System.out.printf("%-24s %14s%n", "configuration", "ops/sec");
        System.out.printf("%-24s %,14.0f%n", "threads=" + threads, benchmarkRateLimiter(threads));

        System.out.println();
        System.out.println("Reminder: one JVM, no forking, no statistics. Use JMH for real figures.");
    }

    /**
     * The interesting comparison: identical code and identical workload, varying only how
     * many independent locks the key space is split across. {@code concurrencyLevel=1} is
     * the single-lock cache that striping is meant to improve on.
     */
    private static double benchmarkCache(int threads, int concurrencyLevel) throws Exception {
        TtlCache<Integer, String> cache = StripedTtlCache.<Integer, String>builder()
                .maximumSize(KEY_SPACE)
                .concurrencyLevel(concurrencyLevel)
                .defaultTtl(Duration.ofHours(1))
                .build();

        for (int i = 0; i < KEY_SPACE; i++) {
            cache.put(i, "value-" + i);
        }

        return measure(threads, stop -> {
            long operations = 0;
            long checksum = 0;
            ThreadLocalRandom random = ThreadLocalRandom.current();
            while (!stop.get()) {
                // Batched so the stop flag is not read on every operation, which would
                // itself become the contended memory location being measured.
                for (int i = 0; i < 128; i++) {
                    int key = random.nextInt(KEY_SPACE);
                    if (random.nextInt(10) == 0) {
                        cache.put(key, "updated-" + key);
                    } else {
                        checksum += cache.get(key).map(String::length).orElse(0);
                    }
                }
                operations += 128;
            }
            return new Result(operations, checksum);
        });
    }

    private static double benchmarkRateLimiter(int threads) throws Exception {
        RateLimiter limiter = new TokenBucketRateLimiter(
                1_000_000, 1_000_000, Duration.ofSeconds(1), io.github.guyfromtv.concurrency.time.Ticker.system());

        return measure(threads, stop -> {
            long operations = 0;
            long checksum = 0;
            while (!stop.get()) {
                for (int i = 0; i < 128; i++) {
                    if (limiter.tryAcquire()) {
                        checksum++;
                    }
                }
                operations += 128;
            }
            return new Result(operations, checksum);
        });
    }

    /** Runs the workload for the warm-up period, discards it, then measures. */
    private static double measure(int threads, Workload workload) throws Exception {
        runFor(threads, workload, WARMUP);
        return runFor(threads, workload, MEASURE);
    }

    private static double runFor(int threads, Workload workload, Duration duration) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicBoolean stop = new AtomicBoolean();
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch startGun = new CountDownLatch(1);
        List<Future<Result>> futures = new ArrayList<>();

        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    startGun.await();
                    return workload.run(stop);
                }));
            }

            ready.await();
            long startNanos = System.nanoTime();
            startGun.countDown();
            Thread.sleep(duration.toMillis());
            stop.set(true);

            long operations = 0;
            long checksum = 0;
            for (Future<Result> future : futures) {
                Result result = future.get();
                operations += result.operations();
                checksum += result.checksum();
            }
            long elapsedNanos = System.nanoTime() - startNanos;

            // Consuming the checksum keeps the JIT from discarding the measured work.
            if (checksum == Long.MIN_VALUE) {
                System.out.print("");
            }
            return operations / (elapsedNanos / 1_000_000_000.0);
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @FunctionalInterface
    private interface Workload {
        Result run(AtomicBoolean stop) throws Exception;
    }

    private record Result(long operations, long checksum) {}
}
