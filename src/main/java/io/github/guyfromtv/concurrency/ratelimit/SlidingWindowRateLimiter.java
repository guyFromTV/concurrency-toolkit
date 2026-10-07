package io.github.guyfromtv.concurrency.ratelimit;

import io.github.guyfromtv.concurrency.time.Ticker;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Allows at most {@code limit} permits in any trailing window, counted in fixed buckets.
 *
 * <h2>Why buckets rather than timestamps</h2>
 *
 * <p>An exact sliding window keeps the timestamp of every permit and discards those that
 * fall out of the window. It is precise and its memory grows with the <em>rate</em> -- a
 * limit of a million per minute means a million stored longs, per limiter.
 *
 * <p>This splits the window into a fixed ring of counters instead, so memory is
 * proportional to the bucket count and not to traffic. The trade is granularity: the
 * oldest bucket is dropped whole rather than aged out gradually, so the effective window
 * jitters by up to one bucket. Ten buckets over a minute means the boundary is accurate to
 * six seconds, which is why the bucket count is configurable.
 *
 * <h2>Why a lock rather than CAS</h2>
 *
 * <p>Admitting a permit means ageing out expired buckets, reading the total and
 * incrementing, all as one unit. That is several words of state, so a single
 * compare-and-set cannot cover it; doing it lock-free would need a redesign around one
 * immutable snapshot. A short lock is honest and the critical section is a few
 * instructions.
 *
 * <p>Contrast {@link TokenBucketRateLimiter}, whose whole state fits in one record and so
 * genuinely suits CAS. The right synchronisation follows from the shape of the state, not
 * from a preference for lock-free code.
 */
public final class SlidingWindowRateLimiter implements RateLimiter {

    private final long limit;
    private final long bucketNanos;
    private final long[] counts;
    private final Ticker ticker;
    private final ReentrantLock lock = new ReentrantLock();

    /** Running total of {@link #counts}, kept current so admission is O(1), not O(buckets). */
    private long total;

    /** Bucket index, in units of {@code bucketNanos}, that {@link #counts} is current for. */
    private long currentEpoch;

    /**
     * @param limit maximum permits allowed within one window
     * @param window the trailing period the limit applies to
     * @param buckets how finely the window is subdivided; more buckets means a more
     *     accurate boundary and more memory
     */
    public SlidingWindowRateLimiter(long limit, Duration window, int buckets, Ticker ticker) {
        Objects.requireNonNull(window, "window");
        this.ticker = Objects.requireNonNull(ticker, "ticker");
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be at least 1");
        }
        if (window.isZero() || window.isNegative()) {
            throw new IllegalArgumentException("window must be positive");
        }
        if (buckets < 1) {
            throw new IllegalArgumentException("buckets must be at least 1");
        }

        this.bucketNanos = window.toNanos() / buckets;
        if (this.bucketNanos < 1) {
            throw new IllegalArgumentException("window too short to divide into " + buckets + " buckets");
        }

        this.limit = limit;
        this.counts = new long[buckets];
        this.currentEpoch = Math.floorDiv(ticker.nanoTime(), this.bucketNanos);
    }

    public static SlidingWindowRateLimiter perMinute(long limit) {
        return new SlidingWindowRateLimiter(limit, Duration.ofMinutes(1), 12, Ticker.system());
    }

    @Override
    public boolean tryAcquire(int permits) {
        if (permits < 1) {
            throw new IllegalArgumentException("permits must be positive");
        }
        if (permits > limit) {
            return false;
        }

        lock.lock();
        try {
            // floorDiv, not /: integer division truncates toward zero, which would make
            // two adjacent negative nanoTime values share a bucket.
            advanceTo(Math.floorDiv(ticker.nanoTime(), bucketNanos));

            if (total + permits > limit) {
                return false;
            }
            int slot = (int) Math.floorMod(currentEpoch, counts.length);
            counts[slot] += permits;
            total += permits;
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Zeroes the buckets that have fallen out of the window. Caller holds the lock. */
    private void advanceTo(long epoch) {
        if (epoch <= currentEpoch) {
            return;
        }

        long elapsedBuckets = epoch - currentEpoch;
        if (elapsedBuckets >= counts.length) {
            // More than a whole window has passed, so nothing survives. Clearing in bulk
            // avoids looping over a huge gap after an idle period.
            java.util.Arrays.fill(counts, 0L);
            total = 0;
        } else {
            for (long i = 1; i <= elapsedBuckets; i++) {
                int slot = (int) Math.floorMod(currentEpoch + i, counts.length);
                total -= counts[slot];
                counts[slot] = 0;
            }
        }
        currentEpoch = epoch;
    }

    /** Permits used in the current window; for tests and diagnostics. */
    public long usedPermits() {
        lock.lock();
        try {
            advanceTo(Math.floorDiv(ticker.nanoTime(), bucketNanos));
            return total;
        } finally {
            lock.unlock();
        }
    }

    public long limit() {
        return limit;
    }
}
