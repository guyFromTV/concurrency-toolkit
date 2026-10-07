package io.github.guyfromtv.concurrency.ratelimit;

import io.github.guyfromtv.concurrency.time.Ticker;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A token bucket: permits accumulate at a fixed rate up to a burst capacity, and each
 * call spends one.
 *
 * <h2>Why a bucket rather than a counter</h2>
 *
 * <p>A fixed-window counter lets a caller spend its whole quota at the end of one window
 * and again at the start of the next, so a "100 per minute" limit permits 200 in two
 * adjacent seconds. A bucket has no window boundary to exploit: it smooths to the refill
 * rate while still allowing a deliberate burst of at most {@code capacity}.
 *
 * <h2>Why it is lock-free</h2>
 *
 * <p>State is one immutable record behind an {@link AtomicReference}, updated by a
 * compare-and-set retry loop. A lock would serialise every call on what is a few
 * nanoseconds of arithmetic -- for a limiter sitting in front of every request, the lock
 * itself becomes the bottleneck it was meant to protect against.
 *
 * <p>The loop is safe because a lost CAS means another thread committed first, so this
 * thread simply re-reads and recomputes. It cannot spin forever: every retry happens only
 * because real progress was made elsewhere.
 *
 * <h2>Refill without drift</h2>
 *
 * <p>Refill is whole tokens from integer division, and the timestamp advances by exactly
 * the time those tokens cost -- not to {@code now}. Advancing to {@code now} would discard
 * the leftover fraction on every call, and a limiter polled more often than its refill
 * interval would never gain a token at all.
 */
public final class TokenBucketRateLimiter implements RateLimiter {

    private final long capacity;
    private final long nanosPerToken;
    private final Ticker ticker;
    private final AtomicReference<State> state;

    /**
     * @param capacity the burst size, and the ceiling on accumulated permits
     * @param refillTokens how many permits are added each {@code refillPeriod}
     * @param refillPeriod how long it takes to add {@code refillTokens}
     */
    public TokenBucketRateLimiter(long capacity, long refillTokens, Duration refillPeriod, Ticker ticker) {
        Objects.requireNonNull(refillPeriod, "refillPeriod");
        this.ticker = Objects.requireNonNull(ticker, "ticker");
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be at least 1");
        }
        if (refillTokens < 1) {
            throw new IllegalArgumentException("refillTokens must be at least 1");
        }
        if (refillPeriod.isZero() || refillPeriod.isNegative()) {
            throw new IllegalArgumentException("refillPeriod must be positive");
        }

        long periodNanos = refillPeriod.toNanos();
        this.nanosPerToken = periodNanos / refillTokens;
        if (this.nanosPerToken < 1) {
            // Faster than one token per nanosecond cannot be represented, and nothing
            // real needs it. Better to refuse than to silently limit nothing.
            throw new IllegalArgumentException(
                    "refill rate too high to represent: " + refillTokens + " per " + refillPeriod);
        }

        this.capacity = capacity;
        // Starts full, so a fresh limiter permits its burst immediately rather than
        // making the first caller wait for a refill it did nothing to deserve.
        this.state = new AtomicReference<>(new State(capacity, ticker.nanoTime()));
    }

    /** Convenience factory: a steady rate with a burst allowance. */
    public static TokenBucketRateLimiter perSecond(long permitsPerSecond, long burstCapacity) {
        return new TokenBucketRateLimiter(burstCapacity, permitsPerSecond, Duration.ofSeconds(1), Ticker.system());
    }

    @Override
    public boolean tryAcquire(int permits) {
        if (permits < 1) {
            throw new IllegalArgumentException("permits must be positive");
        }
        if (permits > capacity) {
            // Could never be satisfied however long the caller waits; say so now.
            return false;
        }

        while (true) {
            State current = state.get();
            long now = ticker.nanoTime();

            // Subtraction, not comparison: nanoTime may wrap.
            long elapsed = now - current.lastRefillNanos;
            long refilled = elapsed > 0 ? elapsed / nanosPerToken : 0;
            long tokens = Math.min(capacity, current.tokens + refilled);
            long lastRefill = current.lastRefillNanos + refilled * nanosPerToken;

            if (tokens < permits) {
                // Still publish the refill, so the accumulated time is not recomputed
                // from scratch on the next call. Losing this CAS is harmless.
                if (refilled > 0) {
                    state.compareAndSet(current, new State(tokens, lastRefill));
                }
                return false;
            }

            if (state.compareAndSet(current, new State(tokens - permits, lastRefill))) {
                return true;
            }
            // Another thread won; re-read and try again.
        }
    }

    /** Current permits, for tests and diagnostics; inherently a stale value. */
    public long availablePermits() {
        State current = state.get();
        long elapsed = ticker.nanoTime() - current.lastRefillNanos;
        long refilled = elapsed > 0 ? elapsed / nanosPerToken : 0;
        return Math.min(capacity, current.tokens + refilled);
    }

    public long capacity() {
        return capacity;
    }

    /**
     * Immutable, so a CAS swaps both fields together. Two separate atomics could be read
     * half-updated, letting a thread apply a refill it had already counted.
     */
    private record State(long tokens, long lastRefillNanos) {}
}
