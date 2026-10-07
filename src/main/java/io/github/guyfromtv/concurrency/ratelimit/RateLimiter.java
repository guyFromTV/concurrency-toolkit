package io.github.guyfromtv.concurrency.ratelimit;

/**
 * Decides whether an action may proceed right now.
 *
 * <p>Every method is non-blocking and returns immediately. A limiter that made callers
 * wait would convert a load problem into a thread-exhaustion problem: under overload the
 * blocked threads pile up and take the process down, which is worse than refusing work.
 * Rejecting fast lets the caller decide whether to retry, queue or fail.
 *
 * <p>Implementations are safe for use by many threads at once.
 */
public interface RateLimiter {

    /** Consumes one permit if available. */
    default boolean tryAcquire() {
        return tryAcquire(1);
    }

    /**
     * Consumes {@code permits} permits if all of them are available.
     *
     * <p>All or nothing: a partial grant would let a large request slip through in pieces
     * and exceed the configured rate.
     *
     * @throws IllegalArgumentException if {@code permits} is not positive
     */
    boolean tryAcquire(int permits);
}
