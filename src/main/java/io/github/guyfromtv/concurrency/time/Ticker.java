package io.github.guyfromtv.concurrency.time;

/**
 * A source of monotonic elapsed time.
 *
 * <p>Everything in this library measures durations rather than wall-clock instants, so
 * it reads {@link System#nanoTime()} and never {@code currentTimeMillis()}: wall-clock
 * time can jump backwards when NTP corrects the clock, which would make a cache entry
 * un-expire or hand a rate limiter a free refill.
 *
 * <p>It is an interface purely so tests can advance time by hand. Every timing test in
 * this project is deterministic and sleeps for nothing.
 */
@FunctionalInterface
public interface Ticker {

    /** Nanoseconds from an arbitrary origin; only differences are meaningful. */
    long nanoTime();

    static Ticker system() {
        return System::nanoTime;
    }
}
