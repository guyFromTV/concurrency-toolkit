package io.github.guyfromtv.concurrency.support;

import io.github.guyfromtv.concurrency.time.Ticker;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A ticker the tests advance by hand.
 *
 * <p>Every timing behaviour in this library is therefore tested deterministically: no
 * test sleeps, and none can fail because a CI runner was briefly busy. Sleep-based timing
 * tests are the classic source of flaky suites.
 *
 * <p>Starts at a large negative value on purpose, so the tests exercise the arithmetic
 * near and across {@code nanoTime}'s sign change rather than only in the comfortable
 * positive range.
 */
public final class ManualTicker implements Ticker {

    private final AtomicLong nanos;

    public ManualTicker() {
        this(-1_000_000_000L);
    }

    public ManualTicker(long startNanos) {
        this.nanos = new AtomicLong(startNanos);
    }

    @Override
    public long nanoTime() {
        return nanos.get();
    }

    public void advance(Duration amount) {
        nanos.addAndGet(amount.toNanos());
    }

    public void advanceNanos(long amount) {
        nanos.addAndGet(amount);
    }
}
