package io.github.guyfromtv.concurrency.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import io.github.guyfromtv.concurrency.support.ManualTicker;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TokenBucketRateLimiterTest {

    private ManualTicker ticker;

    @BeforeEach
    void setUp() {
        ticker = new ManualTicker();
    }

    /** 10 permits of burst, refilling 10 per second, so one permit per 100ms. */
    private TokenBucketRateLimiter limiter() {
        return new TokenBucketRateLimiter(10, 10, Duration.ofSeconds(1), ticker);
    }

    @Test
    @DisplayName("A fresh bucket starts full and allows its whole burst")
    void startsFull() {
        var limiter = limiter();

        for (int i = 0; i < 10; i++) {
            assertThat(limiter.tryAcquire()).as("permit %d of the burst", i + 1).isTrue();
        }
        assertThat(limiter.tryAcquire()).as("burst exhausted").isFalse();
    }

    @Test
    @DisplayName("Permits come back at the configured rate")
    void refillsOverTime() {
        var limiter = limiter();
        for (int i = 0; i < 10; i++) {
            limiter.tryAcquire();
        }
        assertThat(limiter.tryAcquire()).isFalse();

        // One permit costs 100ms.
        ticker.advance(Duration.ofMillis(100));
        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isFalse();

        ticker.advance(Duration.ofMillis(300));
        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isFalse();
    }

    @Test
    @DisplayName("Accumulated permits are capped at the burst capacity")
    void refillCapsAtCapacity() {
        var limiter = limiter();
        for (int i = 0; i < 10; i++) {
            limiter.tryAcquire();
        }

        // An hour of idleness must not bank an hour's worth of permits; that would let a
        // long-idle client flood on its first burst.
        ticker.advance(Duration.ofHours(1));

        assertThat(limiter.availablePermits()).isEqualTo(10);
        for (int i = 0; i < 10; i++) {
            assertThat(limiter.tryAcquire()).isTrue();
        }
        assertThat(limiter.tryAcquire()).isFalse();
    }

    @Test
    @DisplayName("Polling faster than the refill interval still accrues permits")
    void frequentPollingDoesNotLoseFractionalTime() {
        var limiter = limiter();
        for (int i = 0; i < 10; i++) {
            limiter.tryAcquire();
        }

        // This is the drift guard. Each step is 10ms, which is less than the 100ms a
        // permit costs, so every individual step floors to zero new tokens. Only because
        // the refill timestamp advances by the time actually consumed -- rather than
        // jumping to "now" and discarding the remainder -- does the leftover accumulate
        // and grant exactly one permit at the 100ms mark. With the drift bug, none of
        // these ten attempts would ever succeed.
        int granted = 0;
        for (int i = 0; i < 10; i++) {
            ticker.advance(Duration.ofMillis(10));
            if (limiter.tryAcquire()) {
                granted++;
            }
        }

        assertThat(granted)
                .as("100ms of accumulated time is worth exactly one permit")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("A multi-permit request is all or nothing")
    void multiPermitIsAtomic() {
        var limiter = limiter();
        assertThat(limiter.tryAcquire(6)).isTrue();

        // Only 4 left, so a request for 6 must take nothing rather than part.
        assertThat(limiter.tryAcquire(6)).isFalse();
        assertThat(limiter.availablePermits()).isEqualTo(4);
        assertThat(limiter.tryAcquire(4)).isTrue();
    }

    @Test
    @DisplayName("A request larger than the capacity can never succeed")
    void requestBeyondCapacityRefused() {
        assertThat(limiter().tryAcquire(11)).isFalse();
    }

    @Test
    @DisplayName("Invalid configuration and arguments are refused")
    void validation() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new TokenBucketRateLimiter(0, 1, Duration.ofSeconds(1), ticker));
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new TokenBucketRateLimiter(1, 0, Duration.ofSeconds(1), ticker));
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new TokenBucketRateLimiter(1, 1, Duration.ZERO, ticker));
        // Faster than one permit per nanosecond cannot be represented.
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new TokenBucketRateLimiter(1, 2_000_000_000L, Duration.ofSeconds(1), ticker));
        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> limiter().tryAcquire(0));
    }

    @Test
    @DisplayName("The arithmetic survives nanoTime crossing zero")
    void worksAcrossNanoTimeSignChange() {
        // nanoTime has an arbitrary origin and may be negative; a limiter that compared
        // timestamps instead of subtracting them would misbehave right here.
        ManualTicker crossing = new ManualTicker(-50_000_000L);
        var limiter = new TokenBucketRateLimiter(5, 10, Duration.ofSeconds(1), crossing);

        for (int i = 0; i < 5; i++) {
            assertThat(limiter.tryAcquire()).isTrue();
        }
        assertThat(limiter.tryAcquire()).isFalse();

        crossing.advance(Duration.ofMillis(100));
        assertThat(limiter.tryAcquire()).as("refill must work across the sign change").isTrue();
    }
}
