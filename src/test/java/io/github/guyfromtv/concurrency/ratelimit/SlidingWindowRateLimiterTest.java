package io.github.guyfromtv.concurrency.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import io.github.guyfromtv.concurrency.support.ManualTicker;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SlidingWindowRateLimiterTest {

    private ManualTicker ticker;

    @BeforeEach
    void setUp() {
        ticker = new ManualTicker();
    }

    /** 10 permits per second, in 10 buckets of 100ms. */
    private SlidingWindowRateLimiter limiter() {
        return new SlidingWindowRateLimiter(10, Duration.ofSeconds(1), 10, ticker);
    }

    @Test
    @DisplayName("The limit is enforced within one window")
    void enforcesLimit() {
        var limiter = limiter();

        for (int i = 0; i < 10; i++) {
            assertThat(limiter.tryAcquire()).isTrue();
        }
        assertThat(limiter.tryAcquire()).isFalse();
        assertThat(limiter.usedPermits()).isEqualTo(10);
    }

    @Test
    @DisplayName("Capacity returns gradually as buckets age out")
    void capacityReturnsBucketByBucket() {
        var limiter = limiter();

        // Spend 5 in the first bucket, then 5 one bucket later.
        for (int i = 0; i < 5; i++) {
            limiter.tryAcquire();
        }
        ticker.advance(Duration.ofMillis(100));
        for (int i = 0; i < 5; i++) {
            limiter.tryAcquire();
        }
        assertThat(limiter.tryAcquire()).isFalse();

        // Advance until only the first bucket has fallen out of the window: the 5 permits
        // it held come back, the later 5 do not.
        ticker.advance(Duration.ofMillis(900));
        assertThat(limiter.usedPermits()).isEqualTo(5);

        for (int i = 0; i < 5; i++) {
            assertThat(limiter.tryAcquire()).isTrue();
        }
        assertThat(limiter.tryAcquire()).isFalse();
    }

    @Test
    @DisplayName("A full window of silence resets the limiter")
    void fullWindowResets() {
        var limiter = limiter();
        for (int i = 0; i < 10; i++) {
            limiter.tryAcquire();
        }

        ticker.advance(Duration.ofSeconds(2));

        assertThat(limiter.usedPermits()).isZero();
        assertThat(limiter.tryAcquire()).isTrue();
    }

    @Test
    @DisplayName("A long idle gap is handled in bulk without looping over every bucket")
    void hugeIdleGapHandled() {
        var limiter = limiter();
        limiter.tryAcquire();

        // Years of epochs: a naive loop would iterate over all of them.
        ticker.advance(Duration.ofDays(365));

        assertThat(limiter.usedPermits()).isZero();
        assertThat(limiter.tryAcquire()).isTrue();
    }

    @Test
    @DisplayName("A single bucket degenerates to a fixed window")
    void singleBucketIsFixedWindow() {
        var limiter = new SlidingWindowRateLimiter(5, Duration.ofSeconds(1), 1, ticker);

        for (int i = 0; i < 5; i++) {
            assertThat(limiter.tryAcquire()).isTrue();
        }
        assertThat(limiter.tryAcquire()).isFalse();

        ticker.advance(Duration.ofSeconds(1));
        assertThat(limiter.tryAcquire()).isTrue();
    }

    @Test
    @DisplayName("A multi-permit request is all or nothing")
    void multiPermitIsAtomic() {
        var limiter = limiter();
        assertThat(limiter.tryAcquire(7)).isTrue();
        assertThat(limiter.tryAcquire(7)).isFalse();
        assertThat(limiter.usedPermits()).isEqualTo(7);
        assertThat(limiter.tryAcquire(3)).isTrue();
    }

    @Test
    @DisplayName("Invalid configuration and arguments are refused")
    void validation() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new SlidingWindowRateLimiter(0, Duration.ofSeconds(1), 10, ticker));
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new SlidingWindowRateLimiter(1, Duration.ZERO, 10, ticker));
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new SlidingWindowRateLimiter(1, Duration.ofSeconds(1), 0, ticker));
        // Cannot subdivide 5ns into 10 buckets.
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new SlidingWindowRateLimiter(1, Duration.ofNanos(5), 10, ticker));
        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> limiter().tryAcquire(-1));
    }

    @Test
    @DisplayName("Bucket selection is correct for negative nanoTime")
    void worksWithNegativeNanoTime() {
        // floorDiv and floorMod are required here: plain / and % truncate toward zero,
        // which makes two adjacent negative instants share a bucket.
        ManualTicker negative = new ManualTicker(-5_000_000_000L);
        var limiter = new SlidingWindowRateLimiter(2, Duration.ofSeconds(1), 10, negative);

        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isFalse();

        negative.advance(Duration.ofSeconds(2));
        assertThat(limiter.tryAcquire()).isTrue();
    }
}
