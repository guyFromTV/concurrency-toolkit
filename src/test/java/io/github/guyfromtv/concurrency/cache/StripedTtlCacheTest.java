package io.github.guyfromtv.concurrency.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.github.guyfromtv.concurrency.support.ManualTicker;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class StripedTtlCacheTest {

    private ManualTicker ticker;

    @BeforeEach
    void setUp() {
        ticker = new ManualTicker();
    }

    private StripedTtlCache<String, String> cache(int maximumSize, Duration ttl) {
        return StripedTtlCache.<String, String>builder()
                .maximumSize(maximumSize)
                .defaultTtl(ttl)
                .ticker(ticker)
                .build();
    }

    @Nested
    @DisplayName("basic behaviour")
    class Basics {

        @Test
        @DisplayName("A stored value can be read back")
        void putThenGet() {
            var cache = cache(10, Duration.ofMinutes(1));
            cache.put("a", "1");

            assertThat(cache.get("a")).contains("1");
            assertThat(cache.get("missing")).isEmpty();
            assertThat(cache.size()).isEqualTo(1);
        }

        @Test
        @DisplayName("Storing the same key twice replaces the value without growing the cache")
        void putReplaces() {
            var cache = cache(10, Duration.ofMinutes(1));
            cache.put("a", "1");
            cache.put("a", "2");

            assertThat(cache.get("a")).contains("2");
            assertThat(cache.size()).isEqualTo(1);
        }

        @Test
        @DisplayName("Removal reports whether anything was there")
        void removeReportsPresence() {
            var cache = cache(10, Duration.ofMinutes(1));
            cache.put("a", "1");

            assertThat(cache.remove("a")).isTrue();
            assertThat(cache.remove("a")).isFalse();
            assertThat(cache.get("a")).isEmpty();
        }

        @Test
        @DisplayName("Clearing empties every segment")
        void clearEmptiesEverything() {
            var cache = cache(100, Duration.ofMinutes(1));
            for (int i = 0; i < 50; i++) {
                cache.put("k" + i, "v" + i);
            }

            cache.clear();

            assertThat(cache.size()).isZero();
            assertThat(cache.get("k0")).isEmpty();
        }

        @Test
        @DisplayName("Nulls are rejected rather than stored")
        void nullsRejected() {
            var cache = cache(10, Duration.ofMinutes(1));

            // A cache that stores null cannot distinguish "absent" from "null value".
            assertThatNullPointerException().isThrownBy(() -> cache.put("a", null));
            assertThatNullPointerException().isThrownBy(() -> cache.put(null, "1"));
            assertThatNullPointerException().isThrownBy(() -> cache.get(null));
        }

        @Test
        @DisplayName("Invalid configuration is refused at build time")
        void invalidConfigurationRejected() {
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> StripedTtlCache.builder().maximumSize(0));
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> StripedTtlCache.builder().defaultTtl(Duration.ZERO));
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> StripedTtlCache.builder().concurrencyLevel(0));
        }

        @Test
        @DisplayName("The segment count is rounded up to a power of two")
        void segmentCountIsPowerOfTwo() {
            // Striping uses a bitmask, which requires a power-of-two table.
            assertThat(StripedTtlCache.builder().concurrencyLevel(1).build().segmentCount())
                    .isEqualTo(1);
            assertThat(StripedTtlCache.builder().concurrencyLevel(5).build().segmentCount())
                    .isEqualTo(8);
            assertThat(StripedTtlCache.builder().concurrencyLevel(16).build().segmentCount())
                    .isEqualTo(16);
        }
    }

    @Nested
    @DisplayName("expiry")
    class Expiry {

        @Test
        @DisplayName("An entry disappears once its TTL has passed")
        void entryExpires() {
            var cache = cache(10, Duration.ofSeconds(30));
            cache.put("a", "1");

            ticker.advance(Duration.ofSeconds(29));
            assertThat(cache.get("a")).as("still inside the TTL").contains("1");

            ticker.advance(Duration.ofSeconds(2));
            assertThat(cache.get("a")).as("past the TTL").isEmpty();
        }

        @Test
        @DisplayName("A per-entry TTL overrides the default")
        void perEntryTtl() {
            var cache = cache(10, Duration.ofHours(1));
            cache.put("short", "1", Duration.ofSeconds(5));
            cache.put("long", "2");

            ticker.advance(Duration.ofSeconds(6));

            assertThat(cache.get("short")).isEmpty();
            assertThat(cache.get("long")).contains("2");
        }

        @Test
        @DisplayName("Expiry is counted and the entry is reclaimed")
        void expiryIsCountedAndReclaimed() {
            var cache = cache(10, Duration.ofSeconds(1));
            cache.put("a", "1");
            ticker.advance(Duration.ofSeconds(2));

            cache.get("a");

            assertThat(cache.stats().expirations()).isEqualTo(1);
            assertThat(cache.stats().misses()).isEqualTo(1);
            // Lazy expiry still has to free the entry when it is noticed.
            assertThat(cache.size()).isZero();
        }

        @Test
        @DisplayName("Re-storing a key resets its TTL")
        void putResetsTtl() {
            var cache = cache(10, Duration.ofSeconds(10));
            cache.put("a", "1");

            ticker.advance(Duration.ofSeconds(9));
            cache.put("a", "2");
            ticker.advance(Duration.ofSeconds(9));

            assertThat(cache.get("a")).contains("2");
        }
    }

    @Nested
    @DisplayName("size bound and recency")
    class Eviction {

        @Test
        @DisplayName("A single segment evicts its least recently used entry")
        void evictsLeastRecentlyUsed() {
            // concurrencyLevel 1 gives exactly one segment, so recency is strictly global
            // and the victim is fully determined.
            var cache = StripedTtlCache.<String, String>builder()
                    .maximumSize(3)
                    .concurrencyLevel(1)
                    .defaultTtl(Duration.ofHours(1))
                    .ticker(ticker)
                    .build();

            cache.put("a", "1");
            cache.put("b", "2");
            cache.put("c", "3");

            // Touching "a" makes "b" the coldest.
            assertThat(cache.get("a")).contains("1");

            cache.put("d", "4");

            assertThat(cache.get("b")).as("coldest entry should have gone").isEmpty();
            assertThat(cache.get("a")).contains("1");
            assertThat(cache.get("c")).contains("3");
            assertThat(cache.get("d")).contains("4");
            assertThat(cache.stats().evictions()).isEqualTo(1);
        }

        @Test
        @DisplayName("The cache never exceeds its maximum size")
        void neverExceedsMaximumSize() {
            var cache = StripedTtlCache.<String, String>builder()
                    .maximumSize(16)
                    .concurrencyLevel(1)
                    .defaultTtl(Duration.ofHours(1))
                    .ticker(ticker)
                    .build();

            for (int i = 0; i < 1_000; i++) {
                cache.put("k" + i, "v" + i);
                assertThat(cache.size()).isLessThanOrEqualTo(16);
            }
        }

        @Test
        @DisplayName("Striped capacity totals at least the requested maximum")
        void stripedCapacityIsNotUndersized() {
            // Per-segment capacity rounds up, so the cache holds at least what was asked
            // for rather than quietly less.
            var cache = StripedTtlCache.<String, String>builder()
                    .maximumSize(100)
                    .concurrencyLevel(8)
                    .defaultTtl(Duration.ofHours(1))
                    .ticker(ticker)
                    .build();

            for (int i = 0; i < 10_000; i++) {
                cache.put("k" + i, "v" + i);
            }

            assertThat(cache.size()).isGreaterThanOrEqualTo(100);
        }
    }

    @Nested
    @DisplayName("loading")
    class Loading {

        @Test
        @DisplayName("The loader runs on a miss and the value is cached")
        void loaderRunsOnceThenCaches() {
            var cache = cache(10, Duration.ofMinutes(1));
            AtomicInteger calls = new AtomicInteger();

            String first = cache.get("a", key -> "loaded-" + calls.incrementAndGet() + "-" + key);
            String second = cache.get("a", key -> "loaded-" + calls.incrementAndGet() + "-" + key);

            assertThat(first).isEqualTo("loaded-1-a");
            assertThat(second).as("second call must come from the cache").isEqualTo("loaded-1-a");
            assertThat(calls).hasValue(1);
            assertThat(cache.stats().loads()).isEqualTo(1);
        }

        @Test
        @DisplayName("The loader runs again once the entry expires")
        void loaderRerunsAfterExpiry() {
            var cache = cache(10, Duration.ofSeconds(10));
            AtomicInteger calls = new AtomicInteger();

            cache.get("a", key -> "v" + calls.incrementAndGet());
            ticker.advance(Duration.ofSeconds(11));
            String reloaded = cache.get("a", key -> "v" + calls.incrementAndGet());

            assertThat(reloaded).isEqualTo("v2");
            assertThat(calls).hasValue(2);
        }

        @Test
        @DisplayName("A loader returning null is rejected, not cached")
        void nullFromLoaderRejected() {
            var cache = cache(10, Duration.ofMinutes(1));

            assertThatNullPointerException().isThrownBy(() -> cache.get("a", key -> null));
            assertThat(cache.get("a")).isEmpty();
        }

        @Test
        @DisplayName("A failing loader propagates and leaves nothing behind")
        void failingLoaderPropagates() {
            var cache = cache(10, Duration.ofMinutes(1));

            assertThatExceptionOfType(IllegalStateException.class)
                    .isThrownBy(() -> cache.get("a", key -> {
                        throw new IllegalStateException("boom");
                    }))
                    .withMessage("boom");

            // A failed load must not poison the key: a later attempt should try again.
            assertThat(cache.get("a", key -> "recovered")).isEqualTo("recovered");
        }
    }

    @Nested
    @DisplayName("statistics")
    class Stats {

        @Test
        @DisplayName("Hits and misses are counted and the rate computed")
        void hitsAndMisses() {
            var cache = cache(10, Duration.ofMinutes(1));
            cache.put("a", "1");

            cache.get("a");
            cache.get("a");
            cache.get("b");

            CacheStats stats = cache.stats();
            assertThat(stats.hits()).isEqualTo(2);
            assertThat(stats.misses()).isEqualTo(1);
            assertThat(stats.requests()).isEqualTo(3);
            assertThat(stats.hitRate()).isCloseTo(2.0 / 3.0, org.assertj.core.data.Offset.offset(1e-9));
        }

        @Test
        @DisplayName("An untouched cache reports a hit rate of 1.0 rather than NaN")
        void emptyStatsHaveNoDivisionByZero() {
            assertThat(cache(10, Duration.ofMinutes(1)).stats().hitRate()).isEqualTo(1.0);
        }
    }
}
