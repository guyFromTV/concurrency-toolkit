package io.github.guyfromtv.concurrency.ratelimit;

import io.github.guyfromtv.concurrency.cache.StripedTtlCache;
import io.github.guyfromtv.concurrency.cache.TtlCache;
import io.github.guyfromtv.concurrency.time.Ticker;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

/**
 * Applies an independent limit per key -- per user, per API key, per IP.
 *
 * <h2>The leak this avoids</h2>
 *
 * <p>The obvious implementation is {@code ConcurrentHashMap<K, RateLimiter>} with
 * {@code computeIfAbsent}. It works and it leaks: every key ever seen keeps its limiter
 * forever. Keyed by client IP and exposed to the internet, that map is an unbounded
 * allocation driven by strangers, which is a denial-of-service vector rather than a
 * defence against one.
 *
 * <p>So the limiters live in a {@link TtlCache}, which bounds both how many are kept and
 * how long an idle one survives. The cost is that a discarded limiter loses its state, so
 * a key can return after eviction with a full burst. For idle keys that is the intended
 * behaviour. Under genuine pressure the cache may also drop a limiter that is still in
 * use, which hands that key a fresh burst -- an honest trade for a bounded footprint, and
 * the reason {@code maximumKeys} should be set above the expected number of active keys.
 */
public final class KeyedRateLimiter<K> {

    private final TtlCache<K, RateLimiter> limiters;
    private final Function<K, RateLimiter> factory;

    private KeyedRateLimiter(TtlCache<K, RateLimiter> limiters, Function<K, RateLimiter> factory) {
        this.limiters = limiters;
        this.factory = factory;
    }

    /**
     * @param maximumKeys ceiling on tracked keys; set it above the expected active set
     * @param idleTtl how long a key's limiter survives without use
     * @param factory builds the limiter for a key, deciding that key's allowance
     */
    public static <K> KeyedRateLimiter<K> create(
            int maximumKeys, Duration idleTtl, Ticker ticker, Function<K, RateLimiter> factory) {
        Objects.requireNonNull(factory, "factory");
        TtlCache<K, RateLimiter> cache = StripedTtlCache.<K, RateLimiter>builder()
                .maximumSize(maximumKeys)
                .defaultTtl(idleTtl)
                .ticker(Objects.requireNonNull(ticker, "ticker"))
                .build();
        return new KeyedRateLimiter<>(cache, factory);
    }

    public boolean tryAcquire(K key) {
        return tryAcquire(key, 1);
    }

    /**
     * The cache's single-flight loading matters here: without it, two concurrent first
     * requests for one key could each build a limiter and one would be discarded along
     * with the permit it had already granted.
     */
    public boolean tryAcquire(K key, int permits) {
        Objects.requireNonNull(key, "key");
        return limiters.get(key, factory).tryAcquire(permits);
    }

    /** Number of keys currently tracked. */
    public int trackedKeys() {
        return limiters.size();
    }
}
