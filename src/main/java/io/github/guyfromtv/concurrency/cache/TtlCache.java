package io.github.guyfromtv.concurrency.cache;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;

/**
 * A thread-safe cache that bounds both how much it keeps and how long it keeps it.
 *
 * <p>Two independent limits apply. A per-entry <em>time to live</em> drops stale values,
 * and a <em>maximum size</em> drops the least recently used values once the cache is
 * full. Either alone is insufficient: TTL without a size bound grows without limit under
 * a flood of distinct keys, and a size bound without TTL serves arbitrarily old data.
 *
 * @param <K> key type; must have stable {@code hashCode}/{@code equals}
 * @param <V> value type
 */
public interface TtlCache<K, V> {

    /**
     * Returns the live value for {@code key}, or empty if absent or expired.
     *
     * <p>{@code Optional} rather than a nullable return: it removes the ambiguity between
     * "absent" and "present but null", and this cache forbids null values outright. The
     * wrapper costs an allocation per lookup, which is the deliberate trade for an
     * unambiguous API.
     */
    Optional<V> get(K key);

    /**
     * Returns the cached value, computing and storing it with {@code loader} if absent.
     *
     * <p>The loader runs at most once per key across all threads: concurrent callers for
     * a missing key block until the first finishes and then share its result. Without
     * that guarantee, a popular key expiring under load triggers a stampede where every
     * in-flight request recomputes the same value.
     *
     * <p>The loader is called outside any cache lock, so a slow loader never blocks
     * unrelated keys.
     *
     * @throws NullPointerException if the loader returns null
     */
    V get(K key, Function<? super K, ? extends V> loader);

    /** Stores a value under the cache's default TTL. */
    void put(K key, V value);

    /** Stores a value with a TTL that overrides the default for this entry only. */
    void put(K key, V value, Duration ttl);

    /** Returns true if an entry was present and has now been removed. */
    boolean remove(K key);

    void clear();

    /**
     * The number of entries currently held.
     *
     * <p>May include entries that are past their TTL but not yet noticed; this cache
     * expires lazily on access rather than running a background sweeper, so there is no
     * thread to maintain and no cost when the cache is idle.
     */
    int size();

    CacheStats stats();
}
