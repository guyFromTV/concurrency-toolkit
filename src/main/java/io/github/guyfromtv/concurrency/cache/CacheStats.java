package io.github.guyfromtv.concurrency.cache;

/**
 * A point-in-time snapshot of cache counters.
 *
 * <p>The numbers are read from separate counters without a global lock, so a snapshot
 * taken under concurrent traffic is internally consistent only to within a few
 * operations. That is the right trade for statistics: making them exact would mean
 * serialising every cache hit behind one lock.
 *
 * @param hits lookups that found a live entry
 * @param misses lookups that found nothing, or found an entry already expired
 * @param evictions entries dropped because a segment was over capacity
 * @param expirations entries discovered past their TTL and discarded
 * @param loads times a loader function actually ran
 */
public record CacheStats(long hits, long misses, long evictions, long expirations, long loads) {

    public long requests() {
        return hits + misses;
    }

    /** Returns 1.0 for an untouched cache, so callers need not special-case zero. */
    public double hitRate() {
        long requests = requests();
        return requests == 0 ? 1.0 : (double) hits / requests;
    }

    @Override
    public String toString() {
        return "CacheStats[hits=%d, misses=%d, hitRate=%.3f, evictions=%d, expirations=%d, loads=%d]"
                .formatted(hits, misses, hitRate(), evictions, expirations, loads);
    }
}
