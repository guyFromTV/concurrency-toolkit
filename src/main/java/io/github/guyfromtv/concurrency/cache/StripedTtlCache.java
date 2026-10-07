package io.github.guyfromtv.concurrency.cache;

import io.github.guyfromtv.concurrency.time.Ticker;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * A {@link TtlCache} that splits its contents across independently locked segments.
 *
 * <h2>Why striping</h2>
 *
 * <p>An LRU cache has to mutate shared order on <em>every read</em> -- a hit moves its
 * entry to the front of the recency list. That makes the obvious implementation, one lock
 * around a {@code LinkedHashMap}, serialise all traffic including reads, so throughput
 * stops improving past one core.
 *
 * <p>Here the key space is divided into {@code 2^n} segments, each with its own lock, map
 * and recency list. Two threads touching keys in different segments never contend. The
 * cost is that eviction is per-segment, so recency is <em>approximately</em> global: a
 * segment at capacity evicts its own least-recently-used entry even if a globally colder
 * entry exists elsewhere. For caching that is a good trade, and it is the same one made
 * by Guava's cache and by pre-Java-8 {@code ConcurrentHashMap}.
 *
 * <h2>Why an intrusive linked list</h2>
 *
 * <p>Each entry <em>is</em> a list node, holding its own {@code prev}/{@code next}. Moving
 * an entry to the front on a hit therefore costs O(1) with no lookup and no allocation,
 * because the map value is already the node that must be unlinked. Keeping a separate
 * list would mean searching it to find the entry to move, making every hit O(n).
 *
 * <h2>Expiry</h2>
 *
 * <p>Entries expire lazily, when something looks at them. There is no sweeper thread: an
 * idle cache costs nothing, and there is no lifecycle to shut down. The visible
 * consequence is that {@link #size()} can count entries that are already dead.
 */
public final class StripedTtlCache<K, V> implements TtlCache<K, V> {

    private final Segment<K, V>[] segments;
    private final int segmentMask;
    private final long defaultTtlNanos;
    private final Ticker ticker;

    /**
     * In-flight loads, keyed so concurrent callers for one key share a single
     * computation. Held outside the segment locks because user code runs here.
     */
    private final ConcurrentHashMap<K, CompletableFuture<V>> loading = new ConcurrentHashMap<>();

    /**
     * {@link LongAdder} rather than {@link java.util.concurrent.atomic.AtomicLong}: under
     * contention it spreads increments over several cells instead of making every thread
     * CAS the same word, which is exactly the hot-counter case it exists for.
     */
    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();
    private final LongAdder evictions = new LongAdder();
    private final LongAdder expirations = new LongAdder();
    private final LongAdder loads = new LongAdder();

    @SuppressWarnings("unchecked")
    private StripedTtlCache(Builder<K, V> builder) {
        int segmentCount = tableSizeFor(builder.concurrencyLevel);
        this.segments = new Segment[segmentCount];
        this.segmentMask = segmentCount - 1;
        this.ticker = builder.ticker;
        this.defaultTtlNanos = builder.defaultTtl.toNanos();

        // Round the per-segment capacity up, so the total is never below the requested
        // maximum. Rounding down would silently hold less than asked for.
        int perSegment = Math.max(1, ceilDiv(builder.maximumSize, segmentCount));
        for (int i = 0; i < segmentCount; i++) {
            this.segments[i] = new Segment<>(perSegment);
        }
    }

    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    @Override
    public Optional<V> get(K key) {
        Objects.requireNonNull(key, "key");
        Segment<K, V> segment = segmentFor(key);
        long now = ticker.nanoTime();

        segment.lock.lock();
        try {
            Node<K, V> node = segment.table.get(key);
            if (node == null) {
                misses.increment();
                return Optional.empty();
            }
            if (node.isExpiredAt(now)) {
                segment.unlink(node);
                segment.table.remove(key);
                expirations.increment();
                misses.increment();
                return Optional.empty();
            }
            segment.moveToFront(node);
            hits.increment();
            return Optional.of(node.value);
        } finally {
            segment.lock.unlock();
        }
    }

    @Override
    public V get(K key, Function<? super K, ? extends V> loader) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(loader, "loader");

        Optional<V> cached = get(key);
        if (cached.isPresent()) {
            return cached.get();
        }

        // Single-flight: whoever installs their future first does the work, everyone
        // else waits on it. putIfAbsent makes that decision atomic.
        CompletableFuture<V> mine = new CompletableFuture<>();
        CompletableFuture<V> inFlight = loading.putIfAbsent(key, mine);
        if (inFlight != null) {
            return join(inFlight);
        }

        try {
            V value = loader.apply(key);
            Objects.requireNonNull(value, "loader returned null for key " + key);
            put(key, value);
            loads.increment();
            mine.complete(value);
            return value;
        } catch (RuntimeException | Error failure) {
            // Waiters must see the failure rather than hang forever.
            mine.completeExceptionally(failure);
            throw failure;
        } finally {
            // Remove only our own future, never a newer one installed after ours.
            loading.remove(key, mine);
        }
    }

    private V join(CompletableFuture<V> future) {
        try {
            return future.join();
        } catch (CompletionException wrapped) {
            Throwable cause = wrapped.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw wrapped;
        }
    }

    @Override
    public void put(K key, V value) {
        put(key, value, null);
    }

    @Override
    public void put(K key, V value, Duration ttl) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        long ttlNanos = ttl == null ? defaultTtlNanos : ttl.toNanos();
        if (ttlNanos <= 0) {
            throw new IllegalArgumentException("ttl must be positive");
        }

        Segment<K, V> segment = segmentFor(key);
        long expiresAt = ticker.nanoTime() + ttlNanos;

        segment.lock.lock();
        try {
            Node<K, V> existing = segment.table.get(key);
            if (existing != null) {
                existing.value = value;
                existing.expiresAtNanos = expiresAt;
                segment.moveToFront(existing);
                return;
            }

            Node<K, V> node = new Node<>(key, value, expiresAt);
            segment.table.put(key, node);
            segment.addFirst(node);

            if (segment.table.size() > segment.capacity) {
                Node<K, V> coldest = segment.removeLast();
                if (coldest != null) {
                    segment.table.remove(coldest.key);
                    evictions.increment();
                }
            }
        } finally {
            segment.lock.unlock();
        }
    }

    @Override
    public boolean remove(K key) {
        Objects.requireNonNull(key, "key");
        Segment<K, V> segment = segmentFor(key);
        segment.lock.lock();
        try {
            Node<K, V> node = segment.table.remove(key);
            if (node == null) {
                return false;
            }
            segment.unlink(node);
            return true;
        } finally {
            segment.lock.unlock();
        }
    }

    @Override
    public void clear() {
        // Each segment is cleared under its own lock. The cache is therefore never
        // globally frozen, and a concurrent writer may land in an already-cleared
        // segment -- acceptable for a cache, and documented rather than pretended away.
        for (Segment<K, V> segment : segments) {
            segment.lock.lock();
            try {
                segment.table.clear();
                segment.resetList();
            } finally {
                segment.lock.unlock();
            }
        }
    }

    @Override
    public int size() {
        int total = 0;
        for (Segment<K, V> segment : segments) {
            segment.lock.lock();
            try {
                total += segment.table.size();
            } finally {
                segment.lock.unlock();
            }
        }
        return total;
    }

    @Override
    public CacheStats stats() {
        return new CacheStats(hits.sum(), misses.sum(), evictions.sum(), expirations.sum(), loads.sum());
    }

    /** Exposed for tests and diagnostics. */
    public int segmentCount() {
        return segments.length;
    }

    private Segment<K, V> segmentFor(K key) {
        return segments[spread(key.hashCode()) & segmentMask];
    }

    /**
     * Mixes the high bits of a hash down into the low bits.
     *
     * <p>Only the low bits select a segment, so a key type whose hashes differ solely in
     * their high bits -- {@code Integer} multiples of 16, say -- would otherwise pile
     * into one segment and undo the striping entirely. Same reasoning, and nearly the
     * same code, as {@code ConcurrentHashMap.spread}.
     */
    private static int spread(int hash) {
        int h = hash ^ (hash >>> 16);
        return h & 0x7fff_ffff;
    }

    private static int tableSizeFor(int requested) {
        int n = Integer.highestOneBit(Math.max(1, requested));
        return n < requested ? n << 1 : n;
    }

    private static int ceilDiv(int dividend, int divisor) {
        return (dividend + divisor - 1) / divisor;
    }

    /** One independently locked slice of the cache. */
    private static final class Segment<K, V> {

        private final ReentrantLock lock = new ReentrantLock();
        private final Map<K, Node<K, V>> table;
        private final int capacity;

        /**
         * Sentinel head and tail, so linking and unlinking never need a null check:
         * every real node always has both neighbours. Most-recently-used sits after
         * {@code head}; the eviction victim sits before {@code tail}.
         */
        private final Node<K, V> head = new Node<>(null, null, 0);
        private final Node<K, V> tail = new Node<>(null, null, 0);

        Segment(int capacity) {
            this.capacity = capacity;
            // Sized to avoid a rehash just as the segment fills.
            this.table = new HashMap<>(Math.max(16, capacity * 2));
            resetList();
        }

        void resetList() {
            head.next = tail;
            tail.prev = head;
        }

        void addFirst(Node<K, V> node) {
            node.prev = head;
            node.next = head.next;
            head.next.prev = node;
            head.next = node;
        }

        void unlink(Node<K, V> node) {
            if (node.prev != null) {
                node.prev.next = node.next;
            }
            if (node.next != null) {
                node.next.prev = node.prev;
            }
            node.prev = null;
            node.next = null;
        }

        void moveToFront(Node<K, V> node) {
            if (head.next == node) {
                return;
            }
            unlink(node);
            addFirst(node);
        }

        Node<K, V> removeLast() {
            Node<K, V> last = tail.prev;
            if (last == head) {
                return null;
            }
            unlink(last);
            return last;
        }
    }

    /** A cache entry that is also its own node in the segment's recency list. */
    private static final class Node<K, V> {

        private final K key;
        private V value;
        private long expiresAtNanos;
        private Node<K, V> prev;
        private Node<K, V> next;

        Node(K key, V value, long expiresAtNanos) {
            this.key = key;
            this.value = value;
            this.expiresAtNanos = expiresAtNanos;
        }

        /**
         * Compares with subtraction, not {@code now > expiresAt}. {@code nanoTime} is
         * allowed to wrap around, and a direct comparison breaks at the wrap while the
         * difference stays correct.
         */
        boolean isExpiredAt(long now) {
            return now - expiresAtNanos >= 0;
        }
    }

    /** Fluent configuration; every value has a usable default except via explicit calls. */
    public static final class Builder<K, V> {

        private int maximumSize = 1_024;
        private Duration defaultTtl = Duration.ofMinutes(5);
        private int concurrencyLevel = 16;
        private Ticker ticker = Ticker.system();

        private Builder() {}

        public Builder<K, V> maximumSize(int maximumSize) {
            if (maximumSize < 1) {
                throw new IllegalArgumentException("maximumSize must be at least 1");
            }
            this.maximumSize = maximumSize;
            return this;
        }

        public Builder<K, V> defaultTtl(Duration defaultTtl) {
            Objects.requireNonNull(defaultTtl, "defaultTtl");
            if (defaultTtl.isZero() || defaultTtl.isNegative()) {
                throw new IllegalArgumentException("defaultTtl must be positive");
            }
            this.defaultTtl = defaultTtl;
            return this;
        }

        /** Rounded up to a power of two; 1 gives a single lock for the whole cache. */
        public Builder<K, V> concurrencyLevel(int concurrencyLevel) {
            if (concurrencyLevel < 1) {
                throw new IllegalArgumentException("concurrencyLevel must be at least 1");
            }
            this.concurrencyLevel = concurrencyLevel;
            return this;
        }

        public Builder<K, V> ticker(Ticker ticker) {
            this.ticker = Objects.requireNonNull(ticker, "ticker");
            return this;
        }

        public StripedTtlCache<K, V> build() {
            return new StripedTtlCache<>(this);
        }
    }
}
