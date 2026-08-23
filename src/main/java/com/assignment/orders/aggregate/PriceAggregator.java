package com.assignment.orders.aggregate;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Real-time price aggregation: the running average the assignment asks for, plus the extra
 * views that make the live demo legible.
 *
 * <p>Three things are tracked at once:
 * <ul>
 *   <li><b>Lifetime</b> statistics over every order ever processed ({@link RunningStats}).</li>
 *   <li><b>Per product</b> statistics, so you can see Item3 running dearer than Item1.</li>
 *   <li>A <b>sliding window</b> over the last N seconds. The lifetime average converges and
 *       then barely moves, which looks static on screen and hides a shift in the stream. The
 *       windowed average reacts immediately, and is what "real-time" really means here.</li>
 * </ul>
 *
 * <p>The clock is passed in rather than read internally. That keeps window eviction fully
 * deterministic under test -- no sleeping, no flakiness.
 *
 * <p>All mutating and reading methods are synchronised because the consumer thread writes
 * while the dashboard thread reads. Reads return an immutable {@link Snapshot} so the
 * dashboard cannot observe a half-updated state.
 */
public final class PriceAggregator {

    /** One retained sample inside the sliding window. */
    private record Sample(long timestampMillis, double price) {
    }

    private final long windowMillis;

    private final RunningStats lifetime = new RunningStats();
    private final Map<String, RunningStats> byProduct = new TreeMap<>();
    private final Deque<Sample> window = new ArrayDeque<>();

    public PriceAggregator(int windowSeconds) {
        if (windowSeconds <= 0) {
            throw new IllegalArgumentException("windowSeconds must be positive, got " + windowSeconds);
        }
        this.windowMillis = windowSeconds * 1000L;
    }

    /** Records one successfully processed order. */
    public synchronized void record(String product, double price, long nowMillis) {
        lifetime.add(price);
        byProduct.computeIfAbsent(product, key -> new RunningStats()).add(price);

        window.addLast(new Sample(nowMillis, price));
        evictExpired(nowMillis);
    }

    /** Immutable view of every figure the dashboard needs, consistent as of {@code nowMillis}. */
    public synchronized Snapshot snapshot(long nowMillis) {
        evictExpired(nowMillis);

        double windowSum = 0.0;
        for (Sample sample : window) {
            windowSum += sample.price();
        }
        int windowCount = window.size();
        double windowMean = windowCount == 0 ? 0.0 : windowSum / windowCount;

        List<ProductStat> products = new ArrayList<>(byProduct.size());
        for (Map.Entry<String, RunningStats> entry : byProduct.entrySet()) {
            RunningStats stats = entry.getValue();
            products.add(new ProductStat(
                    entry.getKey(), stats.count(), stats.mean(), stats.min(), stats.max()));
        }
        products.sort(Comparator.comparing(ProductStat::product));

        return new Snapshot(
                lifetime.count(),
                lifetime.mean(),
                lifetime.min(),
                lifetime.max(),
                lifetime.stdDev(),
                windowCount,
                windowMean,
                windowMillis / 1000.0,
                List.copyOf(products));
    }

    /** Drops samples that have fallen out of the back of the sliding window. */
    private void evictExpired(long nowMillis) {
        long cutoff = nowMillis - windowMillis;
        while (!window.isEmpty() && window.peekFirst().timestampMillis() <= cutoff) {
            window.removeFirst();
        }
    }

    /** Lifetime statistics for a single product. */
    public record ProductStat(String product, long count, double mean, double min, double max) {
    }

    /**
     * A consistent point-in-time view of the aggregation state.
     *
     * @param count        lifetime number of orders aggregated
     * @param mean         lifetime running average price -- the assignment's headline figure
     * @param windowCount  orders seen inside the sliding window
     * @param windowMean   average price inside the sliding window
     * @param windowSeconds width of that window, for labelling
     */
    public record Snapshot(
            long count,
            double mean,
            double min,
            double max,
            double stdDev,
            long windowCount,
            double windowMean,
            double windowSeconds,
            List<ProductStat> perProduct) {

        /** Orders per second, measured across the sliding window. */
        public double throughputPerSecond() {
            return windowSeconds <= 0 ? 0.0 : windowCount / windowSeconds;
        }
    }
}
