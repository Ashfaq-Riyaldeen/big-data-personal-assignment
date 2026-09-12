package com.ashfaq.bigdata.consumer.aggregate;

import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * The real-time aggregate: how many orders succeeded, their total value, and the running
 * average price.
 *
 * <p>Two design points matter more than the arithmetic:
 *
 * <ul>
 *   <li><b>Counting is idempotent.</b> The set of already-counted order identifiers is checked
 *       and updated inside the same lock as the counter, so an order that fails, is retried and
 *       then succeeds contributes exactly once. Without this, retry handling would silently
 *       inflate the average - the specific mistake the assignment calls out.</li>
 *   <li><b>Reads are consistent.</b> {@link #snapshot()} returns all three values taken together
 *       under the lock. The listener thread writes while an HTTP thread reads, so fetching the
 *       count and the total separately could return a pair that never actually existed.</li>
 * </ul>
 *
 * <p>The total is a {@code double} rather than a {@code float}: prices arrive as floats, but
 * accumulating many of them in float precision drifts, and the average is the headline number.
 */
@Component
public class RunningAverage {

    /**
     * Grows without bound, which is fine for an assignment processing a handful of orders. A
     * production system would externalise this - a compacted Kafka topic, or a store with a
     * retention window - rather than keeping every identifier in memory forever.
     */
    private final Set<String> countedOrderIds = new HashSet<>();

    private long count;
    private double total;

    /**
     * Records a successfully processed order.
     *
     * @return the new aggregate, or empty if this order was already counted
     */
    public synchronized Optional<Snapshot> recordSuccess(String orderId, float price) {
        if (!countedOrderIds.add(orderId)) {
            return Optional.empty();
        }
        count++;
        total += price;
        return Optional.of(snapshotInternal());
    }

    /** The current aggregate. */
    public synchronized Snapshot snapshot() {
        return snapshotInternal();
    }

    private Snapshot snapshotInternal() {
        double average = count == 0 ? 0.0 : total / count;
        return new Snapshot(count, total, average);
    }

    /** An immutable, internally consistent view of the aggregate. */
    public record Snapshot(long count, double total, double average) {
    }
}
