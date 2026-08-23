package com.assignment.orders.aggregate;

import com.assignment.orders.dlq.DlqReason;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reliability counters for the consumer: what came in, what succeeded, what had to be retried,
 * and what was ultimately given up on.
 *
 * <p>Kept separate from {@link PriceAggregator} because the two answer different questions.
 * The aggregator answers "what are these orders worth?"; this answers "is the pipeline healthy?".
 * Mixing them would produce one class with two reasons to change.
 *
 * <p>Written by the consumer thread and read by the dashboard thread, so counters are atomic
 * and reads are taken as a {@link Snapshot}.
 */
public final class ProcessingMetrics {

    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong processedOk = new AtomicLong();
    private final AtomicLong retryAttempts = new AtomicLong();
    private final AtomicLong recoveredAfterRetry = new AtomicLong();
    private final AtomicLong deadLettered = new AtomicLong();

    private final Map<DlqReason, AtomicLong> byReason = new EnumMap<>(DlqReason.class);

    private volatile String lastError = "";

    public ProcessingMetrics() {
        for (DlqReason reason : DlqReason.values()) {
            byReason.put(reason, new AtomicLong());
        }
    }

    /** One record was polled from the topic. */
    public void recordConsumed() {
        consumed.incrementAndGet();
    }

    /**
     * A record completed successfully.
     *
     * @param attempts how many attempts it took; more than one means the retry logic rescued it
     */
    public void recordSuccess(int attempts) {
        processedOk.incrementAndGet();
        if (attempts > 1) {
            recoveredAfterRetry.incrementAndGet();
        }
    }

    /** One failed attempt that is going to be retried. */
    public void recordRetryAttempt() {
        retryAttempts.incrementAndGet();
    }

    /** A record was routed to the dead letter queue. */
    public void recordDeadLettered(DlqReason reason) {
        deadLettered.incrementAndGet();
        byReason.get(reason).incrementAndGet();
    }

    public void recordLastError(String message) {
        this.lastError = message == null ? "" : message;
    }

    public Snapshot snapshot() {
        Map<DlqReason, Long> reasons = new EnumMap<>(DlqReason.class);
        byReason.forEach((reason, counter) -> reasons.put(reason, counter.get()));

        return new Snapshot(
                consumed.get(),
                processedOk.get(),
                retryAttempts.get(),
                recoveredAfterRetry.get(),
                deadLettered.get(),
                Map.copyOf(reasons),
                lastError);
    }

    /** A consistent point-in-time view of the reliability counters. */
    public record Snapshot(
            long consumed,
            long processedOk,
            long retryAttempts,
            long recoveredAfterRetry,
            long deadLettered,
            Map<DlqReason, Long> deadLetteredByReason,
            String lastError) {

        public long countFor(DlqReason reason) {
            return deadLetteredByReason.getOrDefault(reason, 0L);
        }

        /**
         * Share of retried records that went on to succeed. A high number is the retry logic
         * doing its job; a low one means we are mostly retrying things that were never going
         * to work, and the error classification deserves another look.
         */
        public double retrySuccessRate() {
            long retried = recoveredAfterRetry + countFor(DlqReason.RETRIES_EXHAUSTED);
            return retried == 0 ? 0.0 : (double) recoveredAfterRetry / retried;
        }
    }
}
