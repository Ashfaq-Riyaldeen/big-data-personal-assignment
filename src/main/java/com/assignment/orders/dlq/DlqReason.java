package com.assignment.orders.dlq;

/**
 * Why a message ended up in the dead letter queue.
 *
 * <p>Recording the reason as a first-class value, rather than only an exception message,
 * is what makes a DLQ operationally useful: it lets you answer "are we shipping bad data,
 * or is a downstream service sick?" at a glance instead of by reading stack traces.
 */
public enum DlqReason {

    /**
     * The bytes on the topic were not a valid Avro record for the registered schema -- a
     * poison pill. Never retried: the same bytes will fail identically forever.
     */
    DESERIALIZATION_FAILED("poison"),

    /**
     * The record decoded cleanly but broke a business rule (negative price, blank identifier).
     * Never retried: no amount of waiting turns a negative price positive.
     */
    VALIDATION_FAILED("validation"),

    /**
     * Processing kept failing with an error that looked transient, and the retry budget ran
     * out. This is the only reason that arrives having already been retried.
     */
    RETRIES_EXHAUSTED("exhausted"),

    /**
     * Something we did not anticipate. Treated as non-retryable so an unknown fault cannot
     * wedge a partition in an infinite retry loop.
     */
    UNEXPECTED_ERROR("unexpected");

    private final String shortLabel;

    DlqReason(String shortLabel) {
        this.shortLabel = shortLabel;
    }

    /** Compact label for the dashboard, where horizontal space is scarce. */
    public String shortLabel() {
        return shortLabel;
    }
}
