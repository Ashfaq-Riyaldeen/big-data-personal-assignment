package com.assignment.orders.exception;

/**
 * A failure that will never succeed no matter how many times it is retried: a negative price,
 * a blank order identifier, a field that violates a business rule.
 *
 * <p>Records that fail this way skip the retry loop entirely and go straight to the dead letter
 * queue. Retrying them would burn the retry budget, stall the partition, and still fail.
 */
public class PermanentProcessingException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public PermanentProcessingException(String message) {
        super(message);
    }

    public PermanentProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
