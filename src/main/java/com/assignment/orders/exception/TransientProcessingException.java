package com.assignment.orders.exception;

/**
 * A failure that is expected to resolve itself: a downstream timeout, a dropped connection,
 * a service briefly returning 503.
 *
 * <p>Records that fail this way are retried with exponential backoff. Only once the retry
 * budget is exhausted do they go to the dead letter queue.
 */
public class TransientProcessingException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public TransientProcessingException(String message) {
        super(message);
    }

    public TransientProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
