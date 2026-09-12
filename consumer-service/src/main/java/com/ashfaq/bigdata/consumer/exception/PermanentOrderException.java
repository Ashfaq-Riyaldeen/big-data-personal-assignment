package com.ashfaq.bigdata.consumer.exception;

/**
 * An order that will never succeed, no matter how many times it is processed.
 *
 * <p>A negative price is wrong in the data itself, so retrying wastes attempts and delays the
 * queue for no benefit. Orders failing this way go straight to the dead letter queue, skipping
 * the retry topics entirely.
 */
public class PermanentOrderException extends RuntimeException {

    public PermanentOrderException(String message) {
        super(message);
    }
}
