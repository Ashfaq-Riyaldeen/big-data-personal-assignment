package com.ashfaq.bigdata.consumer.exception;

/**
 * A failure that might not happen again - a downstream service briefly unavailable, a timeout,
 * a transient lock.
 *
 * <p>Orders failing this way are republished to the retry topics and processed again after a
 * delay. Only when every attempt is used up do they reach the dead letter queue.
 */
public class TemporaryOrderException extends RuntimeException {

    public TemporaryOrderException(String message) {
        super(message);
    }
}
