package com.assignment.orders.consumer;

import com.assignment.orders.avro.Order;
import com.assignment.orders.exception.PermanentProcessingException;
import com.assignment.orders.exception.TransientProcessingException;

import java.util.random.RandomGenerator;

/**
 * The unit of work performed on each decoded order: validate it, then hand it to the
 * (simulated) downstream service that would settle the payment.
 *
 * <p>Both failure modes the assignment cares about originate here:
 * <ul>
 *   <li>{@link PermanentProcessingException} for a record that breaks a business rule. These
 *       are produced deliberately by the producer's fault injection.</li>
 *   <li>{@link TransientProcessingException} for a simulated downstream outage, thrown with a
 *       configurable probability so retries can be demonstrated on demand.</li>
 * </ul>
 *
 * <p>The randomness is injected, so a test can force a failure or guarantee success rather than
 * hoping the dice fall the right way.
 */
public final class OrderProcessor {

    /**
     * Anything dearer than this is treated as a data-entry fault rather than a real order.
     * A sanity ceiling like this is what stops one absurd record from dragging the running
     * average somewhere meaningless.
     */
    public static final double DEFAULT_MAX_ACCEPTABLE_PRICE = 1_000_000.0;

    private static final String[] OUTAGE_MESSAGES = {
            "payment gateway did not respond within 2000ms",
            "inventory service returned 503 Service Unavailable",
            "connection reset while committing to the ledger",
            "downstream rate limit hit, retry after backoff"
    };

    private final double transientFailureRate;
    private final double maxAcceptablePrice;
    private final RandomGenerator random;

    public OrderProcessor(double transientFailureRate, double maxAcceptablePrice, RandomGenerator random) {
        if (transientFailureRate < 0.0 || transientFailureRate > 1.0) {
            throw new IllegalArgumentException(
                    "transientFailureRate must be within [0, 1], got " + transientFailureRate);
        }
        this.transientFailureRate = transientFailureRate;
        this.maxAcceptablePrice = maxAcceptablePrice;
        this.random = random;
    }

    /**
     * Processes one order.
     *
     * @throws PermanentProcessingException if the order is invalid and always will be
     * @throws TransientProcessingException if the simulated downstream call failed
     */
    public void process(Order order) {
        validate(order);
        callDownstream(order);
    }

    /** Business rules. Every violation here is permanent by definition. */
    private void validate(Order order) {
        if (order == null) {
            throw new PermanentProcessingException("order is null");
        }

        String orderId = order.getOrderId();
        if (orderId == null || orderId.isBlank()) {
            throw new PermanentProcessingException("orderId is blank");
        }

        String product = order.getProduct();
        if (product == null || product.isBlank()) {
            throw new PermanentProcessingException("product is blank for order " + orderId);
        }

        float price = order.getPrice();
        if (Float.isNaN(price) || Float.isInfinite(price)) {
            throw new PermanentProcessingException("price is not a finite number for order " + orderId);
        }
        if (price <= 0.0f) {
            throw new PermanentProcessingException(
                    "price must be greater than zero, got " + price + " for order " + orderId);
        }
        if (price > maxAcceptablePrice) {
            throw new PermanentProcessingException(
                    "price " + price + " exceeds the maximum acceptable price of "
                            + maxAcceptablePrice + " for order " + orderId);
        }
    }

    /**
     * Stands in for the real downstream call -- a payment gateway, a ledger write, an inventory
     * reservation. Fails with the configured probability to exercise the retry path.
     */
    private void callDownstream(Order order) {
        if (transientFailureRate <= 0.0) {
            return;
        }
        if (random.nextDouble() < transientFailureRate) {
            String message = OUTAGE_MESSAGES[random.nextInt(OUTAGE_MESSAGES.length)];
            throw new TransientProcessingException(message);
        }
    }
}
