package com.ashfaq.bigdata.consumer;

import com.ashfaq.bigdata.avro.Order;
import com.ashfaq.bigdata.consumer.exception.PermanentOrderException;
import com.ashfaq.bigdata.consumer.exception.TemporaryOrderException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Business rules for an incoming order.
 *
 * <p>Kept separate from the listener so the rules can be unit tested without Kafka, and so there
 * is one obvious place to look for "what makes an order fail".
 *
 * <p>The failure behaviour is <b>deterministic, never random</b>. A recorded demonstration has to
 * produce the same result every time, and a reviewer has to be able to reproduce it, so the
 * outcome is driven entirely by the product name and the price:
 *
 * <table border="1">
 *   <caption>Failure rules</caption>
 *   <tr><th>Input</th><th>Behaviour</th></tr>
 *   <tr><td>price at or below zero</td><td>permanent failure, never retried</td></tr>
 *   <tr><td>product {@code TEMP_FAIL}</td><td>fails the first two attempts, succeeds on the third</td></tr>
 *   <tr><td>product {@code ALWAYS_FAIL}</td><td>fails every attempt, so retries are exhausted</td></tr>
 *   <tr><td>anything else with a positive price</td><td>succeeds</td></tr>
 * </table>
 */
@Component
public class OrderProcessor {

    /** Fails until this attempt, then succeeds. With three total attempts, that is the last one. */
    static final int TEMP_FAIL_SUCCEEDS_ON_ATTEMPT = 3;

    static final String TEMPORARY_FAILURE_PRODUCT = "TEMP_FAIL";
    static final String ALWAYS_FAILURE_PRODUCT = "ALWAYS_FAIL";

    private final int totalAttempts;

    public OrderProcessor(@Value("${app.retry.attempts:3}") int totalAttempts) {
        this.totalAttempts = totalAttempts;
    }

    /**
     * Validates and processes an order.
     *
     * @param order   the deserialised order
     * @param attempt which processing attempt this is, starting at 1
     * @throws PermanentOrderException if the order can never be processed successfully
     * @throws TemporaryOrderException if the order might succeed on a later attempt
     */
    public void process(Order order, int attempt) {
        // Checked first: a bad price is wrong in the data itself, so it is permanent regardless
        // of which product it claims to be.
        if (order.getPrice() <= 0f) {
            throw new PermanentOrderException(
                    "Price must be greater than zero, but was " + order.getPrice());
        }

        String product = order.getProduct();

        // The messages are kept short: they are echoed in the [RETRY] and [DLQ] log lines, which
        // must stay on one line at a demonstration-sized font.
        if (ALWAYS_FAILURE_PRODUCT.equals(product)) {
            throw new TemporaryOrderException(
                    "Downstream unavailable (attempt " + attempt + "/" + totalAttempts + ")");
        }

        if (TEMPORARY_FAILURE_PRODUCT.equals(product) && attempt < TEMP_FAIL_SUCCEEDS_ON_ATTEMPT) {
            throw new TemporaryOrderException(
                    "Transient failure (attempt " + attempt + "/" + totalAttempts + ")");
        }

        // A real system would persist the order or call downstream services here. For this
        // assignment, reaching this point is what "processed successfully" means.
    }
}
