package com.ashfaq.bigdata.consumer;

import com.ashfaq.bigdata.avro.Order;
import com.ashfaq.bigdata.consumer.exception.PermanentOrderException;
import org.springframework.stereotype.Component;

/**
 * Business rules for an incoming order.
 *
 * <p>Kept separate from the listener so the rules can be unit tested without Kafka, and so
 * there is one obvious place to look for "what makes an order fail".
 *
 * <p>Task 7 extends this with the deterministic temporary-failure rules ({@code TEMP_FAIL},
 * {@code ALWAYS_FAIL}); today it enforces the one permanent rule.
 */
@Component
public class OrderProcessor {

    /**
     * Validates and processes an order.
     *
     * @throws PermanentOrderException if the order can never be processed successfully
     */
    public void process(Order order) {
        if (order.getPrice() <= 0f) {
            throw new PermanentOrderException(
                    "Price must be greater than zero, but was " + order.getPrice());
        }

        // A real system would persist the order or call downstream services here. For this
        // assignment, reaching this point is what "processed successfully" means.
    }
}
