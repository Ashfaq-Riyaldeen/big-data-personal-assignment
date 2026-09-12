package com.ashfaq.bigdata.consumer;

import com.ashfaq.bigdata.avro.Order;
import com.ashfaq.bigdata.consumer.exception.PermanentOrderException;
import com.ashfaq.bigdata.consumer.exception.TemporaryOrderException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The failure rules are what make the demonstration deterministic, so each one is pinned here.
 */
class OrderProcessorTest {

    private static final int TOTAL_ATTEMPTS = 3;

    private final OrderProcessor processor = new OrderProcessor(TOTAL_ATTEMPTS);

    @ParameterizedTest(name = "attempt {0}")
    @ValueSource(ints = {1, 2, 3})
    @DisplayName("a normal order with a positive price succeeds on any attempt")
    void normalOrderSucceeds(int attempt) {
        assertThatCode(() -> processor.process(order("1001", "Item1", 100.0f), attempt))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest(name = "attempt {0}")
    @ValueSource(ints = {1, 2})
    @DisplayName("TEMP_FAIL raises a temporary failure on attempts 1 and 2")
    void tempFailFailsOnEarlyAttempts(int attempt) {
        assertThatThrownBy(() -> processor.process(order("1003", "TEMP_FAIL", 500.0f), attempt))
                .isInstanceOf(TemporaryOrderException.class)
                .hasMessageContaining("attempt " + attempt + "/" + TOTAL_ATTEMPTS);
    }

    @Test
    @DisplayName("TEMP_FAIL succeeds on attempt 3")
    void tempFailRecoversOnThirdAttempt() {
        assertThatCode(() -> processor.process(order("1003", "TEMP_FAIL", 500.0f), 3))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest(name = "attempt {0}")
    @ValueSource(ints = {1, 2, 3, 4, 10})
    @DisplayName("ALWAYS_FAIL raises a temporary failure on every attempt, so retries exhaust")
    void alwaysFailNeverRecovers(int attempt) {
        assertThatThrownBy(() -> processor.process(order("1005", "ALWAYS_FAIL", 700.0f), attempt))
                .isInstanceOf(TemporaryOrderException.class);
    }

    @ParameterizedTest(name = "price {0}")
    @ValueSource(floats = {-10.0f, -0.01f, 0.0f})
    @DisplayName("a zero or negative price is a permanent failure")
    void nonPositivePriceIsPermanent(float price) {
        assertThatThrownBy(() -> processor.process(order("1004", "InvalidItem", price), 1))
                .isInstanceOf(PermanentOrderException.class)
                .hasMessageContaining("greater than zero");
    }

    @Test
    @DisplayName("the price rule wins over the product rule - bad data is never retried")
    void priceRuleTakesPrecedenceOverProductRule() {
        // TEMP_FAIL would normally be retried, but a negative price makes the order permanently
        // invalid regardless. Retrying it would waste attempts for nothing.
        assertThatThrownBy(() -> processor.process(order("x", "TEMP_FAIL", -1.0f), 1))
                .isInstanceOf(PermanentOrderException.class);
    }

    @Test
    @DisplayName("the attempt number passed in drives the outcome, not any hidden state")
    void attemptNumberIsTheOnlyState() {
        Order tempFail = order("1003", "TEMP_FAIL", 500.0f);

        // Processing attempt 3 first, then attempt 1, must behave the same as the other way
        // round: there is no counter inside the processor to get out of step.
        assertThatCode(() -> processor.process(tempFail, 3)).doesNotThrowAnyException();
        assertThatThrownBy(() -> processor.process(tempFail, 1))
                .isInstanceOf(TemporaryOrderException.class);
        assertThatCode(() -> processor.process(tempFail, 3)).doesNotThrowAnyException();
    }

    private static Order order(String id, String product, float price) {
        return Order.newBuilder().setOrderId(id).setProduct(product).setPrice(price).build();
    }
}
