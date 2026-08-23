package com.assignment.orders.consumer;

import com.assignment.orders.avro.Order;
import com.assignment.orders.exception.PermanentProcessingException;
import com.assignment.orders.exception.TransientProcessingException;
import com.assignment.orders.support.SequenceRandom;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("OrderProcessor")
class OrderProcessorTest {

    /** No simulated outages, so only validation can fail. */
    private static OrderProcessor reliableProcessor() {
        return new OrderProcessor(0.0, OrderProcessor.DEFAULT_MAX_ACCEPTABLE_PRICE,
                SequenceRandom.always(0.99));
    }

    private static Order order(String orderId, String product, float price) {
        return Order.newBuilder()
                .setOrderId(orderId)
                .setProduct(product)
                .setPrice(price)
                .build();
    }

    @Test
    @DisplayName("accepts a well-formed order")
    void acceptsValidOrder() {
        assertThatCode(() -> reliableProcessor().process(order("1001", "Item1", 249.99f)))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("rejects a null order")
    void rejectsNull() {
        assertThatThrownBy(() -> reliableProcessor().process(null))
                .isInstanceOf(PermanentProcessingException.class)
                .hasMessageContaining("null");
    }

    @Test
    @DisplayName("rejects a blank order identifier")
    void rejectsBlankOrderId() {
        assertThatThrownBy(() -> reliableProcessor().process(order("", "Item1", 10.0f)))
                .isInstanceOf(PermanentProcessingException.class)
                .hasMessageContaining("orderId is blank");
    }

    @Test
    @DisplayName("rejects a blank product name")
    void rejectsBlankProduct() {
        assertThatThrownBy(() -> reliableProcessor().process(order("1001", "   ", 10.0f)))
                .isInstanceOf(PermanentProcessingException.class)
                .hasMessageContaining("product is blank");
    }

    @Test
    @DisplayName("rejects a negative price")
    void rejectsNegativePrice() {
        assertThatThrownBy(() -> reliableProcessor().process(order("1001", "Item1", -5.0f)))
                .isInstanceOf(PermanentProcessingException.class)
                .hasMessageContaining("greater than zero");
    }

    @Test
    @DisplayName("rejects a zero price")
    void rejectsZeroPrice() {
        assertThatThrownBy(() -> reliableProcessor().process(order("1001", "Item1", 0.0f)))
                .isInstanceOf(PermanentProcessingException.class)
                .hasMessageContaining("greater than zero");
    }

    @Test
    @DisplayName("rejects a price beyond the sanity ceiling")
    void rejectsAbsurdPrice() {
        // One absurd record would otherwise drag the running average somewhere meaningless.
        assertThatThrownBy(() -> reliableProcessor().process(order("1001", "Item1", 5_000_000f)))
                .isInstanceOf(PermanentProcessingException.class)
                .hasMessageContaining("exceeds the maximum");
    }

    @Test
    @DisplayName("rejects a non-finite price")
    void rejectsNonFinitePrice() {
        assertThatThrownBy(() -> reliableProcessor().process(order("1001", "Item1", Float.NaN)))
                .isInstanceOf(PermanentProcessingException.class)
                .hasMessageContaining("finite");

        assertThatThrownBy(() -> reliableProcessor().process(order("1001", "Item1", Float.POSITIVE_INFINITY)))
                .isInstanceOf(PermanentProcessingException.class)
                .hasMessageContaining("finite");
    }

    @Test
    @DisplayName("raises a transient failure when the simulated downstream call fails")
    void simulatesTransientOutage() {
        // The draw of 0.0 is below the failure rate, so the outage fires.
        OrderProcessor processor = new OrderProcessor(
                0.5, OrderProcessor.DEFAULT_MAX_ACCEPTABLE_PRICE, SequenceRandom.always(0.0));

        assertThatThrownBy(() -> processor.process(order("1001", "Item1", 100.0f)))
                .isInstanceOf(TransientProcessingException.class);
    }

    @Test
    @DisplayName("succeeds when the draw lands above the failure rate")
    void succeedsWhenDrawIsAboveTheRate() {
        OrderProcessor processor = new OrderProcessor(
                0.5, OrderProcessor.DEFAULT_MAX_ACCEPTABLE_PRICE, SequenceRandom.always(0.9));

        assertThatCode(() -> processor.process(order("1001", "Item1", 100.0f)))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("validation runs before the downstream call, so invalid orders fail permanently")
    void validationPrecedesTheDownstreamCall() {
        // Even with outages guaranteed, an invalid order must report the permanent fault --
        // otherwise it would be retried pointlessly and land in the DLQ under the wrong reason.
        OrderProcessor alwaysFailing = new OrderProcessor(
                1.0, OrderProcessor.DEFAULT_MAX_ACCEPTABLE_PRICE, SequenceRandom.always(0.0));

        assertThatThrownBy(() -> alwaysFailing.process(order("1001", "Item1", -1.0f)))
                .isInstanceOf(PermanentProcessingException.class);
    }

    @Test
    @DisplayName("rejects a failure rate outside [0, 1]")
    void rejectsInvalidRate() {
        assertThatThrownBy(() -> new OrderProcessor(1.5, 100.0, SequenceRandom.always(0.5)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("transientFailureRate");
    }
}
