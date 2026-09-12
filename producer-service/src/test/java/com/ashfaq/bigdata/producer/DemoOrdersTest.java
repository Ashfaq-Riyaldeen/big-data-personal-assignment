package com.ashfaq.bigdata.producer;

import com.ashfaq.bigdata.producer.dto.OrderRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The demonstration orders are the acceptance test data. If they drift, the recorded walkthrough
 * and the README stop agreeing with the code, so the exact values are pinned.
 */
class DemoOrdersTest {

    @Test
    @DisplayName("acceptance case 3: TEMP_FAIL at 500.00")
    void temporaryFailureOrder() {
        OrderRequest order = DemoOrders.temporaryFailure("1003");

        assertThat(order.orderId()).isEqualTo("1003");
        assertThat(order.product()).isEqualTo("TEMP_FAIL");
        assertThat(order.price()).isEqualTo(500.0f);
    }

    @Test
    @DisplayName("acceptance case 4: a negative price that must go straight to the DLQ")
    void permanentFailureOrder() {
        OrderRequest order = DemoOrders.permanentFailure("1004");

        assertThat(order.orderId()).isEqualTo("1004");
        assertThat(order.product()).isEqualTo("InvalidItem");
        assertThat(order.price()).isEqualTo(-10.0f);
    }

    @Test
    @DisplayName("acceptance case 5: ALWAYS_FAIL at 700.00")
    void alwaysFailsOrder() {
        OrderRequest order = DemoOrders.alwaysFails("1005");

        assertThat(order.orderId()).isEqualTo("1005");
        assertThat(order.product()).isEqualTo("ALWAYS_FAIL");
        assertThat(order.price()).isEqualTo(700.0f);
    }

    @Test
    @DisplayName("the order id can be overridden so the demo can be re-run with fresh data")
    void orderIdIsOverridable() {
        assertThat(DemoOrders.temporaryFailure("9003").orderId()).isEqualTo("9003");
        assertThat(DemoOrders.permanentFailure("9004").orderId()).isEqualTo("9004");
        assertThat(DemoOrders.alwaysFails("9005").orderId()).isEqualTo("9005");
    }

    @Test
    @DisplayName("the trigger product names match what the consumer keys off")
    void triggerProductNamesAreStable() {
        // The consumer's OrderProcessor compares against these exact strings.
        assertThat(DemoOrders.TEMPORARY_FAILURE_PRODUCT).isEqualTo("TEMP_FAIL");
        assertThat(DemoOrders.ALWAYS_FAILURE_PRODUCT).isEqualTo("ALWAYS_FAIL");
    }
}
