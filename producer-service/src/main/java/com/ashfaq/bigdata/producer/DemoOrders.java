package com.ashfaq.bigdata.producer;

import com.ashfaq.bigdata.producer.dto.OrderRequest;

/**
 * The fixed demonstration orders from the acceptance test cases.
 *
 * <p>These values are deliberately not random. The recorded demonstration has to produce the
 * same running average every time, and a reviewer has to be able to reproduce it exactly.
 *
 * <p>The product names are the triggers the consumer keys its failure rules off:
 * {@code TEMP_FAIL} fails twice then succeeds, {@code ALWAYS_FAIL} never succeeds, and any
 * order priced at or below zero is a permanent validation failure regardless of product.
 */
public final class DemoOrders {

    public static final String TEMPORARY_FAILURE_PRODUCT = "TEMP_FAIL";
    public static final String ALWAYS_FAILURE_PRODUCT = "ALWAYS_FAIL";

    private DemoOrders() {
    }

    /** Acceptance case 3: retried twice, succeeds on the third attempt. */
    public static OrderRequest temporaryFailure(String orderId) {
        return new OrderRequest(orderId, TEMPORARY_FAILURE_PRODUCT, 500.0f);
    }

    /** Acceptance case 4: negative price, permanent failure, straight to the DLQ. */
    public static OrderRequest permanentFailure(String orderId) {
        return new OrderRequest(orderId, "InvalidItem", -10.0f);
    }

    /** Acceptance case 5: retries are exhausted, then the DLQ. */
    public static OrderRequest alwaysFails(String orderId) {
        return new OrderRequest(orderId, ALWAYS_FAILURE_PRODUCT, 700.0f);
    }
}
