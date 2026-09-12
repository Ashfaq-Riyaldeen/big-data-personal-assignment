package com.ashfaq.bigdata.producer.dto;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Confirmation that an order actually reached Kafka.
 *
 * <p>The broker-assigned partition and offset are returned rather than a bare "ok", so the
 * response is evidence in its own right: the same offset can be located on the topic in
 * Kafka UI during the live demonstration.
 */
@Schema(description = "Broker acknowledgement for a published order.")
public record PublishResult(

        @Schema(example = "1001") String orderId,
        @Schema(example = "Item1") String product,
        @Schema(example = "100.0") float price,
        @Schema(example = "orders.v1") String topic,
        @Schema(example = "0") int partition,
        @Schema(example = "0") long offset) {
}
