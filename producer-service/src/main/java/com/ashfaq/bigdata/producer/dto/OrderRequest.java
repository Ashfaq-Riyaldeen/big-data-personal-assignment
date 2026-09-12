package com.ashfaq.bigdata.producer.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * An order as supplied over REST, before it becomes an Avro record.
 *
 * <p><b>Note on {@code price}:</b> there is deliberately no {@code @Positive} constraint. A
 * zero or negative price must be publishable, because that is exactly the input the consumer
 * has to reject as a permanent failure and route to the dead letter queue. Adding a positive
 * constraint here would reject the order at the REST layer and leave the DLQ path with nothing
 * to demonstrate. Business validation belongs in the consumer, not in the producer.
 */
@Schema(description = "An order to publish to Kafka as an Avro record.")
public record OrderRequest(

        @NotBlank(message = "orderId must not be blank")
        @Schema(description = "Unique identifier for the order", example = "1001")
        String orderId,

        @NotBlank(message = "product must not be blank")
        @Schema(description = "Name of the purchased item", example = "Item1")
        String product,

        @NotNull(message = "price must not be null")
        @Schema(description = "Price of the product. Negative values are accepted here on "
                + "purpose, so the consumer can reject them as permanent failures.",
                example = "100.0")
        Float price) {
}
