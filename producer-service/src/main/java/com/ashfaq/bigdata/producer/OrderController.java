package com.ashfaq.bigdata.producer;

import com.ashfaq.bigdata.producer.dto.OrderRequest;
import com.ashfaq.bigdata.producer.dto.PublishResult;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * REST entry point for publishing orders.
 *
 * <p>The demonstration endpoints exist so the recorded walkthrough is a sequence of single
 * clicks in Swagger UI rather than hand-typed JSON, and so the exact acceptance-case data is
 * reproducible by anyone reviewing the project.
 */
@RestController
@RequestMapping("/api/orders")
@Tag(name = "Orders", description = "Publish Avro order events to Kafka")
public class OrderController {

    private final OrderPublisher publisher;
    private final OrderGenerator generator;

    public OrderController(OrderPublisher publisher, OrderGenerator generator) {
        this.publisher = publisher;
        this.generator = generator;
    }

    @PostMapping
    @Operation(summary = "Publish one order",
            description = "Serialises the supplied order as Avro and publishes it to orders.v1, "
                    + "using orderId as the Kafka message key. A zero or negative price is "
                    + "accepted here on purpose so the consumer can reject it as a permanent "
                    + "failure.")
    public PublishResult publish(@Valid @RequestBody OrderRequest request) {
        return publisher.publish(request);
    }

    @PostMapping("/generate")
    @Operation(summary = "Generate normal orders",
            description = "Publishes the requested number of valid orders with randomised "
                    + "positive prices. Identifiers start at 2001 so they never collide with "
                    + "the fixed demonstration orders 1001-1005.")
    public List<PublishResult> generate(
            @Parameter(description = "How many orders to publish", example = "5")
            @RequestParam(defaultValue = "5") int count) {

        return generator.generate(count).stream()
                .map(publisher::publish)
                .toList();
    }

    @PostMapping("/demo/temporary")
    @Operation(summary = "Demo: temporary failure that recovers",
            description = "Publishes a TEMP_FAIL order priced 500.00. The consumer fails it on "
                    + "attempts 1 and 2, then succeeds on attempt 3, so the order is counted "
                    + "exactly once in the running average.")
    public PublishResult demoTemporaryFailure(
            @Parameter(description = "Override the order id to re-run the demo with fresh data",
                    example = "1003")
            @RequestParam(defaultValue = "1003") String orderId) {

        return publisher.publish(DemoOrders.temporaryFailure(orderId));
    }

    @PostMapping("/demo/permanent")
    @Operation(summary = "Demo: permanent failure, straight to the DLQ",
            description = "Publishes an order priced -10.00. The consumer rejects it as a "
                    + "permanent validation failure, so it bypasses the retry topics entirely "
                    + "and lands in orders.v1-dlt without changing the running average.")
    public PublishResult demoPermanentFailure(
            @Parameter(description = "Override the order id to re-run the demo with fresh data",
                    example = "1004")
            @RequestParam(defaultValue = "1004") String orderId) {

        return publisher.publish(DemoOrders.permanentFailure(orderId));
    }

    @PostMapping("/demo/always-fail")
    @Operation(summary = "Demo: retries exhausted, then the DLQ",
            description = "Publishes an ALWAYS_FAIL order priced 700.00. Every attempt fails, "
                    + "so the order travels through both retry topics and ends in orders.v1-dlt "
                    + "with the running average unchanged.")
    public PublishResult demoAlwaysFails(
            @Parameter(description = "Override the order id to re-run the demo with fresh data",
                    example = "1005")
            @RequestParam(defaultValue = "1005") String orderId) {

        return publisher.publish(DemoOrders.alwaysFails(orderId));
    }
}
