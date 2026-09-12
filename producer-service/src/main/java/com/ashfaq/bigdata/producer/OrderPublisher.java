package com.ashfaq.bigdata.producer;

import com.ashfaq.bigdata.avro.Order;
import com.ashfaq.bigdata.producer.dto.OrderRequest;
import com.ashfaq.bigdata.producer.dto.PublishResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes Avro orders to the orders topic.
 *
 * <p>{@code orderId} is used as the Kafka message key, so every event for a given order is
 * routed to the same partition and keeps its relative ordering.
 */
@Service
public class OrderPublisher {

    private static final Logger log = LoggerFactory.getLogger(OrderPublisher.class);

    /** Long enough to cover a slow first send, which pays for schema registration. */
    private static final long ACK_TIMEOUT_SECONDS = 15;

    private final KafkaTemplate<String, Order> kafkaTemplate;
    private final String ordersTopic;

    public OrderPublisher(KafkaTemplate<String, Order> kafkaTemplate,
                          @Value("${app.orders-topic}") String ordersTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.ordersTopic = ordersTopic;
    }

    /**
     * Sends one order and waits for the broker acknowledgement, so the caller receives the real
     * partition and offset instead of an optimistic success.
     */
    public PublishResult publish(OrderRequest request) {
        Order order = OrderMapper.toAvro(request);

        CompletableFuture<SendResult<String, Order>> future =
                kafkaTemplate.send(ordersTopic, order.getOrderId(), order);

        future.whenComplete((result, failure) -> {
            if (failure != null) {
                log.error("[PUBLISH-FAILED] orderId={} product={} price={} reason={}",
                        order.getOrderId(), order.getProduct(), order.getPrice(),
                        failure.toString());
            } else {
                var metadata = result.getRecordMetadata();
                log.info("[PUBLISHED] orderId={} product={} price={} topic={} partition={} offset={}",
                        order.getOrderId(), order.getProduct(), order.getPrice(),
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });

        SendResult<String, Order> result = awaitAcknowledgement(future, order.getOrderId());
        var metadata = result.getRecordMetadata();

        return new PublishResult(
                order.getOrderId(),
                order.getProduct(),
                order.getPrice(),
                metadata.topic(),
                metadata.partition(),
                metadata.offset());
    }

    private SendResult<String, Order> awaitAcknowledgement(
            CompletableFuture<SendResult<String, Order>> future, String orderId) {
        try {
            return future.get(ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublishException("Interrupted while publishing order " + orderId, e);
        } catch (TimeoutException e) {
            throw new PublishException(
                    "Timed out waiting for Kafka to acknowledge order " + orderId, e);
        } catch (ExecutionException e) {
            throw new PublishException(
                    "Kafka rejected order " + orderId + ": " + e.getCause().getMessage(),
                    e.getCause());
        }
    }

    /** Raised when an order could not be written to Kafka. */
    public static class PublishException extends RuntimeException {
        public PublishException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
