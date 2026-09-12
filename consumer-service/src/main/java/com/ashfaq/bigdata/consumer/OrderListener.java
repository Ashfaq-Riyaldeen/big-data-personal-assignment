package com.ashfaq.bigdata.consumer;

import com.ashfaq.bigdata.avro.Order;
import com.ashfaq.bigdata.consumer.aggregate.RunningAverage;
import com.ashfaq.bigdata.consumer.exception.PermanentOrderException;
import com.ashfaq.bigdata.consumer.exception.TemporaryOrderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Consumes Avro orders, maintains the running average, and drives the retry policy.
 *
 * <p>The ordering inside {@link #onOrder} is the point of the whole class: the aggregate is
 * updated <b>only after</b> processing succeeds. An order that throws never reaches the counter,
 * so failures and retries cannot move the average.
 */
@Component
public class OrderListener {

    private static final Logger log = LoggerFactory.getLogger(OrderListener.class);

    /** Shown in the [DLQ] log so the destination is obvious on screen during the demonstration. */
    private static final String DLT_TOPIC_LABEL = "orders.v1-dlt";

    private final OrderProcessor processor;
    private final RunningAverage runningAverage;

    public OrderListener(OrderProcessor processor, RunningAverage runningAverage) {
        this.processor = processor;
        this.runningAverage = runningAverage;
    }

    /**
     * Retry policy: three total processing attempts, two seconds apart, on the dedicated retry
     * topics rather than by blocking the consumer thread.
     *
     * <p>Three settings here are load-bearing and easy to get wrong:
     *
     * <ul>
     *   <li>{@code autoCreateTopics = "false"} - every topic is created explicitly by the
     *       kafka-init service, and broker-side auto-creation is disabled, so a typo must fail
     *       loudly rather than conjure an empty topic.</li>
     *   <li>{@code sameIntervalTopicReuseStrategy = MULTIPLE_TOPICS} - with a fixed backoff
     *       Spring otherwise collapses both retries into a <i>single</i> topic.</li>
     *   <li>{@code topicSuffixingStrategy = SUFFIX_WITH_INDEX_VALUE} - the default suffixes by
     *       delay, producing orders.v1-retry-2000 instead of orders.v1-retry-0 and -retry-1.</li>
     * </ul>
     *
     * Together those three make the generated names match the pre-created topics exactly.
     * Without them the retry path breaks silently while the happy path still looks fine.
     *
     * <p>{@code PermanentOrderException} is excluded: an order with an invalid price will never
     * become valid, so retrying wastes attempts and delays the queue. It goes straight to the
     * dead letter queue.
     */
    @RetryableTopic(
            attempts = "${app.retry.attempts:3}",
            backoff = @Backoff(delayExpression = "${app.retry.delay-ms:2000}"),
            exclude = PermanentOrderException.class,
            autoCreateTopics = "false",
            retryTopicSuffix = "-retry",
            dltTopicSuffix = "-dlt",
            sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS,
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
    @KafkaListener(topics = "${app.orders-topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void onOrder(Order order,
                        @Header(name = KafkaHeaders.RECEIVED_TOPIC, required = false) String topic,
                        @Header(name = RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS, required = false)
                        Integer attemptsHeader) {

        // The header is absent on the original topic and set by Spring on every retry record.
        int attempt = attemptsHeader == null ? 1 : attemptsHeader;

        log.info("[RECEIVED] orderId={} product={} price={} attempt={} topic={}",
                order.getOrderId(), order.getProduct(), formatMoney(order.getPrice()),
                attempt, topic);

        try {
            processor.process(order, attempt);
        } catch (TemporaryOrderException e) {
            log.warn("[RETRY] orderId={} attempt={} reason={}",
                    order.getOrderId(), attempt, e.getMessage());
            throw e;
        } catch (PermanentOrderException e) {
            log.error("[PERMANENT-ERROR] orderId={} reason={}", order.getOrderId(), e.getMessage());
            throw e;
        }

        log.info("[SUCCESS] orderId={} attempt={} processed=true", order.getOrderId(), attempt);

        runningAverage.recordSuccess(order.getOrderId(), order.getPrice()).ifPresentOrElse(
                snapshot -> log.info("[AVERAGE] count={} total={} runningAverage={}",
                        snapshot.count(), formatMoney(snapshot.total()),
                        formatMoney(snapshot.average())),
                () -> log.info("[AVERAGE] orderId={} already counted, aggregate unchanged at {}",
                        order.getOrderId(), formatMoney(runningAverage.snapshot().average())));
    }

    /**
     * Last stop for an order that could not be processed: a permanent validation failure, or a
     * temporary one that used up every retry.
     *
     * <p>The record arriving here still carries the <b>original Avro order</b> as its value, with
     * the error context added as Kafka headers, so a failed event stays inspectable and could be
     * replayed once the underlying problem is fixed.
     *
     * <p>The payload is optional as a safety net. A poison pill - bytes that were never valid Avro -
     * does reach this topic, but it never reaches this method: deserialisation fails first, and
     * Spring logs "no further action will be taken" and commits the offset rather than looping. The
     * record still sits on the dead letter queue for inspection, which is the behaviour we want.
     * Accepting a null payload simply means a record that arrives without a value is logged instead
     * of throwing, since a throw here has nowhere further to route.
     */
    @DltHandler
    public void onDeadLetter(@Payload(required = false) Order order,
                             @Headers MessageHeaders headers) {

        String originTopic = headerAsString(headers, KafkaHeaders.ORIGINAL_TOPIC);
        String origin = originTopic == null ? "unknown"
                : originTopic
                        + "-" + headerAsNumber(headers, KafkaHeaders.ORIGINAL_PARTITION)
                        + "@" + headerAsNumber(headers, KafkaHeaders.ORIGINAL_OFFSET);

        String cause = simpleName(headerAsString(headers, KafkaHeaders.EXCEPTION_CAUSE_FQCN));
        String reason = headerAsString(headers, KafkaHeaders.EXCEPTION_MESSAGE);
        String attempts = String.valueOf(
                headerAsNumber(headers, RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS));

        if (order == null) {
            // A poison pill: bytes on the topic that were never a valid Avro order.
            log.error("[DLQ] undeserialisable record destination={} cause={} reason={} origin={}",
                    DLT_TOPIC_LABEL, cause, reason, origin);
            return;
        }

        log.error("[DLQ] orderId={} product={} price={} destination={} cause={} reason={} "
                        + "attempts={} origin={}",
                order.getOrderId(), order.getProduct(), formatMoney(order.getPrice()),
                DLT_TOPIC_LABEL, cause, reason, attempts, origin);

        log.info("[AVERAGE] unchanged at {} - failed orders never enter the aggregate",
                formatMoney(runningAverage.snapshot().average()));
    }

    private static String formatMoney(double value) {
        return String.format("%.2f", value);
    }

    /** Header values arrive as raw bytes or as already-converted objects depending on the type. */
    private static String headerAsString(MessageHeaders headers, String name) {
        Object value = headers.get(name);
        if (value == null) {
            return null;
        }
        return value instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8)
                : value.toString();
    }

    /** Kafka writes the numeric headers as four big-endian bytes rather than as text. */
    private static long headerAsNumber(MessageHeaders headers, String name) {
        Object value = headers.get(name);
        if (value == null) {
            return -1;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof byte[] bytes) {
            long result = 0;
            for (byte b : bytes) {
                result = (result << 8) | (b & 0xFF);
            }
            return result;
        }
        return -1;
    }

    private static String simpleName(String fullyQualifiedName) {
        if (fullyQualifiedName == null) {
            return "unknown";
        }
        int lastDot = fullyQualifiedName.lastIndexOf('.');
        return lastDot < 0 ? fullyQualifiedName : fullyQualifiedName.substring(lastDot + 1);
    }
}
