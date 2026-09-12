package com.ashfaq.bigdata.consumer;

import com.ashfaq.bigdata.avro.Order;
import com.ashfaq.bigdata.consumer.aggregate.RunningAverage;
import com.ashfaq.bigdata.consumer.exception.PermanentOrderException;
import com.ashfaq.bigdata.consumer.exception.TemporaryOrderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

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

    private static String formatMoney(double value) {
        return String.format("%.2f", value);
    }
}
