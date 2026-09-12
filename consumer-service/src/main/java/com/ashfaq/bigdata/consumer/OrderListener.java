package com.ashfaq.bigdata.consumer;

import com.ashfaq.bigdata.avro.Order;
import com.ashfaq.bigdata.consumer.aggregate.RunningAverage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes Avro orders and maintains the running average.
 *
 * <p>The ordering inside {@link #onOrder} is the point of the whole class: the aggregate is
 * updated <b>only after</b> processing succeeds. An order that throws never reaches the
 * counter, so failures and retries cannot move the average.
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

    @KafkaListener(topics = "${app.orders-topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void onOrder(Order order) {
        log.info("[RECEIVED] orderId={} product={} price={}",
                order.getOrderId(), order.getProduct(), formatMoney(order.getPrice()));

        // Throws on failure, so nothing below runs for an order that could not be processed.
        processor.process(order);

        log.info("[SUCCESS] orderId={} processed=true", order.getOrderId());

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
