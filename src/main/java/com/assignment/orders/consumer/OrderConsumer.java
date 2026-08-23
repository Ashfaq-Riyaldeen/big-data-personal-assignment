package com.assignment.orders.consumer;

import com.assignment.orders.aggregate.PriceAggregator;
import com.assignment.orders.aggregate.ProcessingMetrics;
import com.assignment.orders.avro.Order;
import com.assignment.orders.config.AppConfig;
import com.assignment.orders.dlq.DeadLetterPublisher;
import com.assignment.orders.dlq.DlqReason;
import com.assignment.orders.exception.PermanentProcessingException;
import com.assignment.orders.ui.Dashboard;
import com.assignment.orders.ui.EventLog;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consumes Avro order messages, aggregates their prices in real time, retries transient
 * failures, and dead-letters anything it cannot process.
 *
 * <h2>Why the values are deserialised by hand</h2>
 *
 * <p>The obvious configuration is {@code value.deserializer=KafkaAvroDeserializer}, letting the
 * client return {@code Order} objects directly. It is also a trap. When a message on the topic
 * is not valid Avro, the deserialiser throws from inside {@code poll()} itself -- before any
 * application code gets a reference to the record. There is nothing to route to a DLQ, because
 * the record never surfaces. Worse, the offset is never advanced, so the next {@code poll()}
 * returns the same bad message and throws again: the consumer is wedged, and one malformed
 * message has stopped the partition permanently. This is the classic Kafka "poison pill".
 *
 * <p>So the consumer reads {@code byte[]} values, which cannot fail to deserialise, and invokes
 * {@link KafkaAvroDeserializer} itself inside a try/catch. A decode failure is then just an
 * ordinary error attached to a record we are holding, and it can be dead-lettered like any other.
 *
 * <h2>Delivery guarantees</h2>
 *
 * <p>Auto-commit is off. Offsets are committed only after every record in a batch has reached a
 * terminal state -- processed successfully, or durably written to the DLQ. If the process dies
 * mid-batch the uncommitted records are redelivered, giving at-least-once semantics: a record may
 * be processed twice, but it is never silently dropped. For an aggregation that is the right
 * trade, since losing orders would quietly corrupt the average while a duplicate merely nudges it.
 */
public final class OrderConsumer {

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    private final KafkaConsumer<String, byte[]> consumer;
    private final KafkaAvroDeserializer avroDeserializer;
    private final OrderProcessor processor;
    private final RetryPolicy retryPolicy;
    private final DeadLetterPublisher deadLetterPublisher;
    private final PriceAggregator aggregator;
    private final ProcessingMetrics metrics;
    private final EventLog events;

    private final String topic;
    private final boolean dashboardEnabled;
    /** Stop cleanly after this many milliseconds; 0 runs until interrupted. */
    private final long runForMillis;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private OrderConsumer(KafkaConsumer<String, byte[]> consumer,
                          KafkaAvroDeserializer avroDeserializer,
                          OrderProcessor processor,
                          RetryPolicy retryPolicy,
                          DeadLetterPublisher deadLetterPublisher,
                          PriceAggregator aggregator,
                          ProcessingMetrics metrics,
                          EventLog events,
                          String topic,
                          boolean dashboardEnabled,
                          long runForMillis) {
        this.consumer = consumer;
        this.avroDeserializer = avroDeserializer;
        this.processor = processor;
        this.retryPolicy = retryPolicy;
        this.deadLetterPublisher = deadLetterPublisher;
        this.aggregator = aggregator;
        this.metrics = metrics;
        this.events = events;
        this.topic = topic;
        this.dashboardEnabled = dashboardEnabled;
        this.runForMillis = runForMillis;
    }

    public static void main(String[] args) {
        String bootstrapServers = AppConfig.bootstrapServers();
        String schemaRegistryUrl = AppConfig.schemaRegistryUrl();
        String topic = AppConfig.ordersTopic();
        String dlqTopic = AppConfig.dlqTopic();
        String group = AppConfig.consumerGroup();
        boolean dashboardEnabled = AppConfig.dashboardEnabled();

        RetryPolicy retryPolicy = new RetryPolicy(
                AppConfig.maxAttempts(),
                AppConfig.initialBackoffMs(),
                AppConfig.backoffMultiplier(),
                AppConfig.maxBackoffMs(),
                AppConfig.jitterFactor(),
                ThreadLocalRandom.current());

        OrderProcessor processor = new OrderProcessor(
                AppConfig.transientFailureRate(),
                OrderProcessor.DEFAULT_MAX_ACCEPTABLE_PRICE,
                ThreadLocalRandom.current());

        PriceAggregator aggregator = new PriceAggregator(AppConfig.windowSeconds());
        ProcessingMetrics metrics = new ProcessingMetrics();
        EventLog events = new EventLog(64);

        printBanner(bootstrapServers, schemaRegistryUrl, topic, dlqTopic, group, retryPolicy);

        KafkaConsumer<String, byte[]> kafkaConsumer = createConsumer(bootstrapServers, group);
        KafkaAvroDeserializer avroDeserializer = createAvroDeserializer(schemaRegistryUrl);

        try (DeadLetterPublisher dlq = DeadLetterPublisher.create(bootstrapServers, dlqTopic, group)) {

            OrderConsumer app = new OrderConsumer(
                    kafkaConsumer, avroDeserializer, processor, retryPolicy, dlq,
                    aggregator, metrics, events, topic, dashboardEnabled,
                    AppConfig.runForSeconds() * 1000L);

            Dashboard dashboard = dashboardEnabled
                    ? new Dashboard(aggregator, metrics, events, System.out,
                    bootstrapServers, topic, dlqTopic, group, retryPolicy.describe())
                    : null;

            // Ctrl+C arrives on a different thread. wakeup() makes the in-flight poll() throw
            // WakeupException, which unwinds the loop so offsets can be committed on the way out
            // instead of the JVM being killed mid-batch.
            Thread mainThread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                app.running.set(false);
                kafkaConsumer.wakeup();
                try {
                    mainThread.join(10_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "consumer-shutdown"));

            if (dashboard != null) {
                dashboard.start();
            }
            try {
                app.run();
            } finally {
                if (dashboard != null) {
                    dashboard.close();
                }
                app.printSummary();
            }
        }
    }

    /** The poll loop. */
    private void run() {
        try {
            consumer.subscribe(List.of(topic));
            note(EventLog.Level.INFO, "subscribed to " + topic);

            // A bounded run exits through the same path as Ctrl+C, so offsets are still committed
            // and the final totals still print.
            long deadline = runForMillis > 0
                    ? System.currentTimeMillis() + runForMillis
                    : Long.MAX_VALUE;

            while (running.get()) {
                if (System.currentTimeMillis() >= deadline) {
                    note(EventLog.Level.INFO, "run time limit reached, shutting down");
                    break;
                }
                ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT);
                if (records.isEmpty()) {
                    continue;
                }

                for (ConsumerRecord<String, byte[]> record : records) {
                    metrics.recordConsumed();
                    handle(record);
                }

                // Commit once per batch, after every record above reached a terminal state.
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            // Expected on shutdown.
        } catch (DeadLetterPublisher.DeadLetterPublishException e) {
            // The DLQ is the safety net; if it is unavailable there is nowhere safe to put a
            // failed record. Stopping without committing means the record is redelivered later
            // rather than lost.
            System.err.println();
            System.err.println("FATAL: could not write to the dead letter queue, stopping without committing.");
            System.err.println("  " + e.getMessage());
        } finally {
            try {
                // Best-effort commit of anything finished since the last batch commit.
                consumer.commitSync(Duration.ofSeconds(5));
            } catch (RuntimeException ignored) {
                // Already shutting down; a failed final commit just means redelivery.
            }
            consumer.close(Duration.ofSeconds(5));
        }
    }

    /** Decodes one record and drives it to a terminal state. */
    private void handle(ConsumerRecord<String, byte[]> record) {
        Order order;
        try {
            order = decode(record);
        } catch (Exception e) {
            // A poison pill. Retrying identical bytes would fail identically, so it goes
            // straight to the DLQ where the raw bytes can be inspected.
            deadLetter(record, DlqReason.DESERIALIZATION_FAILED, e, 1);
            return;
        }

        int attempt = 0;
        while (true) {
            attempt++;
            try {
                processor.process(order);

                aggregator.record(order.getProduct(), order.getPrice(), System.currentTimeMillis());
                metrics.recordSuccess(attempt);
                if (attempt > 1) {
                    note(EventLog.Level.RECOVERED, String.format(
                            "order %s succeeded on attempt %d/%d",
                            order.getOrderId(), attempt, retryPolicy.maxAttempts()));
                }
                return;

            } catch (RuntimeException error) {
                ErrorClassifier.Kind kind = ErrorClassifier.classify(error);
                metrics.recordLastError(error.getMessage());

                if (kind == ErrorClassifier.Kind.PERMANENT) {
                    // No amount of retrying fixes a negative price. Skip the backoff entirely.
                    deadLetter(record, ErrorClassifier.reasonFor(error, false), error, attempt);
                    return;
                }

                if (!retryPolicy.shouldRetry(attempt)) {
                    deadLetter(record, DlqReason.RETRIES_EXHAUSTED, error, attempt);
                    return;
                }

                long backoffMillis = retryPolicy.backoffMillis(attempt + 1);
                metrics.recordRetryAttempt();
                note(EventLog.Level.RETRY, String.format(
                        "order %s attempt %d/%d failed, retrying in %dms - %s",
                        order.getOrderId(), attempt, retryPolicy.maxAttempts(),
                        backoffMillis, error.getMessage()));

                if (!sleep(backoffMillis)) {
                    return;
                }
            }
        }
    }

    /**
     * Turns the raw bytes into an {@link Order}, throwing if they are not valid Avro for the
     * registered schema.
     */
    private Order decode(ConsumerRecord<String, byte[]> record) {
        if (record.value() == null) {
            throw new PermanentProcessingException("record value was null (tombstone)");
        }
        Object decoded = avroDeserializer.deserialize(record.topic(), record.value());
        if (!(decoded instanceof Order order)) {
            throw new PermanentProcessingException(
                    "expected an Order but decoded " + (decoded == null ? "null" : decoded.getClass().getName()));
        }
        return order;
    }

    /** Routes a record to the DLQ and records why. */
    private void deadLetter(ConsumerRecord<String, byte[]> record,
                            DlqReason reason,
                            Throwable error,
                            int attempts) {

        deadLetterPublisher.publish(record, reason, error, attempts);
        metrics.recordDeadLettered(reason);
        metrics.recordLastError(error == null ? "" : error.getMessage());

        note(EventLog.Level.DLQ, String.format(
                "offset %d -> DLQ [%s] after %d attempt%s - %s",
                record.offset(), reason.shortLabel(), attempts, attempts == 1 ? "" : "s",
                error == null ? "unknown" : error.getMessage()));
    }

    /** Sleeps for the backoff, returning false if the wait was interrupted by shutdown. */
    private boolean sleep(long millis) {
        if (millis <= 0) {
            return true;
        }
        try {
            Thread.sleep(millis);
            return running.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            running.set(false);
            return false;
        }
    }

    /**
     * Records an event. It always goes to the ring buffer the dashboard renders; when the
     * dashboard is off, it is also printed, since otherwise nothing would be visible.
     */
    private void note(EventLog.Level level, String message) {
        events.add(level, message);
        if (!dashboardEnabled) {
            System.out.printf("  %-10s %s%n", level, message);
        }
    }

    private void printSummary() {
        ProcessingMetrics.Snapshot health = metrics.snapshot();
        PriceAggregator.Snapshot prices = aggregator.snapshot(System.currentTimeMillis());

        System.out.println();
        System.out.println("------------------------------------------------------------");
        System.out.println("  FINAL TOTALS");
        System.out.println("------------------------------------------------------------");
        System.out.printf("  consumed          : %,d%n", health.consumed());
        System.out.printf("  processed         : %,d%n", health.processedOk());
        System.out.printf("  running average   : %,.2f  (over %,d orders)%n", prices.mean(), prices.count());
        System.out.printf("  retry attempts    : %,d%n", health.retryAttempts());
        System.out.printf("  recovered by retry: %,d%n", health.recoveredAfterRetry());
        System.out.printf("  dead-lettered     : %,d%n", health.deadLettered());
        for (DlqReason reason : DlqReason.values()) {
            System.out.printf("      %-22s %,d%n", reason.name(), health.countFor(reason));
        }
        System.out.println("------------------------------------------------------------");
    }

    private static KafkaConsumer<String, byte[]> createConsumer(String bootstrapServers, String group) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Bytes, not Avro. See the class javadoc: this is what makes poison pills survivable.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        // Offsets are committed by hand once records are terminal.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Blocking retries spend real time inside the poll loop. If a batch takes longer than
        // max.poll.interval.ms the broker assumes this consumer has died and rebalances the
        // partitions away, which causes duplicate processing. Small batches plus a generous
        // interval keep the worst case (every record exhausting its retry budget) well inside
        // the limit. This tension is the main cost of retrying in process.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 20);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 600_000);

        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "order-consumer");
        return new KafkaConsumer<>(props);
    }

    /**
     * Builds a standalone Avro deserialiser. Configured with {@code specific.avro.reader} so it
     * returns generated {@link Order} objects rather than generic records.
     */
    private static KafkaAvroDeserializer createAvroDeserializer(String schemaRegistryUrl) {
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
        deserializer.configure(config, false);
        return deserializer;
    }

    private static void printBanner(String bootstrapServers,
                                    String schemaRegistryUrl,
                                    String topic,
                                    String dlqTopic,
                                    String group,
                                    RetryPolicy retryPolicy) {
        System.out.println("""
                ------------------------------------------------------------
                  ORDER CONSUMER
                ------------------------------------------------------------""");
        System.out.printf("  broker           : %s%n", bootstrapServers);
        System.out.printf("  schema registry  : %s%n", schemaRegistryUrl);
        System.out.printf("  topic            : %s%n", topic);
        System.out.printf("  dead letter queue: %s%n", dlqTopic);
        System.out.printf("  consumer group   : %s%n", group);
        System.out.printf("  retry policy     : %s%n", retryPolicy.describe());
        System.out.printf("  simulated outages: %.0f%%%n", AppConfig.transientFailureRate() * 100);
        System.out.println("------------------------------------------------------------");
        System.out.println("  connecting...");
    }
}
