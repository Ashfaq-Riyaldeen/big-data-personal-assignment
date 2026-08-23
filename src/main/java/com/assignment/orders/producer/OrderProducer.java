package com.assignment.orders.producer;

import com.assignment.orders.avro.Order;
import com.assignment.orders.config.AppConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Publishes a continuous stream of Avro-serialised orders to the {@code orders} topic.
 *
 * <p>Two producers are used, and the split is deliberate rather than incidental:
 *
 * <ul>
 *   <li>The <b>Avro producer</b> is configured the idiomatic way, with
 *       {@link KafkaAvroSerializer} as its value serialiser. It registers {@code order.avsc}
 *       with the Schema Registry on first send and thereafter writes the compact
 *       {@code [magic byte][schema id][payload]} wire format.</li>
 *   <li>The <b>raw producer</b> writes plain bytes with no serialiser involved. It exists only
 *       to inject poison pills, and it models something real: a different team's service
 *       publishing to your topic with the wrong serialiser. Keeping it as a visibly separate
 *       client makes clear that the corruption comes from outside this application, not from a
 *       bug in the Avro path.</li>
 * </ul>
 *
 * <p>Run with {@code TOTAL_MESSAGES=0} (the default) to stream until interrupted.
 */
public final class OrderProducer {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = AppConfig.bootstrapServers();
        String schemaRegistryUrl = AppConfig.schemaRegistryUrl();
        String topic = AppConfig.ordersTopic();
        double ratePerSecond = AppConfig.produceRatePerSecond();
        long totalMessages = AppConfig.totalMessages();

        OrderGenerator generator = new OrderGenerator(
                AppConfig.productCount(),
                AppConfig.minPrice(),
                AppConfig.maxPrice(),
                AppConfig.badRecordRate(),
                AppConfig.poisonRate(),
                ThreadLocalRandom.current());

        System.out.println("""
                ------------------------------------------------------------
                  ORDER PRODUCER
                ------------------------------------------------------------""");
        System.out.printf("  broker           : %s%n", bootstrapServers);
        System.out.printf("  schema registry  : %s%n", schemaRegistryUrl);
        System.out.printf("  topic            : %s%n", topic);
        System.out.printf("  rate             : %.1f orders/sec%n", ratePerSecond);
        System.out.printf("  messages         : %s%n", totalMessages == 0 ? "unlimited (Ctrl+C to stop)" : totalMessages);
        System.out.printf("  invalid orders   : %.0f%%%n", AppConfig.badRecordRate() * 100);
        System.out.printf("  poison pills     : %.0f%%%n", AppConfig.poisonRate() * 100);
        System.out.println("------------------------------------------------------------");
        System.out.println();

        AtomicLong sent = new AtomicLong();
        AtomicLong valid = new AtomicLong();
        AtomicLong flawed = new AtomicLong();
        AtomicLong poisoned = new AtomicLong();
        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch finished = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            try {
                // Give the send loop a moment to drain and print its summary before the JVM dies.
                finished.await(5, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "producer-shutdown"));

        try (Producer<String, Order> avroProducer = createAvroProducer(bootstrapServers, schemaRegistryUrl);
             Producer<String, byte[]> rawProducer = createRawProducer(bootstrapServers)) {

            long intervalNanos = (long) (1_000_000_000L / ratePerSecond);
            long nextSendAt = System.nanoTime();

            while (running.get() && (totalMessages == 0 || sent.get() < totalMessages)) {
                OrderGenerator.Generated generated = generator.next();

                if (generated.isPoison()) {
                    rawProducer.send(new ProducerRecord<>(topic, generated.key(), generated.poisonBytes()));
                    poisoned.incrementAndGet();
                    System.out.printf("  %-8s key=%-6s  %d bytes of non-Avro data%n",
                            "POISON", generated.key(), generated.poisonBytes().length);
                } else {
                    Order order = generated.order();
                    avroProducer.send(new ProducerRecord<>(topic, generated.key(), order));
                    if (generated.isValid()) {
                        valid.incrementAndGet();
                        System.out.printf("  %-8s key=%-6s  %-7s %10.2f%n",
                                "ok", generated.key(), order.getProduct(), order.getPrice());
                    } else {
                        flawed.incrementAndGet();
                        System.out.printf("  %-8s key=%-6s  %-7s %10.2f   <- %s%n",
                                "INVALID", generated.key(),
                                order.getProduct().isBlank() ? "(blank)" : order.getProduct(),
                                order.getPrice(), generated.flaw());
                    }
                }
                sent.incrementAndGet();

                // Pace the stream. Tracking an absolute deadline rather than sleeping a fixed
                // interval stops the send time itself from dragging the rate below target.
                nextSendAt += intervalNanos;
                long sleepNanos = nextSendAt - System.nanoTime();
                if (sleepNanos > 0) {
                    Thread.sleep(sleepNanos / 1_000_000L, (int) (sleepNanos % 1_000_000L));
                } else {
                    // Running behind; reset the deadline so the lag does not compound.
                    nextSendAt = System.nanoTime();
                }
            }

            avroProducer.flush();
            rawProducer.flush();
        } finally {
            System.out.println();
            System.out.println("------------------------------------------------------------");
            System.out.printf("  sent %d messages: %d valid, %d invalid, %d poison%n",
                    sent.get(), valid.get(), flawed.get(), poisoned.get());
            System.out.println("------------------------------------------------------------");
            finished.countDown();
        }
    }

    /** The idiomatic Avro producer: the serialiser handles schema registration and framing. */
    private static Producer<String, Order> createAvroProducer(String bootstrapServers, String schemaRegistryUrl) {
        Properties props = baseProps(bootstrapServers, "order-producer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        // Convenient for an assignment; in production schemas are usually registered by CI so a
        // deploy cannot quietly introduce an incompatible one.
        props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
        return new KafkaProducer<>(props);
    }

    /** Bypasses Avro entirely, to simulate a misconfigured external producer. */
    private static Producer<String, byte[]> createRawProducer(String bootstrapServers) {
        Properties props = baseProps(bootstrapServers, "rogue-producer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private static Properties baseProps(String bootstrapServers, String clientId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);

        // acks=all plus idempotence gives exactly-once semantics per partition on the write
        // side: no silent loss when a broker fails over, and no duplicates from internal retries.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);

        // A small linger lets the client batch without adding noticeable latency at demo rates.
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        return props;
    }
}
