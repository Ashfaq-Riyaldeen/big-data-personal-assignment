package com.ashfaq.bigdata.consumer;

import com.ashfaq.bigdata.avro.Order;
import com.ashfaq.bigdata.consumer.aggregate.RunningAverage;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.awaitility.Awaitility.await;

/**
 * Proves the whole pipeline composes, with nothing but a JDK: a real Avro record goes through a
 * real (embedded) broker, Spring routes failures across the retry topics to the dead letter
 * queue, and the aggregate comes out right at the end.
 *
 * <p>The Schema Registry is the in-memory {@code mock://} implementation. Scopes are JVM-global,
 * so the test producer, the consumer's deserialiser and the consumer's own republishing producer
 * all share one registry by naming the same scope.
 *
 * <p>The retry delay is shortened to 200 ms. The routing logic under test is identical; only the
 * wait is shorter. The production 2-second delay is demonstrated against the real stack.
 *
 * <p>Tests are ordered because they share one running average, exactly as a live run does: the
 * five acceptance cases are replayed in sequence and the aggregate is checked after each.
 */
@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"orders.v1", "orders.v1-retry-0", "orders.v1-retry-1", "orders.v1-dlt"})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.producer.properties.schema.registry.url=" + OrderPipelineIntegrationTest.MOCK_REGISTRY,
        "spring.kafka.consumer.properties.schema.registry.url=" + OrderPipelineIntegrationTest.MOCK_REGISTRY,
        "app.retry.delay-ms=200",
        // Poll faster than the default so the assertions settle quickly.
        "spring.kafka.consumer.properties.fetch.max.wait.ms=100"
})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OrderPipelineIntegrationTest {

    static final String MOCK_REGISTRY = "mock://pipeline-test";

    private static final String ORDERS_TOPIC = "orders.v1";
    private static final String DLT_TOPIC = "orders.v1-dlt";
    private static final Duration SETTLE = Duration.ofSeconds(15);
    private static final Duration QUIET = Duration.ofMillis(1500);

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private RunningAverage runningAverage;

    private KafkaTemplate<String, Order> producer;
    private Consumer<String, Order> dltConsumer;

    @BeforeAll
    void setUpClients() {
        Map<String, Object> producerProps = new HashMap<>(KafkaTestUtils.producerProps(broker));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_REGISTRY);
        producer = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));

        Map<String, Object> consumerProps =
                new HashMap<>(KafkaTestUtils.consumerProps("dlt-observer", "true", broker));
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_REGISTRY);
        consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        dltConsumer = new DefaultKafkaConsumerFactory<String, Order>(consumerProps).createConsumer();
        broker.consumeFromAnEmbeddedTopic(dltConsumer, DLT_TOPIC);
    }

    @AfterAll
    void tearDownClients() {
        dltConsumer.close();
        producer.destroy();
    }

    // ---------------------------------------------------------------------------------------
    // Acceptance cases 1 and 2: the happy path and the aggregate
    // ---------------------------------------------------------------------------------------

    @Test
    @org.junit.jupiter.api.Order(1)
    @DisplayName("two normal orders are consumed as Avro and the running average is 200.00")
    void normalOrdersUpdateTheRunningAverage() {
        publish("1001", "Item1", 100.0f);
        publish("1002", "Item2", 300.0f);

        await().atMost(SETTLE).untilAsserted(() ->
                assertAggregate(2, 400.0, 200.0));
    }

    // ---------------------------------------------------------------------------------------
    // Acceptance case 3: temporary failure, retried, recovered, counted once
    // ---------------------------------------------------------------------------------------

    @Test
    @org.junit.jupiter.api.Order(2)
    @DisplayName("TEMP_FAIL is retried, succeeds on the third attempt, and is counted once")
    void temporaryFailureIsRetriedAndCountedOnce() {
        publish("1003", "TEMP_FAIL", 500.0f);

        await().atMost(SETTLE).untilAsserted(() ->
                assertAggregate(3, 900.0, 300.0));

        // Negative wait: nothing else may move the aggregate. If the retried order were counted
        // on every attempt, or a stray redelivery slipped through, this would catch it.
        await().during(QUIET).atMost(SETTLE).untilAsserted(() ->
                assertAggregate(3, 900.0, 300.0));
    }

    // ---------------------------------------------------------------------------------------
    // Acceptance case 4: permanent failure straight to the DLQ, no retry
    // ---------------------------------------------------------------------------------------

    @Test
    @org.junit.jupiter.api.Order(3)
    @DisplayName("a negative price bypasses retry and lands in the DLQ with its cause recorded")
    void permanentFailureGoesStraightToTheDlq() {
        long retryRecordsBefore = countRecords("orders.v1-retry-0") + countRecords("orders.v1-retry-1");

        publish("1004", "InvalidItem", -10.0f);

        ConsumerRecord<String, Order> dead = awaitDeadLetter("1004");

        assertThat(header(dead, KafkaHeaders.EXCEPTION_CAUSE_FQCN))
                .endsWith(".PermanentOrderException");
        assertThat(header(dead, KafkaHeaders.ORIGINAL_TOPIC)).isEqualTo(ORDERS_TOPIC);

        // Permanent means permanent: the retry topics must not have seen this order.
        assertThat(countRecords("orders.v1-retry-0") + countRecords("orders.v1-retry-1"))
                .isEqualTo(retryRecordsBefore);

        assertAggregate(3, 900.0, 300.0);
    }

    // ---------------------------------------------------------------------------------------
    // Acceptance case 5: temporary failure that never recovers, exhausted, then the DLQ
    // ---------------------------------------------------------------------------------------

    @Test
    @org.junit.jupiter.api.Order(4)
    @DisplayName("ALWAYS_FAIL passes both retry topics, exhausts its attempts, and lands in the DLQ")
    void exhaustedRetriesGoToTheDlq() {
        long retry0Before = countRecords("orders.v1-retry-0");
        long retry1Before = countRecords("orders.v1-retry-1");

        publish("1005", "ALWAYS_FAIL", 700.0f);

        ConsumerRecord<String, Order> dead = awaitDeadLetter("1005");

        assertThat(header(dead, KafkaHeaders.EXCEPTION_CAUSE_FQCN))
                .endsWith(".TemporaryOrderException");

        // Exhausted means it travelled the whole retry chain first.
        assertThat(countRecords("orders.v1-retry-0")).isEqualTo(retry0Before + 1);
        assertThat(countRecords("orders.v1-retry-1")).isEqualTo(retry1Before + 1);

        assertAggregate(3, 900.0, 300.0);
    }

    // ---------------------------------------------------------------------------------------
    // The DLQ record is still the original order
    // ---------------------------------------------------------------------------------------

    @Test
    @org.junit.jupiter.api.Order(5)
    @DisplayName("a dead-lettered record still carries the original Avro order, so it can be replayed")
    void deadLetterPreservesTheOriginalOrder() {
        // Both earlier DLQ records were consumed by awaitDeadLetter; re-read the topic from the
        // start with a fresh consumer so this test stands on its own.
        List<ConsumerRecord<String, Order>> all = readAllDeadLetters();

        assertThat(all).extracting(ConsumerRecord::key).contains("1004", "1005");

        ConsumerRecord<String, Order> negative = all.stream()
                .filter(r -> "1004".equals(r.key())).findFirst().orElseThrow();
        assertThat(negative.value().getOrderId()).isEqualTo("1004");
        assertThat(negative.value().getProduct()).isEqualTo("InvalidItem");
        assertThat(negative.value().getPrice()).isEqualTo(-10.0f);
        assertThat(negative.value().getSchema()).isEqualTo(Order.getClassSchema());
    }

    // ---------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------

    private void publish(String orderId, String product, float price) {
        Order order = Order.newBuilder()
                .setOrderId(orderId).setProduct(product).setPrice(price).build();
        producer.send(ORDERS_TOPIC, orderId, order).join();
    }

    private void assertAggregate(long count, double total, double average) {
        RunningAverage.Snapshot snapshot = runningAverage.snapshot();
        assertThat(snapshot.count()).isEqualTo(count);
        assertThat(snapshot.total()).isCloseTo(total, within(1e-6));
        assertThat(snapshot.average()).isCloseTo(average, within(1e-6));
    }

    private ConsumerRecord<String, Order> awaitDeadLetter(String orderId) {
        List<ConsumerRecord<String, Order>> seen = new ArrayList<>();
        await().atMost(SETTLE).untilAsserted(() -> {
            ConsumerRecords<String, Order> polled = dltConsumer.poll(Duration.ofMillis(200));
            polled.forEach(seen::add);
            assertThat(seen).anyMatch(r -> orderId.equals(r.key()));
        });
        return seen.stream().filter(r -> orderId.equals(r.key())).findFirst().orElseThrow();
    }

    private List<ConsumerRecord<String, Order>> readAllDeadLetters() {
        Map<String, Object> props =
                new HashMap<>(KafkaTestUtils.consumerProps("dlt-reader-" + System.nanoTime(), "true", broker));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_REGISTRY);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        try (Consumer<String, Order> reader =
                     new DefaultKafkaConsumerFactory<String, Order>(props).createConsumer()) {
            broker.consumeFromAnEmbeddedTopic(reader, DLT_TOPIC);
            List<ConsumerRecord<String, Order>> all = new ArrayList<>();
            await().atMost(SETTLE).untilAsserted(() -> {
                reader.poll(Duration.ofMillis(200)).forEach(all::add);
                assertThat(all).hasSizeGreaterThanOrEqualTo(2);
            });
            return all;
        }
    }

    /** Record count on a topic, read with a throwaway consumer that never commits. */
    private long countRecords(String topic) {
        Map<String, Object> props =
                new HashMap<>(KafkaTestUtils.consumerProps("counter-" + System.nanoTime(), "false", broker));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArrayDeserializer.class);

        try (Consumer<String, byte[]> reader =
                     new DefaultKafkaConsumerFactory<String, byte[]>(props).createConsumer()) {
            broker.consumeFromAnEmbeddedTopic(reader, topic);
            return reader.endOffsets(reader.assignment()).values().stream()
                    .mapToLong(Long::longValue).sum();
        }
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
