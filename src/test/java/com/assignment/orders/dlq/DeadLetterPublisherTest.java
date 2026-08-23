package com.assignment.orders.dlq;

import com.assignment.orders.exception.PermanentProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("DeadLetterPublisher")
class DeadLetterPublisherTest {

    private static final String DLQ_TOPIC = "orders.DLQ";
    private static final String CONSUMER_GROUP = "order-processor";

    private MockProducer<String, byte[]> producer;
    private DeadLetterPublisher publisher;

    @BeforeEach
    void setUp() {
        producer = new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
        publisher = new DeadLetterPublisher(producer, DLQ_TOPIC, CONSUMER_GROUP);
    }

    @AfterEach
    void tearDown() {
        publisher.close();
    }

    private static ConsumerRecord<String, byte[]> sourceRecord(byte[] value) {
        return new ConsumerRecord<>(
                "orders", 2, 4711L, 1_700_000_000_000L, TimestampType.CREATE_TIME,
                ConsumerRecord.NULL_SIZE, ConsumerRecord.NULL_SIZE, "1001", value,
                new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());
    }

    private static String header(ProducerRecord<String, byte[]> record, String key) {
        var found = record.headers().lastHeader(key);
        return found == null ? null : new String(found.value(), StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("publishes to the dead letter topic, preserving the key")
    void publishesToDlqTopic() {
        publisher.publish(sourceRecord("payload".getBytes(StandardCharsets.UTF_8)),
                DlqReason.VALIDATION_FAILED, new PermanentProcessingException("price is negative"), 1);

        assertThat(producer.history()).hasSize(1);
        ProducerRecord<String, byte[]> sent = producer.history().get(0);
        assertThat(sent.topic()).isEqualTo(DLQ_TOPIC);
        assertThat(sent.key()).isEqualTo("1001");
    }

    @Test
    @DisplayName("carries the original bytes through untouched")
    void preservesOriginalBytesExactly() {
        // The critical property: a poison pill cannot be re-serialised, so the DLQ has to hold
        // whatever arrived, byte for byte. That is also what makes replay possible later.
        byte[] original = {0x00, 0x00, (byte) 0xFF, (byte) 0xFF, (byte) 0xFE, 0x17, 0x42};

        publisher.publish(sourceRecord(original), DlqReason.DESERIALIZATION_FAILED,
                new IllegalStateException("Unknown magic byte!"), 1);

        assertThat(producer.history().get(0).value()).isEqualTo(original);
    }

    @Test
    @DisplayName("stamps every diagnostic header needed to trace the failure")
    void stampsDiagnosticHeaders() {
        publisher.publish(sourceRecord("payload".getBytes(StandardCharsets.UTF_8)),
                DlqReason.RETRIES_EXHAUSTED,
                new PermanentProcessingException("gateway timeout"), 3);

        ProducerRecord<String, byte[]> sent = producer.history().get(0);

        assertThat(header(sent, DlqHeaders.REASON)).isEqualTo("RETRIES_EXHAUSTED");
        assertThat(header(sent, DlqHeaders.ERROR_CLASS))
                .isEqualTo(PermanentProcessingException.class.getName());
        assertThat(header(sent, DlqHeaders.ERROR_MESSAGE)).isEqualTo("gateway timeout");
        assertThat(header(sent, DlqHeaders.ATTEMPTS)).isEqualTo("3");
        assertThat(header(sent, DlqHeaders.CONSUMER_GROUP)).isEqualTo(CONSUMER_GROUP);

        // Origin coordinates: enough to find the message on the source topic again.
        assertThat(header(sent, DlqHeaders.ORIGINAL_TOPIC)).isEqualTo("orders");
        assertThat(header(sent, DlqHeaders.ORIGINAL_PARTITION)).isEqualTo("2");
        assertThat(header(sent, DlqHeaders.ORIGINAL_OFFSET)).isEqualTo("4711");
        assertThat(header(sent, DlqHeaders.ORIGINAL_TIMESTAMP)).isEqualTo("1700000000000");

        assertThat(header(sent, DlqHeaders.FAILED_AT)).isNotBlank();
    }

    @Test
    @DisplayName("keeps headers set by the original producer")
    void preservesOriginalHeaders() {
        ConsumerRecord<String, byte[]> record = sourceRecord("payload".getBytes(StandardCharsets.UTF_8));
        record.headers().add("trace-id", "abc-123".getBytes(StandardCharsets.UTF_8));

        publisher.publish(record, DlqReason.VALIDATION_FAILED,
                new PermanentProcessingException("bad"), 1);

        // Upstream tracing context has to survive the trip, or the DLQ record cannot be
        // correlated back to the request that produced it.
        assertThat(header(producer.history().get(0), "trace-id")).isEqualTo("abc-123");
    }

    @Test
    @DisplayName("copes with an exception that carries no message")
    void handlesMessagelessException() {
        publisher.publish(sourceRecord("payload".getBytes(StandardCharsets.UTF_8)),
                DlqReason.UNEXPECTED_ERROR, new IllegalStateException(), 1);

        assertThat(header(producer.history().get(0), DlqHeaders.ERROR_MESSAGE))
                .isEqualTo("IllegalStateException");
    }

    @Test
    @DisplayName("records the reason for each failure class")
    void recordsEveryReason() {
        for (DlqReason reason : DlqReason.values()) {
            publisher.publish(sourceRecord("payload".getBytes(StandardCharsets.UTF_8)),
                    reason, new IllegalStateException("boom"), 1);
        }

        assertThat(producer.history())
                .hasSize(DlqReason.values().length)
                .allSatisfy(record -> assertThat(header(record, DlqHeaders.REASON)).isNotBlank());
    }
}
