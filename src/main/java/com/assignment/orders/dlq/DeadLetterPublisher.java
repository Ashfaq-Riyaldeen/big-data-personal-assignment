package com.assignment.orders.dlq;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Publishes messages the consumer has given up on to the dead letter queue.
 *
 * <p>Two decisions here carry the weight:
 *
 * <p><b>The payload is the original bytes, untouched.</b> Not a JSON envelope, not a
 * re-serialised object. A poison pill is precisely a message that could not be turned into an
 * object, so any format that requires understanding the payload cannot represent the records
 * you most need to keep. Byte-identical payloads also replay onto the source topic directly
 * once the underlying bug is fixed.
 *
 * <p><b>The send is synchronous.</b> {@code send()} on its own only queues a record into the
 * client's buffer; if the consumer then commits its offset and exits, the record can be lost
 * from both topics at once -- the one outcome a DLQ exists to prevent. Blocking on the future
 * means that by the time the caller proceeds to commit, the broker has acknowledged the record.
 */
public final class DeadLetterPublisher implements AutoCloseable {

    private final Producer<String, byte[]> producer;
    private final String dlqTopic;
    private final String consumerGroup;

    public DeadLetterPublisher(Producer<String, byte[]> producer, String dlqTopic, String consumerGroup) {
        this.producer = producer;
        this.dlqTopic = dlqTopic;
        this.consumerGroup = consumerGroup;
    }

    /** Builds a publisher backed by a real Kafka producer configured for durability. */
    public static DeadLetterPublisher create(String bootstrapServers, String dlqTopic, String consumerGroup) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // The DLQ is the last line of defence, so it is configured for durability over speed:
        // wait for all in-sync replicas, and never let a retry reorder or duplicate a record.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "dlq-publisher-" + consumerGroup);

        return new DeadLetterPublisher(new KafkaProducer<>(props), dlqTopic, consumerGroup);
    }

    /**
     * Sends a failed record to the DLQ and blocks until the broker acknowledges it.
     *
     * @param record   the original record, exactly as consumed
     * @param reason   why it is being abandoned
     * @param error    the failure that ended processing
     * @param attempts how many attempts were made before giving up
     * @return broker metadata for the dead-lettered record
     * @throws DeadLetterPublishException if the record could not be durably written
     */
    public RecordMetadata publish(ConsumerRecord<String, byte[]> record,
                                  DlqReason reason,
                                  Throwable error,
                                  int attempts) {

        ProducerRecord<String, byte[]> dlqRecord =
                new ProducerRecord<>(dlqTopic, null, record.key(), record.value());

        // Carry over anything the original producer attached, so upstream tracing context
        // survives the trip. Our own dlq.* headers are added afterwards and win on conflict.
        for (Header header : record.headers()) {
            dlqRecord.headers().add(header);
        }

        addHeader(dlqRecord, DlqHeaders.REASON, reason.name());
        addHeader(dlqRecord, DlqHeaders.ERROR_CLASS, error == null ? "unknown" : error.getClass().getName());
        addHeader(dlqRecord, DlqHeaders.ERROR_MESSAGE, describe(error));
        addHeader(dlqRecord, DlqHeaders.ORIGINAL_TOPIC, record.topic());
        addHeader(dlqRecord, DlqHeaders.ORIGINAL_PARTITION, Integer.toString(record.partition()));
        addHeader(dlqRecord, DlqHeaders.ORIGINAL_OFFSET, Long.toString(record.offset()));
        addHeader(dlqRecord, DlqHeaders.ORIGINAL_TIMESTAMP, Long.toString(record.timestamp()));
        addHeader(dlqRecord, DlqHeaders.ATTEMPTS, Integer.toString(attempts));
        addHeader(dlqRecord, DlqHeaders.FAILED_AT, Instant.now().toString());
        addHeader(dlqRecord, DlqHeaders.CONSUMER_GROUP, consumerGroup);

        try {
            return producer.send(dlqRecord).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DeadLetterPublishException(
                    "Interrupted while writing to the dead letter queue " + dlqTopic, e);
        } catch (ExecutionException e) {
            throw new DeadLetterPublishException(
                    "Failed to write to the dead letter queue " + dlqTopic, e.getCause());
        }
    }

    private static void addHeader(ProducerRecord<String, byte[]> record, String key, String value) {
        record.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
    }

    private static String describe(Throwable error) {
        if (error == null) {
            return "";
        }
        String message = error.getMessage();
        return (message == null || message.isBlank()) ? error.getClass().getSimpleName() : message;
    }

    @Override
    public void close() {
        producer.close();
    }

    /**
     * Thrown when a record could not be durably written to the DLQ. The consumer treats this as
     * fatal: continuing past it and committing the offset would discard the record entirely.
     */
    public static class DeadLetterPublishException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public DeadLetterPublishException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
