package com.assignment.orders.dlq;

import com.assignment.orders.avro.Order;
import com.assignment.orders.config.AppConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Prints everything currently sitting in the dead letter queue, with the diagnostics that were
 * attached when each message was abandoned.
 *
 * <p>A DLQ nobody can read is just a slower way of dropping messages, so this is the tool that
 * makes the whole mechanism worth having. For each message it shows why it failed, how many
 * attempts it took, and where on the source topic it came from -- enough to reproduce the
 * failure or, once the bug is fixed, to replay it.
 *
 * <p>It deliberately does <em>not</em> join a consumer group. It assigns the partitions directly
 * and seeks to the beginning, so it reads the full contents every time and never commits an
 * offset. Running it does not consume the queue or interfere with any other reader.
 */
public final class DlqViewer {

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    /** Stop after this long without a new record; the queue is assumed fully read. */
    private static final Duration IDLE_TIMEOUT = Duration.ofSeconds(3);

    public static void main(String[] args) {
        String bootstrapServers = AppConfig.bootstrapServers();
        String schemaRegistryUrl = AppConfig.schemaRegistryUrl();
        String dlqTopic = AppConfig.dlqTopic();

        System.out.println("""
                ------------------------------------------------------------
                  DEAD LETTER QUEUE VIEWER
                ------------------------------------------------------------""");
        System.out.printf("  broker : %s%n", bootstrapServers);
        System.out.printf("  topic  : %s%n", dlqTopic);
        System.out.println("------------------------------------------------------------");
        System.out.println();

        KafkaAvroDeserializer avroDeserializer = createAvroDeserializer(schemaRegistryUrl);
        List<ConsumerRecord<String, byte[]>> records = readAll(bootstrapServers, dlqTopic);

        if (records.isEmpty()) {
            System.out.println("  The dead letter queue is empty.");
            System.out.println();
            System.out.println("  Run the producer and consumer for a while: invalid orders and");
            System.out.println("  poison pills will start arriving here.");
            return;
        }

        Map<DlqReason, Integer> counts = new EnumMap<>(DlqReason.class);

        for (ConsumerRecord<String, byte[]> record : records) {
            Map<String, String> headers = readHeaders(record);
            DlqReason reason = parseReason(headers.get(DlqHeaders.REASON));
            counts.merge(reason, 1, Integer::sum);

            System.out.println("  " + "-".repeat(74));
            System.out.printf("  DLQ offset %-8s  key %-10s  %s%n",
                    record.offset(),
                    record.key() == null ? "(none)" : record.key(),
                    headers.getOrDefault(DlqHeaders.FAILED_AT, ""));
            System.out.printf("    reason      : %s%n", reason);
            System.out.printf("    attempts    : %s%n", headers.getOrDefault(DlqHeaders.ATTEMPTS, "?"));
            System.out.printf("    origin      : %s[%s] offset %s%n",
                    headers.getOrDefault(DlqHeaders.ORIGINAL_TOPIC, "?"),
                    headers.getOrDefault(DlqHeaders.ORIGINAL_PARTITION, "?"),
                    headers.getOrDefault(DlqHeaders.ORIGINAL_OFFSET, "?"));
            System.out.printf("    error       : %s%n",
                    headers.getOrDefault(DlqHeaders.ERROR_CLASS, "?"));
            System.out.printf("                  %s%n",
                    headers.getOrDefault(DlqHeaders.ERROR_MESSAGE, ""));
            System.out.printf("    payload     : %s%n", describePayload(record, avroDeserializer));
        }

        System.out.println("  " + "-".repeat(74));
        System.out.println();
        System.out.printf("  %d message%s in the dead letter queue:%n",
                records.size(), records.size() == 1 ? "" : "s");
        for (DlqReason reason : DlqReason.values()) {
            int count = counts.getOrDefault(reason, 0);
            if (count > 0) {
                System.out.printf("      %-24s %d%n", reason, count);
            }
        }
        System.out.println();
    }

    /**
     * Renders the payload. Records that failed validation still decode, so their fields are
     * shown; poison pills cannot decode, so a hex and text preview of the raw bytes is shown
     * instead -- which is exactly what you need to work out what the foreign producer sent.
     */
    private static String describePayload(ConsumerRecord<String, byte[]> record,
                                          KafkaAvroDeserializer avroDeserializer) {
        byte[] value = record.value();
        if (value == null) {
            return "(null)";
        }
        try {
            Object decoded = avroDeserializer.deserialize(record.topic(), value);
            if (decoded instanceof Order order) {
                return String.format("orderId=%s product=%s price=%.2f",
                        order.getOrderId(),
                        order.getProduct().isBlank() ? "(blank)" : order.getProduct(),
                        order.getPrice());
            }
            return String.valueOf(decoded);
        } catch (Exception e) {
            return String.format("undecodable, %d bytes%n                  hex  %s%n                  text %s",
                    value.length, toHex(value, 16), toPrintable(value, 48));
        }
    }

    /** Reads the whole topic without joining a consumer group or committing anything. */
    private static List<ConsumerRecord<String, byte[]>> readAll(String bootstrapServers, String topic) {
        List<ConsumerRecord<String, byte[]>> collected = new ArrayList<>();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "dlq-viewer");

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            if (partitionInfos == null || partitionInfos.isEmpty()) {
                System.out.printf("  Topic %s does not exist yet.%n", topic);
                return collected;
            }

            List<TopicPartition> partitions = new ArrayList<>();
            for (PartitionInfo info : partitionInfos) {
                partitions.add(new TopicPartition(topic, info.partition()));
            }

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            long idleDeadline = System.currentTimeMillis() + IDLE_TIMEOUT.toMillis();
            while (System.currentTimeMillis() < idleDeadline) {
                ConsumerRecords<String, byte[]> polled = consumer.poll(POLL_TIMEOUT);
                if (polled.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<String, byte[]> record : polled) {
                    collected.add(record);
                }
                idleDeadline = System.currentTimeMillis() + IDLE_TIMEOUT.toMillis();
            }
        }
        return collected;
    }

    private static Map<String, String> readHeaders(ConsumerRecord<String, byte[]> record) {
        Map<String, String> headers = new HashMap<>();
        for (Header header : record.headers()) {
            headers.put(header.key(),
                    header.value() == null ? "" : new String(header.value(), StandardCharsets.UTF_8));
        }
        return headers;
    }

    private static DlqReason parseReason(String raw) {
        if (raw == null) {
            return DlqReason.UNEXPECTED_ERROR;
        }
        try {
            return DlqReason.valueOf(raw);
        } catch (IllegalArgumentException e) {
            return DlqReason.UNEXPECTED_ERROR;
        }
    }

    private static String toHex(byte[] bytes, int limit) {
        StringBuilder sb = new StringBuilder();
        int count = Math.min(bytes.length, limit);
        for (int i = 0; i < count; i++) {
            sb.append(String.format("%02x ", bytes[i]));
        }
        if (bytes.length > limit) {
            sb.append("...");
        }
        return sb.toString().trim();
    }

    private static String toPrintable(byte[] bytes, int limit) {
        StringBuilder sb = new StringBuilder();
        int count = Math.min(bytes.length, limit);
        for (int i = 0; i < count; i++) {
            char c = (char) (bytes[i] & 0xFF);
            sb.append(c >= 32 && c < 127 ? c : '.');
        }
        if (bytes.length > limit) {
            sb.append("...");
        }
        return sb.toString();
    }

    private static KafkaAvroDeserializer createAvroDeserializer(String schemaRegistryUrl) {
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
        deserializer.configure(config, false);
        return deserializer;
    }
}
