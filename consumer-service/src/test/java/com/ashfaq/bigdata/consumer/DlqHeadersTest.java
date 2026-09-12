package com.ashfaq.bigdata.consumer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.MessageHeaders;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The header values Spring writes arrive in more than one shape; these tests cover each one so
 * the [DLQ] log line never prints garbage or throws.
 */
class DlqHeadersTest {

    @Test
    @DisplayName("a numeric header stored as four big-endian bytes decodes to the number")
    void decodesBigEndianBytes() {
        // This is how Kafka serialises the original partition and offset headers.
        byte[] offsetBytes = ByteBuffer.allocate(4).putInt(258).array();
        MessageHeaders headers = headers(Map.of("kafka_original-offset", offsetBytes));

        assertThat(DlqHeaders.asNumber(headers, "kafka_original-offset")).isEqualTo(258);
    }

    @Test
    @DisplayName("a numeric header already converted to a Number passes straight through")
    void passesNumbersThrough() {
        MessageHeaders headers = headers(Map.of("kafka_original-partition", 0));

        assertThat(DlqHeaders.asNumber(headers, "kafka_original-partition")).isEqualTo(0);
    }

    @Test
    @DisplayName("a missing numeric header reads as -1 rather than throwing")
    void missingNumberIsMinusOne() {
        assertThat(DlqHeaders.asNumber(headers(Map.of()), "absent")).isEqualTo(-1);
    }

    @Test
    @DisplayName("a string header stored as UTF-8 bytes decodes to text")
    void decodesUtf8Bytes() {
        byte[] topicBytes = "orders.v1".getBytes(StandardCharsets.UTF_8);
        MessageHeaders headers = headers(Map.of("kafka_original-topic", topicBytes));

        assertThat(DlqHeaders.asString(headers, "kafka_original-topic")).isEqualTo("orders.v1");
    }

    @Test
    @DisplayName("a string header already converted passes straight through")
    void passesStringsThrough() {
        MessageHeaders headers = headers(Map.of("kafka_original-topic", "orders.v1"));

        assertThat(DlqHeaders.asString(headers, "kafka_original-topic")).isEqualTo("orders.v1");
    }

    @Test
    @DisplayName("a missing string header reads as null")
    void missingStringIsNull() {
        assertThat(DlqHeaders.asString(headers(Map.of()), "absent")).isNull();
    }

    @Test
    @DisplayName("Spring's 'Listener failed; ' wrapper is stripped from the reason")
    void stripsListenerFailedPrefix() {
        assertThat(DlqHeaders.reason("Listener failed; Price must be greater than zero"))
                .isEqualTo("Price must be greater than zero");
    }

    @Test
    @DisplayName("a reason without the wrapper is left untouched")
    void leavesOtherReasonsAlone() {
        assertThat(DlqHeaders.reason("Downstream unavailable (attempt 3/3)"))
                .isEqualTo("Downstream unavailable (attempt 3/3)");
    }

    @Test
    @DisplayName("a missing reason reads as 'unknown'")
    void missingReasonIsUnknown() {
        assertThat(DlqHeaders.reason(null)).isEqualTo("unknown");
    }

    @Test
    @DisplayName("a fully qualified class name is reduced to its simple name")
    void reducesToSimpleName() {
        assertThat(DlqHeaders.simpleName(
                "com.ashfaq.bigdata.consumer.exception.PermanentOrderException"))
                .isEqualTo("PermanentOrderException");
    }

    @Test
    @DisplayName("a name with no package, or no name at all, is handled")
    void handlesEdgeCaseNames() {
        assertThat(DlqHeaders.simpleName("NoPackage")).isEqualTo("NoPackage");
        assertThat(DlqHeaders.simpleName(null)).isEqualTo("unknown");
    }

    private static MessageHeaders headers(Map<String, Object> values) {
        return new MessageHeaders(values);
    }
}
