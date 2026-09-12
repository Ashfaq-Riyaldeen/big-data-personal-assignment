package com.ashfaq.bigdata.consumer;

import org.springframework.messaging.MessageHeaders;

import java.nio.charset.StandardCharsets;

/**
 * Reads the diagnostic headers Spring attaches to a record on its way to the dead letter queue.
 *
 * <p>Spring writes these headers itself; nothing here creates them. What this class does is turn
 * the raw values into something printable, because they arrive in two different shapes: numeric
 * headers such as the original offset come as four big-endian bytes, while string headers may
 * arrive either as bytes or already converted, depending on the path the record took.
 */
final class DlqHeaders {

    /** Spring wraps the original exception message in this before storing it as a header. */
    static final String LISTENER_FAILED_PREFIX = "Listener failed; ";

    private DlqHeaders() {
    }

    /** Header as text, decoding UTF-8 bytes if that is how it arrived; null if absent. */
    static String asString(MessageHeaders headers, String name) {
        Object value = headers.get(name);
        if (value == null) {
            return null;
        }
        return value instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8)
                : value.toString();
    }

    /** Header as a number, decoding Kafka's big-endian byte layout if needed; -1 if absent. */
    static long asNumber(MessageHeaders headers, String name) {
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

    /**
     * Removes Spring's "Listener failed; " wrapper, which adds nothing on screen, leaving only the
     * original reason. Returns "unknown" for a missing message.
     */
    static String reason(String message) {
        if (message == null) {
            return "unknown";
        }
        return message.startsWith(LISTENER_FAILED_PREFIX)
                ? message.substring(LISTENER_FAILED_PREFIX.length())
                : message;
    }

    /** The simple class name from a fully qualified one, or "unknown" if absent. */
    static String simpleName(String fullyQualifiedName) {
        if (fullyQualifiedName == null) {
            return "unknown";
        }
        int lastDot = fullyQualifiedName.lastIndexOf('.');
        return lastDot < 0 ? fullyQualifiedName : fullyQualifiedName.substring(lastDot + 1);
    }
}
