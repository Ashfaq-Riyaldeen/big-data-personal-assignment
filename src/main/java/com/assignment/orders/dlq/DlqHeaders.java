package com.assignment.orders.dlq;

/**
 * Names of the Kafka headers attached to every dead-lettered message.
 *
 * <p>Diagnostics live in headers rather than in the message body on purpose. The body of a
 * DLQ record is the <em>original, unmodified bytes</em>, which is the only representation
 * that also works for a poison pill that could not be deserialised in the first place. Wrapping
 * the payload in a JSON envelope would mean the records you most need to inspect are the ones
 * you could not wrap.
 *
 * <p>Keeping the payload byte-identical also means a fixed message can be replayed onto the
 * source topic without any unwrapping step.
 */
public final class DlqHeaders {

    private DlqHeaders() {
    }

    /** The {@link DlqReason} name. */
    public static final String REASON = "dlq.reason";

    /** Fully qualified class name of the exception that caused the failure. */
    public static final String ERROR_CLASS = "dlq.error.class";

    /** The exception's message. */
    public static final String ERROR_MESSAGE = "dlq.error.message";

    /** Topic the message was originally consumed from. */
    public static final String ORIGINAL_TOPIC = "dlq.original.topic";

    /** Partition the message was originally consumed from. */
    public static final String ORIGINAL_PARTITION = "dlq.original.partition";

    /** Offset the message occupied on its original partition -- the key to finding it again. */
    public static final String ORIGINAL_OFFSET = "dlq.original.offset";

    /** The original record's Kafka timestamp, in epoch milliseconds. */
    public static final String ORIGINAL_TIMESTAMP = "dlq.original.timestamp";

    /** How many processing attempts were made before giving up. */
    public static final String ATTEMPTS = "dlq.attempts";

    /** When the message was dead-lettered, as an ISO-8601 instant. */
    public static final String FAILED_AT = "dlq.failed.at";

    /** Consumer group that gave up on the message. */
    public static final String CONSUMER_GROUP = "dlq.consumer.group";
}
