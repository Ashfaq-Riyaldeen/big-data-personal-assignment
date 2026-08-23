package com.assignment.orders.consumer;

import com.assignment.orders.dlq.DlqReason;
import com.assignment.orders.exception.PermanentProcessingException;
import com.assignment.orders.exception.TransientProcessingException;
import org.apache.avro.AvroRuntimeException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeoutException;

/**
 * Decides whether a failure is worth retrying.
 *
 * <p>This is the judgement call the whole reliability design rests on. Get it wrong in one
 * direction and you retry a malformed record until the retry budget is gone, stalling the
 * partition behind it for no possible benefit. Get it wrong the other way and a two-second
 * network blip permanently discards good orders into the DLQ.
 *
 * <p>The default for anything unrecognised is {@link Kind#PERMANENT}. That is the safer bias:
 * an unknown fault sent to the DLQ is visible, inspectable, and replayable, whereas an unknown
 * fault retried forever silently wedges a partition and is far harder to notice.
 */
public final class ErrorClassifier {

    private ErrorClassifier() {
    }

    public enum Kind {
        /** Expected to pass on a later attempt; retry with backoff. */
        TRANSIENT,
        /** Will fail identically forever; dead-letter immediately. */
        PERMANENT
    }

    /** Classifies a failure, unwrapping causes if the top-level type is inconclusive. */
    public static Kind classify(Throwable error) {
        for (Throwable current = error; current != null; current = current.getCause()) {
            Kind direct = classifyDirect(current);
            if (direct != null) {
                return direct;
            }
            // A self-referencing cause chain would otherwise spin forever.
            if (current.getCause() == current) {
                break;
            }
        }
        return Kind.PERMANENT;
    }

    /** Classifies a single throwable, or returns null if this type says nothing either way. */
    private static Kind classifyDirect(Throwable error) {
        // Our own signals are explicit and always win.
        if (error instanceof TransientProcessingException) {
            return Kind.TRANSIENT;
        }
        if (error instanceof PermanentProcessingException) {
            return Kind.PERMANENT;
        }

        // Bad bytes stay bad. Checked before RetriableException because Kafka's
        // SerializationException is emphatically not something to retry.
        if (error instanceof SerializationException || error instanceof AvroRuntimeException) {
            return Kind.PERMANENT;
        }

        // Kafka marks the faults it considers worth another go.
        if (error instanceof RetriableException) {
            return Kind.TRANSIENT;
        }

        // Infrastructure wobbles: reachability and timeouts.
        if (error instanceof TimeoutException
                || error instanceof SocketTimeoutException
                || error instanceof ConnectException
                || error instanceof org.apache.kafka.common.errors.TimeoutException) {
            return Kind.TRANSIENT;
        }

        // A general IOException is usually a broken pipe or a dropped connection.
        // Deliberately checked last so its more specific subclasses match first.
        if (error instanceof IOException) {
            return Kind.TRANSIENT;
        }

        return null;
    }

    /**
     * The DLQ reason to stamp on a record that is being given up on.
     *
     * @param error       the failure that ended processing
     * @param wasRetried  whether the record had already burned retry attempts
     */
    public static DlqReason reasonFor(Throwable error, boolean wasRetried) {
        if (wasRetried) {
            return DlqReason.RETRIES_EXHAUSTED;
        }
        if (error instanceof PermanentProcessingException) {
            return DlqReason.VALIDATION_FAILED;
        }
        if (error instanceof SerializationException || error instanceof AvroRuntimeException) {
            return DlqReason.DESERIALIZATION_FAILED;
        }
        return DlqReason.UNEXPECTED_ERROR;
    }
}
