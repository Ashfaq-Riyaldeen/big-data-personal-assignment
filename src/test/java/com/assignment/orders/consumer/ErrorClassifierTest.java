package com.assignment.orders.consumer;

import com.assignment.orders.dlq.DlqReason;
import com.assignment.orders.exception.PermanentProcessingException;
import com.assignment.orders.exception.TransientProcessingException;
import org.apache.avro.AvroRuntimeException;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("ErrorClassifier")
class ErrorClassifierTest {

    @Nested
    @DisplayName("transient failures, which are worth retrying")
    class Transient {

        @Test
        @DisplayName("our own transient signal")
        void ownSignal() {
            assertThat(ErrorClassifier.classify(new TransientProcessingException("gateway timeout")))
                    .isEqualTo(ErrorClassifier.Kind.TRANSIENT);
        }

        @Test
        @DisplayName("timeouts and connection failures")
        void infrastructureWobbles() {
            assertThat(ErrorClassifier.classify(new TimeoutException("timed out")))
                    .isEqualTo(ErrorClassifier.Kind.TRANSIENT);
            assertThat(ErrorClassifier.classify(new SocketTimeoutException("read timed out")))
                    .isEqualTo(ErrorClassifier.Kind.TRANSIENT);
            assertThat(ErrorClassifier.classify(new ConnectException("connection refused")))
                    .isEqualTo(ErrorClassifier.Kind.TRANSIENT);
        }

        @Test
        @DisplayName("Kafka's own retriable errors")
        void kafkaRetriable() {
            assertThat(ErrorClassifier.classify(new org.apache.kafka.common.errors.TimeoutException("no leader")))
                    .isEqualTo(ErrorClassifier.Kind.TRANSIENT);
        }

        @Test
        @DisplayName("a general IO failure, usually a dropped connection")
        void ioFailure() {
            assertThat(ErrorClassifier.classify(new IOException("broken pipe")))
                    .isEqualTo(ErrorClassifier.Kind.TRANSIENT);
        }

        @Test
        @DisplayName("a transient cause buried inside an unremarkable wrapper")
        void wrappedCause() {
            RuntimeException wrapped = new RuntimeException("processing failed",
                    new TransientProcessingException("downstream 503"));

            assertThat(ErrorClassifier.classify(wrapped)).isEqualTo(ErrorClassifier.Kind.TRANSIENT);
        }
    }

    @Nested
    @DisplayName("permanent failures, which must not be retried")
    class Permanent {

        @Test
        @DisplayName("our own permanent signal")
        void ownSignal() {
            assertThat(ErrorClassifier.classify(new PermanentProcessingException("price is negative")))
                    .isEqualTo(ErrorClassifier.Kind.PERMANENT);
        }

        @Test
        @DisplayName("deserialisation failures, because the same bytes always fail the same way")
        void deserialisationFailures() {
            assertThat(ErrorClassifier.classify(new SerializationException("Unknown magic byte!")))
                    .isEqualTo(ErrorClassifier.Kind.PERMANENT);
            assertThat(ErrorClassifier.classify(new AvroRuntimeException("bad record")))
                    .isEqualTo(ErrorClassifier.Kind.PERMANENT);
        }

        @Test
        @DisplayName("an unrecognised error defaults to permanent, the safer bias")
        void unknownDefaultsToPermanent() {
            // Retrying an unknown fault forever would silently wedge the partition. Sending it
            // to the DLQ makes it visible and replayable instead.
            assertThat(ErrorClassifier.classify(new IllegalStateException("who knows")))
                    .isEqualTo(ErrorClassifier.Kind.PERMANENT);
        }

        @Test
        @DisplayName("a null error is treated as permanent rather than throwing")
        void nullIsPermanent() {
            assertThat(ErrorClassifier.classify(null)).isEqualTo(ErrorClassifier.Kind.PERMANENT);
        }

        @Test
        @DisplayName("a self-referencing cause chain terminates instead of spinning")
        void selfReferencingCauseTerminates() {
            // Throwable.initCause refuses self-causation, so the loop has to be built by
            // overriding getCause(). Contrived, but it proves the walk cannot hang.
            RuntimeException loop = new RuntimeException("looping") {
                @Override
                public synchronized Throwable getCause() {
                    return this;
                }
            };

            assertThat(ErrorClassifier.classify(loop)).isEqualTo(ErrorClassifier.Kind.PERMANENT);
        }
    }

    @Nested
    @DisplayName("choosing the DLQ reason")
    class Reasons {

        @Test
        @DisplayName("anything already retried is recorded as exhausted")
        void retriedBecomesExhausted() {
            assertThat(ErrorClassifier.reasonFor(new TransientProcessingException("timeout"), true))
                    .isEqualTo(DlqReason.RETRIES_EXHAUSTED);
        }

        @Test
        @DisplayName("a business rule violation is recorded as a validation failure")
        void validationFailure() {
            assertThat(ErrorClassifier.reasonFor(new PermanentProcessingException("blank orderId"), false))
                    .isEqualTo(DlqReason.VALIDATION_FAILED);
        }

        @Test
        @DisplayName("bad bytes are recorded as a deserialisation failure")
        void deserialisationFailure() {
            assertThat(ErrorClassifier.reasonFor(new SerializationException("Unknown magic byte!"), false))
                    .isEqualTo(DlqReason.DESERIALIZATION_FAILED);
        }

        @Test
        @DisplayName("anything else is recorded as unexpected")
        void unexpected() {
            assertThat(ErrorClassifier.reasonFor(new IllegalStateException("surprise"), false))
                    .isEqualTo(DlqReason.UNEXPECTED_ERROR);
        }
    }
}
