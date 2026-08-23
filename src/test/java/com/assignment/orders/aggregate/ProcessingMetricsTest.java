package com.assignment.orders.aggregate;

import com.assignment.orders.dlq.DlqReason;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

@DisplayName("ProcessingMetrics")
class ProcessingMetricsTest {

    @Test
    @DisplayName("starts at zero")
    void startsAtZero() {
        ProcessingMetrics.Snapshot snapshot = new ProcessingMetrics().snapshot();

        assertThat(snapshot.consumed()).isZero();
        assertThat(snapshot.processedOk()).isZero();
        assertThat(snapshot.retryAttempts()).isZero();
        assertThat(snapshot.deadLettered()).isZero();
        assertThat(snapshot.lastError()).isEmpty();
        for (DlqReason reason : DlqReason.values()) {
            assertThat(snapshot.countFor(reason)).isZero();
        }
    }

    @Test
    @DisplayName("counts a first-attempt success without crediting the retry logic")
    void firstAttemptSuccess() {
        ProcessingMetrics metrics = new ProcessingMetrics();
        metrics.recordConsumed();
        metrics.recordSuccess(1);

        ProcessingMetrics.Snapshot snapshot = metrics.snapshot();
        assertThat(snapshot.processedOk()).isEqualTo(1);
        assertThat(snapshot.recoveredAfterRetry()).isZero();
    }

    @Test
    @DisplayName("counts a later-attempt success as a recovery")
    void recoveryIsCredited() {
        ProcessingMetrics metrics = new ProcessingMetrics();
        metrics.recordSuccess(3);

        ProcessingMetrics.Snapshot snapshot = metrics.snapshot();
        assertThat(snapshot.processedOk()).isEqualTo(1);
        assertThat(snapshot.recoveredAfterRetry()).isEqualTo(1);
    }

    @Test
    @DisplayName("breaks dead letters down by reason")
    void countsByReason() {
        ProcessingMetrics metrics = new ProcessingMetrics();
        metrics.recordDeadLettered(DlqReason.VALIDATION_FAILED);
        metrics.recordDeadLettered(DlqReason.VALIDATION_FAILED);
        metrics.recordDeadLettered(DlqReason.DESERIALIZATION_FAILED);

        ProcessingMetrics.Snapshot snapshot = metrics.snapshot();
        assertThat(snapshot.deadLettered()).isEqualTo(3);
        assertThat(snapshot.countFor(DlqReason.VALIDATION_FAILED)).isEqualTo(2);
        assertThat(snapshot.countFor(DlqReason.DESERIALIZATION_FAILED)).isEqualTo(1);
        assertThat(snapshot.countFor(DlqReason.RETRIES_EXHAUSTED)).isZero();
    }

    @Test
    @DisplayName("retry success rate measures recoveries against retried records only")
    void retrySuccessRate() {
        ProcessingMetrics metrics = new ProcessingMetrics();

        // Three records recovered after retrying; one burned its whole budget and was dropped.
        metrics.recordSuccess(2);
        metrics.recordSuccess(2);
        metrics.recordSuccess(3);
        metrics.recordDeadLettered(DlqReason.RETRIES_EXHAUSTED);

        // Records that never failed are excluded, so the figure reports how well retrying works
        // rather than how healthy the stream is overall.
        assertThat(metrics.snapshot().retrySuccessRate()).isCloseTo(0.75, within(1e-9));
    }

    @Test
    @DisplayName("retry success rate is zero when nothing has been retried")
    void retrySuccessRateWithoutRetries() {
        ProcessingMetrics metrics = new ProcessingMetrics();
        metrics.recordSuccess(1);

        // Guards against dividing by zero on a clean run.
        assertThat(metrics.snapshot().retrySuccessRate()).isZero();
    }

    @Test
    @DisplayName("validation failures are excluded from the retry success rate")
    void validationFailuresDoNotSkewRetryRate() {
        ProcessingMetrics metrics = new ProcessingMetrics();
        metrics.recordSuccess(2);
        metrics.recordDeadLettered(DlqReason.VALIDATION_FAILED);
        metrics.recordDeadLettered(DlqReason.DESERIALIZATION_FAILED);

        // These never entered the retry loop, so counting them would understate how well the
        // retry logic is doing.
        assertThat(metrics.snapshot().retrySuccessRate()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("keeps the most recent error message")
    void keepsLastError() {
        ProcessingMetrics metrics = new ProcessingMetrics();
        metrics.recordLastError("first");
        metrics.recordLastError("second");

        assertThat(metrics.snapshot().lastError()).isEqualTo("second");
    }

    @Test
    @DisplayName("tolerates a null error message")
    void toleratesNullError() {
        ProcessingMetrics metrics = new ProcessingMetrics();
        metrics.recordLastError(null);

        assertThat(metrics.snapshot().lastError()).isEmpty();
    }

    @Test
    @DisplayName("counters survive concurrent updates from several threads")
    void countersAreThreadSafe() throws InterruptedException {
        // The consumer thread writes while the dashboard thread reads, so the counters have to
        // hold up under concurrency.
        ProcessingMetrics metrics = new ProcessingMetrics();
        int threads = 8;
        int perThread = 5_000;

        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            Thread worker = new Thread(() -> {
                for (int n = 0; n < perThread; n++) {
                    metrics.recordConsumed();
                    metrics.recordSuccess(1);
                }
            });
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join();
        }

        assertThat(metrics.snapshot().consumed()).isEqualTo((long) threads * perThread);
        assertThat(metrics.snapshot().processedOk()).isEqualTo((long) threads * perThread);
    }
}
