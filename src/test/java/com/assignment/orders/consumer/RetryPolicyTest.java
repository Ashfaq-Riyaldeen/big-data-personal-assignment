package com.assignment.orders.consumer;

import com.assignment.orders.support.SequenceRandom;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.random.RandomGenerator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("RetryPolicy (exponential backoff with jitter)")
class RetryPolicyTest {

    /** Draws 0.5, which maps to zero jitter, isolating the exponential growth. */
    private static final RandomGenerator NO_JITTER = SequenceRandom.always(0.5);

    private static RetryPolicy policy(RandomGenerator random) {
        return new RetryPolicy(3, 200, 2.0, 5_000, 0.2, random);
    }

    @Test
    @DisplayName("does not wait before the first attempt")
    void firstAttemptIsImmediate() {
        assertThat(policy(NO_JITTER).baseBackoffMillis(1)).isZero();
    }

    @Test
    @DisplayName("doubles the delay for each successive attempt")
    void backoffGrowsExponentially() {
        RetryPolicy retryPolicy = policy(NO_JITTER);

        assertThat(retryPolicy.baseBackoffMillis(2)).isEqualTo(200);
        assertThat(retryPolicy.baseBackoffMillis(3)).isEqualTo(400);
        assertThat(retryPolicy.baseBackoffMillis(4)).isEqualTo(800);
        assertThat(retryPolicy.baseBackoffMillis(5)).isEqualTo(1_600);
    }

    @Test
    @DisplayName("never exceeds the configured ceiling, even at absurd attempt counts")
    void backoffIsCapped() {
        RetryPolicy retryPolicy = policy(NO_JITTER);

        assertThat(retryPolicy.baseBackoffMillis(10)).isEqualTo(5_000);
        // Guards the overflow case: Math.pow grows past Long.MAX_VALUE and would wrap negative
        // if the cap were applied after the cast rather than before it.
        assertThat(retryPolicy.baseBackoffMillis(500)).isEqualTo(5_000);
        assertThat(retryPolicy.backoffMillis(500)).isLessThanOrEqualTo(5_000);
    }

    @Test
    @DisplayName("allows exactly maxAttempts tries in total")
    void exhaustsAfterMaxAttempts() {
        RetryPolicy retryPolicy = policy(NO_JITTER);

        assertThat(retryPolicy.maxAttempts()).isEqualTo(3);
        assertThat(retryPolicy.shouldRetry(1)).isTrue();
        assertThat(retryPolicy.shouldRetry(2)).isTrue();
        // Three attempts have now been made, which is the whole budget.
        assertThat(retryPolicy.shouldRetry(3)).isFalse();
        assertThat(retryPolicy.shouldRetry(4)).isFalse();
    }

    @Test
    @DisplayName("a single-attempt policy never retries")
    void singleAttemptPolicyNeverRetries() {
        RetryPolicy retryPolicy = new RetryPolicy(1, 200, 2.0, 5_000, 0.2, NO_JITTER);
        assertThat(retryPolicy.shouldRetry(1)).isFalse();
    }

    @Test
    @DisplayName("jitter spreads the delay across the expected band")
    void jitterSpreadsTheDelay() {
        // 0.0 -> the bottom of the band, 0.5 -> the centre, 0.75 -> halfway up.
        RetryPolicy retryPolicy = policy(new SequenceRandom(0.0, 0.5, 0.75));

        assertThat(retryPolicy.backoffMillis(2)).isEqualTo(160);   // 200 * (1 - 0.2)
        assertThat(retryPolicy.backoffMillis(2)).isEqualTo(200);   // 200 * (1 + 0.0)
        assertThat(retryPolicy.backoffMillis(2)).isEqualTo(220);   // 200 * (1 + 0.1)
    }

    @Test
    @DisplayName("every jittered delay stays inside the band, whatever the draw")
    void jitterStaysWithinBounds() {
        RetryPolicy retryPolicy = new RetryPolicy(
                10, 200, 2.0, 5_000, 0.2, RandomGenerator.getDefault());

        for (int attempt = 2; attempt <= 8; attempt++) {
            long base = retryPolicy.baseBackoffMillis(attempt);
            for (int draw = 0; draw < 200; draw++) {
                assertThat(retryPolicy.backoffMillis(attempt))
                        .isBetween(Math.round(base * 0.8), Math.round(base * 1.2));
            }
        }
    }

    @Test
    @DisplayName("zero jitter yields the exact nominal delay")
    void zeroJitterIsDeterministic() {
        RetryPolicy retryPolicy = new RetryPolicy(
                3, 200, 2.0, 5_000, 0.0, RandomGenerator.getDefault());

        assertThat(retryPolicy.backoffMillis(2)).isEqualTo(200);
        assertThat(retryPolicy.backoffMillis(3)).isEqualTo(400);
    }

    @Test
    @DisplayName("rejects a nonsensical configuration at construction time")
    void rejectsInvalidConfiguration() {
        assertThatThrownBy(() -> new RetryPolicy(0, 200, 2.0, 5_000, 0.2, NO_JITTER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxAttempts");

        assertThatThrownBy(() -> new RetryPolicy(3, -1, 2.0, 5_000, 0.2, NO_JITTER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initialBackoffMillis");

        assertThatThrownBy(() -> new RetryPolicy(3, 200, 0.5, 5_000, 0.2, NO_JITTER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multiplier");

        assertThatThrownBy(() -> new RetryPolicy(3, 200, 2.0, 100, 0.2, NO_JITTER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBackoffMillis");

        assertThatThrownBy(() -> new RetryPolicy(3, 200, 2.0, 5_000, 1.5, NO_JITTER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jitterFactor");
    }
}
