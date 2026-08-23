package com.assignment.orders.consumer;

import java.util.random.RandomGenerator;

/**
 * Exponential backoff with jitter, governing how a transient failure is retried.
 *
 * <p>The delay before attempt <i>n</i> is {@code initialBackoff * multiplier^(n-1)}, capped at
 * {@code maxBackoff}. With the defaults (200ms, x2, 3 attempts) a record waits 200ms then 400ms
 * before being dead-lettered.
 *
 * <p><b>Why back off at all?</b> If a downstream service is failing because it is overloaded,
 * retrying immediately adds load to something already struggling. Backing off gives it room to
 * recover.
 *
 * <p><b>Why jitter?</b> Without it, every consumer that hit the same outage retries at exactly
 * the same instants, so the recovering service is hit by synchronised spikes -- a thundering
 * herd that can knock it straight back down. Spreading each delay randomly over a band around
 * its nominal value de-synchronises the retries.
 *
 * <p>The randomness is injected rather than taken from a static source, so tests can pin it
 * and assert exact delays.
 */
public final class RetryPolicy {

    private final int maxAttempts;
    private final long initialBackoffMillis;
    private final double multiplier;
    private final long maxBackoffMillis;
    private final double jitterFactor;
    private final RandomGenerator random;

    public RetryPolicy(int maxAttempts,
                       long initialBackoffMillis,
                       double multiplier,
                       long maxBackoffMillis,
                       double jitterFactor,
                       RandomGenerator random) {

        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1, got " + maxAttempts);
        }
        if (initialBackoffMillis < 0) {
            throw new IllegalArgumentException("initialBackoffMillis must not be negative, got " + initialBackoffMillis);
        }
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier must be at least 1.0, got " + multiplier);
        }
        if (maxBackoffMillis < initialBackoffMillis) {
            throw new IllegalArgumentException(
                    "maxBackoffMillis (" + maxBackoffMillis + ") must be >= initialBackoffMillis (" + initialBackoffMillis + ")");
        }
        if (jitterFactor < 0.0 || jitterFactor > 1.0) {
            throw new IllegalArgumentException("jitterFactor must be within [0, 1], got " + jitterFactor);
        }

        this.maxAttempts = maxAttempts;
        this.initialBackoffMillis = initialBackoffMillis;
        this.multiplier = multiplier;
        this.maxBackoffMillis = maxBackoffMillis;
        this.jitterFactor = jitterFactor;
        this.random = random;
    }

    /** Total attempts allowed per record, including the first one. */
    public int maxAttempts() {
        return maxAttempts;
    }

    /**
     * Whether another attempt is permitted.
     *
     * @param attemptsMade attempts already completed, all of which failed
     */
    public boolean shouldRetry(int attemptsMade) {
        return attemptsMade < maxAttempts;
    }

    /**
     * The nominal delay before the given attempt, ignoring jitter.
     *
     * @param attempt 1-based index of the attempt that is about to be made; the delay before
     *                attempt 2 is the initial backoff
     */
    public long baseBackoffMillis(int attempt) {
        if (attempt <= 1) {
            return 0L;
        }
        double raw = initialBackoffMillis * Math.pow(multiplier, attempt - 2.0);
        // Compare as double before casting: at large attempt counts the pow() result can
        // overflow a long and wrap negative.
        return raw >= maxBackoffMillis ? maxBackoffMillis : (long) raw;
    }

    /**
     * The actual delay to sleep before the given attempt, spread randomly over
     * {@code base * [1 - jitterFactor, 1 + jitterFactor]} and clamped to the configured ceiling.
     */
    public long backoffMillis(int attempt) {
        long base = baseBackoffMillis(attempt);
        if (base == 0L || jitterFactor == 0.0) {
            return base;
        }
        // random.nextDouble() is in [0,1), so `spread` lands in [-jitterFactor, +jitterFactor).
        double spread = (random.nextDouble() * 2.0 - 1.0) * jitterFactor;
        long jittered = Math.round(base * (1.0 + spread));
        return Math.max(0L, Math.min(jittered, maxBackoffMillis));
    }

    /** Human-readable summary for the startup banner. */
    public String describe() {
        return String.format(
                "%d attempts, backoff %dms x%.1f (max %dms), jitter +/-%.0f%%",
                maxAttempts, initialBackoffMillis, multiplier, maxBackoffMillis, jitterFactor * 100);
    }
}
