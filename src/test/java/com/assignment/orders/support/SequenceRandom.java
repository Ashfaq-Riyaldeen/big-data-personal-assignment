package com.assignment.orders.support;

import java.util.random.RandomGenerator;

/**
 * A {@link RandomGenerator} that hands out a scripted sequence of values.
 *
 * <p>Retry backoff and fault injection are both driven by randomness, which would otherwise make
 * them awkward to assert on. Injecting this instead turns "usually fails about 15% of the time"
 * into "fails on exactly this call", so the tests are exact rather than statistical.
 *
 * <p>The sequence wraps around, so a single value can be supplied to pin every draw.
 */
public final class SequenceRandom implements RandomGenerator {

    private final double[] doubles;
    private int cursor = 0;

    public SequenceRandom(double... doubles) {
        if (doubles.length == 0) {
            throw new IllegalArgumentException("at least one value is required");
        }
        this.doubles = doubles.clone();
    }

    /** Always draws the same value. */
    public static SequenceRandom always(double value) {
        return new SequenceRandom(value);
    }

    @Override
    public double nextDouble() {
        double value = doubles[cursor % doubles.length];
        cursor++;
        return value;
    }

    @Override
    public int nextInt(int bound) {
        return 0;
    }

    @Override
    public void nextBytes(byte[] bytes) {
        java.util.Arrays.fill(bytes, (byte) 0);
    }

    /** {@link RandomGenerator}'s only abstract method; unused by the code under test. */
    @Override
    public long nextLong() {
        return 0L;
    }
}
