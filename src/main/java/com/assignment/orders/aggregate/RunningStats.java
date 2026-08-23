package com.assignment.orders.aggregate;

/**
 * Single-pass running statistics using Welford's online algorithm.
 *
 * <p>The obvious way to keep a running average is to accumulate a sum and divide by the count.
 * That works, but the sum grows without bound while each new price stays small, so the
 * floating-point exponents drift apart and precision is quietly lost -- exactly the situation
 * a long-lived stream consumer sits in. Welford's method instead nudges the mean by the
 * scaled residual of each new sample:
 *
 * <pre>
 *   mean_n = mean_(n-1) + (x_n - mean_(n-1)) / n
 * </pre>
 *
 * <p>Every term stays the same order of magnitude as the data, so accuracy holds up over
 * millions of messages. Carrying the companion {@code m2} term costs one extra multiply and
 * yields variance and standard deviation for free, which the dashboard uses to show how
 * spread out prices are.
 *
 * <p>This class is deliberately not thread safe; {@link PriceAggregator} owns the locking.
 */
public final class RunningStats {

    private long count;
    private double mean;
    /** Running sum of squared deviations from the current mean. */
    private double m2;
    private double min = Double.NaN;
    private double max = Double.NaN;

    /** Folds one sample into the statistics. */
    public void add(double value) {
        count++;
        double delta = value - mean;
        mean += delta / count;
        double deltaAfter = value - mean;
        m2 += delta * deltaAfter;

        if (count == 1) {
            min = value;
            max = value;
        } else {
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
        }
    }

    public long count() {
        return count;
    }

    /** The running average. Zero before any sample has been seen. */
    public double mean() {
        return count == 0 ? 0.0 : mean;
    }

    /** Sample variance (Bessel-corrected). Zero until at least two samples exist. */
    public double variance() {
        return count < 2 ? 0.0 : m2 / (count - 1);
    }

    public double stdDev() {
        return Math.sqrt(variance());
    }

    /** Smallest sample seen, or {@code NaN} if there have been none. */
    public double min() {
        return min;
    }

    /** Largest sample seen, or {@code NaN} if there have been none. */
    public double max() {
        return max;
    }
}
