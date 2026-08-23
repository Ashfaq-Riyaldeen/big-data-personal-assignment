package com.assignment.orders.aggregate;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

@DisplayName("RunningStats (Welford's online algorithm)")
class RunningStatsTest {

    @Test
    @DisplayName("reports empty statistics before any sample arrives")
    void emptyStats() {
        RunningStats stats = new RunningStats();

        assertThat(stats.count()).isZero();
        assertThat(stats.mean()).isZero();
        assertThat(stats.variance()).isZero();
        assertThat(stats.stdDev()).isZero();
        assertThat(stats.min()).isNaN();
        assertThat(stats.max()).isNaN();
    }

    @Test
    @DisplayName("a single sample is its own mean, min and max, with zero variance")
    void singleSample() {
        RunningStats stats = new RunningStats();
        stats.add(42.5);

        assertThat(stats.count()).isEqualTo(1);
        assertThat(stats.mean()).isEqualTo(42.5);
        assertThat(stats.min()).isEqualTo(42.5);
        assertThat(stats.max()).isEqualTo(42.5);
        // Sample variance is undefined for n=1; reporting 0 keeps the dashboard sane.
        assertThat(stats.variance()).isZero();
    }

    @Test
    @DisplayName("computes mean, sample variance, min and max for a known data set")
    void knownDataSet() {
        // Deviations from the mean of 5 are -3,-1,-1,-1,0,0,2,4 so the squares sum to 32,
        // giving a sample variance of 32/7.
        RunningStats stats = new RunningStats();
        for (double value : new double[]{2, 4, 4, 4, 5, 5, 7, 9}) {
            stats.add(value);
        }

        assertThat(stats.count()).isEqualTo(8);
        assertThat(stats.mean()).isEqualTo(5.0);
        assertThat(stats.variance()).isCloseTo(32.0 / 7.0, within(1e-12));
        assertThat(stats.stdDev()).isCloseTo(Math.sqrt(32.0 / 7.0), within(1e-12));
        assertThat(stats.min()).isEqualTo(2.0);
        assertThat(stats.max()).isEqualTo(9.0);
    }

    @Test
    @DisplayName("the incremental mean matches a batch computation over many samples")
    void matchesBatchComputation() {
        Random random = new Random(20260823L);
        double[] values = new double[10_000];
        for (int i = 0; i < values.length; i++) {
            values[i] = 5.0 + random.nextDouble() * 495.0;
        }

        RunningStats stats = new RunningStats();
        double naiveSum = 0.0;
        for (double value : values) {
            stats.add(value);
            naiveSum += value;
        }

        assertThat(stats.mean()).isCloseTo(naiveSum / values.length, within(1e-9));
        assertThat(stats.count()).isEqualTo(values.length);
    }

    @Test
    @DisplayName("stays accurate on large values where a naive running sum loses precision")
    void staysAccurateOnLargeMagnitudes() {
        // The motivation for Welford's method: with a huge offset, accumulating a raw sum
        // loses low-order bits, while tracking the mean directly does not.
        double offset = 1e9;
        RunningStats stats = new RunningStats();
        for (int i = 0; i < 1_000; i++) {
            stats.add(offset + (i % 2 == 0 ? 1.0 : -1.0));
        }

        assertThat(stats.count()).isEqualTo(1_000);
        assertThat(stats.mean()).isCloseTo(offset, within(1e-6));
        assertThat(stats.stdDev()).isCloseTo(1.0, within(1e-3));
    }

    @Test
    @DisplayName("tracks min and max independently of arrival order")
    void tracksExtremes() {
        RunningStats ascending = new RunningStats();
        RunningStats descending = new RunningStats();
        for (double value : new double[]{1, 2, 3, 4, 5}) {
            ascending.add(value);
        }
        for (double value : new double[]{5, 4, 3, 2, 1}) {
            descending.add(value);
        }

        assertThat(ascending.min()).isEqualTo(descending.min()).isEqualTo(1.0);
        assertThat(ascending.max()).isEqualTo(descending.max()).isEqualTo(5.0);
        assertThat(ascending.mean()).isEqualTo(descending.mean()).isEqualTo(3.0);
    }
}
