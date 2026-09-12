package com.ashfaq.bigdata.consumer.aggregate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Pins the two guarantees the assignment's aggregation rests on: the arithmetic is right, and an
 * order is counted once no matter how many times it is recorded.
 */
class RunningAverageTest {

    private RunningAverage runningAverage;

    @BeforeEach
    void setUp() {
        runningAverage = new RunningAverage();
    }

    @Test
    @DisplayName("the acceptance-case prices 100, 300, 500 produce averages 100, 200, 300")
    void computesTheAcceptanceCaseAverages() {
        assertThat(runningAverage.recordSuccess("1001", 100.0f))
                .hasValueSatisfying(s -> assertSnapshot(s, 1, 100.0, 100.0));

        assertThat(runningAverage.recordSuccess("1002", 300.0f))
                .hasValueSatisfying(s -> assertSnapshot(s, 2, 400.0, 200.0));

        assertThat(runningAverage.recordSuccess("1003", 500.0f))
                .hasValueSatisfying(s -> assertSnapshot(s, 3, 900.0, 300.0));
    }

    @Test
    @DisplayName("an order recorded twice is counted once - the retry guarantee")
    void countsARepeatedOrderOnce() {
        runningAverage.recordSuccess("1001", 100.0f);
        runningAverage.recordSuccess("1002", 300.0f);

        Optional<RunningAverage.Snapshot> second = runningAverage.recordSuccess("1001", 100.0f);

        assertThat(second).isEmpty();
        assertSnapshot(runningAverage.snapshot(), 2, 400.0, 200.0);
    }

    @Test
    @DisplayName("an order that is never recorded leaves the aggregate untouched")
    void failedOrdersDoNotAffectTheAggregate() {
        runningAverage.recordSuccess("1001", 100.0f);
        runningAverage.recordSuccess("1002", 300.0f);

        // A failed order simply never reaches recordSuccess. Nothing to call; the snapshot must
        // be exactly what the two successes produced.
        assertSnapshot(runningAverage.snapshot(), 2, 400.0, 200.0);
    }

    @Test
    @DisplayName("an empty aggregate reports zero, not NaN")
    void emptyAggregateIsZeroNotNaN() {
        assertSnapshot(runningAverage.snapshot(), 0, 0.0, 0.0);
    }

    @Test
    @DisplayName("the snapshot is a consistent triple - the average is total divided by count")
    void snapshotIsInternallyConsistent() {
        runningAverage.recordSuccess("a", 10.0f);
        runningAverage.recordSuccess("b", 20.0f);
        runningAverage.recordSuccess("c", 40.0f);

        RunningAverage.Snapshot snapshot = runningAverage.snapshot();

        assertThat(snapshot.average())
                .isCloseTo(snapshot.total() / snapshot.count(), within(1e-9));
    }

    @Test
    @DisplayName("many threads recording the same order at once still count it exactly once")
    void isIdempotentUnderConcurrency() throws Exception {
        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Optional<RunningAverage.Snapshot>>> results = new ArrayList<>();

        try {
            for (int i = 0; i < threads; i++) {
                results.add(pool.submit(() -> {
                    start.await();
                    return runningAverage.recordSuccess("1003", 500.0f);
                }));
            }
            start.countDown();

            long counted = 0;
            for (Future<Optional<RunningAverage.Snapshot>> result : results) {
                if (result.get(5, TimeUnit.SECONDS).isPresent()) {
                    counted++;
                }
            }

            // Exactly one thread wins; the rest see "already counted".
            assertThat(counted).isEqualTo(1);
            assertSnapshot(runningAverage.snapshot(), 1, 500.0, 500.0);
        } finally {
            pool.shutdownNow();
        }
    }

    private static void assertSnapshot(RunningAverage.Snapshot snapshot,
                                       long count, double total, double average) {
        assertThat(snapshot.count()).isEqualTo(count);
        assertThat(snapshot.total()).isCloseTo(total, within(1e-6));
        assertThat(snapshot.average()).isCloseTo(average, within(1e-6));
    }
}
