package com.assignment.orders.aggregate;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

@DisplayName("PriceAggregator")
class PriceAggregatorTest {

    private static final long T0 = 1_700_000_000_000L;

    @Test
    @DisplayName("rejects a non-positive window")
    void rejectsInvalidWindow() {
        assertThatThrownBy(() -> new PriceAggregator(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("windowSeconds");
    }

    @Test
    @DisplayName("starts empty")
    void startsEmpty() {
        PriceAggregator aggregator = new PriceAggregator(60);
        PriceAggregator.Snapshot snapshot = aggregator.snapshot(T0);

        assertThat(snapshot.count()).isZero();
        assertThat(snapshot.mean()).isZero();
        assertThat(snapshot.perProduct()).isEmpty();
        assertThat(snapshot.windowCount()).isZero();
        assertThat(snapshot.windowMean()).isZero();
    }

    @Test
    @DisplayName("computes the lifetime running average across all products")
    void lifetimeAverage() {
        PriceAggregator aggregator = new PriceAggregator(60);
        aggregator.record("Item1", 100.0, T0);
        aggregator.record("Item2", 200.0, T0);
        aggregator.record("Item1", 300.0, T0);

        PriceAggregator.Snapshot snapshot = aggregator.snapshot(T0);

        assertThat(snapshot.count()).isEqualTo(3);
        assertThat(snapshot.mean()).isEqualTo(200.0);
        assertThat(snapshot.min()).isEqualTo(100.0);
        assertThat(snapshot.max()).isEqualTo(300.0);
    }

    @Test
    @DisplayName("keeps each product's statistics separate")
    void perProductIsolation() {
        PriceAggregator aggregator = new PriceAggregator(60);
        aggregator.record("Item1", 10.0, T0);
        aggregator.record("Item1", 20.0, T0);
        aggregator.record("Item2", 500.0, T0);

        PriceAggregator.Snapshot snapshot = aggregator.snapshot(T0);

        assertThat(snapshot.perProduct()).hasSize(2);

        PriceAggregator.ProductStat item1 = findProduct(snapshot, "Item1");
        assertThat(item1.count()).isEqualTo(2);
        assertThat(item1.mean()).isEqualTo(15.0);

        PriceAggregator.ProductStat item2 = findProduct(snapshot, "Item2");
        assertThat(item2.count()).isEqualTo(1);
        assertThat(item2.mean()).isEqualTo(500.0);
    }

    @Test
    @DisplayName("products come back in a stable alphabetical order")
    void productsAreSorted() {
        PriceAggregator aggregator = new PriceAggregator(60);
        aggregator.record("Item3", 10.0, T0);
        aggregator.record("Item1", 10.0, T0);
        aggregator.record("Item2", 10.0, T0);

        assertThat(aggregator.snapshot(T0).perProduct())
                .extracting(PriceAggregator.ProductStat::product)
                .containsExactly("Item1", "Item2", "Item3");
    }

    @Test
    @DisplayName("drops samples once they fall out of the sliding window")
    void windowEviction() {
        PriceAggregator aggregator = new PriceAggregator(10);

        aggregator.record("Item1", 100.0, T0);
        aggregator.record("Item1", 200.0, T0 + 5_000);

        // Both samples are still inside the 10 second window.
        PriceAggregator.Snapshot inside = aggregator.snapshot(T0 + 9_000);
        assertThat(inside.windowCount()).isEqualTo(2);
        assertThat(inside.windowMean()).isEqualTo(150.0);

        // Advancing past the first sample's expiry leaves only the second.
        PriceAggregator.Snapshot partial = aggregator.snapshot(T0 + 11_000);
        assertThat(partial.windowCount()).isEqualTo(1);
        assertThat(partial.windowMean()).isEqualTo(200.0);

        // Eventually the window empties, while lifetime figures are untouched.
        PriceAggregator.Snapshot empty = aggregator.snapshot(T0 + 60_000);
        assertThat(empty.windowCount()).isZero();
        assertThat(empty.windowMean()).isZero();
        assertThat(empty.count()).isEqualTo(2);
        assertThat(empty.mean()).isEqualTo(150.0);
    }

    @Test
    @DisplayName("the windowed average reacts to a shift that barely moves the lifetime average")
    void windowReactsFasterThanLifetime() {
        PriceAggregator aggregator = new PriceAggregator(10);

        // A long run of cheap orders establishes a low lifetime average.
        for (int i = 0; i < 1_000; i++) {
            aggregator.record("Item1", 10.0, T0);
        }
        // Then prices jump, well after the earlier samples have left the window.
        for (int i = 0; i < 10; i++) {
            aggregator.record("Item1", 500.0, T0 + 60_000);
        }

        PriceAggregator.Snapshot snapshot = aggregator.snapshot(T0 + 60_000);

        // The lifetime average is dominated by history and hardly moves...
        assertThat(snapshot.mean()).isCloseTo(14.85, within(0.01));
        // ...while the window shows the new reality immediately. This is why both are tracked.
        assertThat(snapshot.windowMean()).isEqualTo(500.0);
    }

    @Test
    @DisplayName("derives throughput from the window contents")
    void throughput() {
        PriceAggregator aggregator = new PriceAggregator(10);
        for (int i = 0; i < 50; i++) {
            aggregator.record("Item1", 10.0, T0);
        }

        // 50 samples across a 10 second window is 5 per second.
        assertThat(aggregator.snapshot(T0).throughputPerSecond()).isEqualTo(5.0);
    }

    private static PriceAggregator.ProductStat findProduct(PriceAggregator.Snapshot snapshot, String product) {
        return snapshot.perProduct().stream()
                .filter(stat -> stat.product().equals(product))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no statistics for " + product));
    }
}
