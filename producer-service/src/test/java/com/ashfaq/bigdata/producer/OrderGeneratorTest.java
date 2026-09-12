package com.ashfaq.bigdata.producer;

import com.ashfaq.bigdata.producer.dto.OrderRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Generated orders feed the bulk endpoint. They must never collide with the fixed acceptance
 * cases 1001-1005, whose running average has to stay predictable.
 */
class OrderGeneratorTest {

    private static final Set<String> KNOWN_PRODUCTS =
            Set.of("Item1", "Item2", "Item3", "Item4", "Item5");

    @Test
    @DisplayName("identifiers start at 2001 and increase by one")
    void identifiersStartAt2001() {
        List<OrderRequest> orders = new OrderGenerator().generate(3);

        assertThat(orders).extracting(OrderRequest::orderId)
                .containsExactly("2001", "2002", "2003");
    }

    @Test
    @DisplayName("identifiers stay unique across successive batches")
    void identifiersAreUniqueAcrossBatches() {
        OrderGenerator generator = new OrderGenerator();

        List<OrderRequest> first = generator.generate(5);
        List<OrderRequest> second = generator.generate(5);

        assertThat(first).extracting(OrderRequest::orderId)
                .doesNotContainAnyElementsOf(
                        second.stream().map(OrderRequest::orderId).toList());
        assertThat(second.get(0).orderId()).isEqualTo("2006");
    }

    @Test
    @DisplayName("generated orders never reuse an acceptance-case identifier")
    void neverCollidesWithAcceptanceCases() {
        List<OrderRequest> orders = new OrderGenerator().generate(50);

        assertThat(orders).extracting(OrderRequest::orderId)
                .doesNotContain("1001", "1002", "1003", "1004", "1005");
    }

    @Test
    @DisplayName("every generated price is positive, within range, and has at most two decimals")
    void pricesAreWellFormed() {
        List<OrderRequest> orders = new OrderGenerator().generate(200);

        assertThat(orders).allSatisfy(order -> {
            assertThat(order.price()).isBetween(10.0f, 500.0f);
            // The generator rounds to two decimals in float arithmetic. Checking the decimal
            // scale has to go through Float.toString, which gives the shortest representation
            // that round-trips; widening the float to double first would expose binary noise
            // such as 271.480010986328125 and fail for the wrong reason.
            assertThat(new BigDecimal(Float.toString(order.price())).scale())
                    .isLessThanOrEqualTo(2);
        });
    }

    @Test
    @DisplayName("every generated product comes from the fixed list, so none triggers a failure rule")
    void productsComeFromTheFixedList() {
        List<OrderRequest> orders = new OrderGenerator().generate(200);

        assertThat(orders).extracting(OrderRequest::product)
                .allMatch(KNOWN_PRODUCTS::contains);
    }

    @Test
    @DisplayName("asking for zero orders returns an empty list")
    void zeroCountIsEmpty() {
        assertThat(new OrderGenerator().generate(0)).isEmpty();
    }
}
