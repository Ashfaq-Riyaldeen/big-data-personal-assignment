package com.assignment.orders.producer;

import com.assignment.orders.support.SequenceRandom;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("OrderGenerator")
class OrderGeneratorTest {

    private static OrderGenerator generator(double badRate, double poisonRate) {
        // A draw of 0.99 sits above any rate below it, so faults fire only when the rate is 1.0.
        return new OrderGenerator(6, 5.0, 500.0, badRate, poisonRate, SequenceRandom.always(0.99));
    }

    @Test
    @DisplayName("issues sequential identifiers starting at 1001, as in the brief")
    void sequentialOrderIds() {
        OrderGenerator generator = generator(0.0, 0.0);

        assertThat(generator.next().key()).isEqualTo("1001");
        assertThat(generator.next().key()).isEqualTo("1002");
        assertThat(generator.next().key()).isEqualTo("1003");
    }

    @Test
    @DisplayName("produces clean orders when no faults are configured")
    void producesCleanOrders() {
        OrderGenerator generator = generator(0.0, 0.0);

        for (int i = 0; i < 50; i++) {
            OrderGenerator.Generated generated = generator.next();

            assertThat(generated.isValid()).isTrue();
            assertThat(generated.isPoison()).isFalse();
            assertThat(generated.flaw()).isEqualTo(OrderGenerator.Flaw.NONE);
            assertThat(generated.order().getOrderId()).isEqualTo(generated.key());
            assertThat(generated.order().getProduct()).startsWith("Item");
            assertThat(generated.order().getPrice()).isBetween(5.0f, 500.0f);
        }
    }

    @Test
    @DisplayName("keeps prices and product names inside the configured bounds")
    void respectsBounds() {
        OrderGenerator generator = new OrderGenerator(
                3, 10.0, 20.0, 0.0, 0.0, ThreadLocalRandom.current());

        List<String> products = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            OrderGenerator.Generated generated = generator.next();
            assertThat(generated.order().getPrice()).isBetween(10.0f, 20.0f);
            products.add(generated.order().getProduct());
        }

        assertThat(products).isSubsetOf("Item1", "Item2", "Item3");
    }

    @Test
    @DisplayName("cycles through every flaw so a short run exercises all of them")
    void cyclesThroughFlaws() {
        OrderGenerator generator = generator(1.0, 0.0);

        // Rotating rather than picking at random guarantees the demo shows each failure mode,
        // even if it only runs for a few seconds.
        assertThat(generator.next().flaw()).isEqualTo(OrderGenerator.Flaw.NEGATIVE_PRICE);
        assertThat(generator.next().flaw()).isEqualTo(OrderGenerator.Flaw.ZERO_PRICE);
        assertThat(generator.next().flaw()).isEqualTo(OrderGenerator.Flaw.BLANK_ORDER_ID);
        assertThat(generator.next().flaw()).isEqualTo(OrderGenerator.Flaw.BLANK_PRODUCT);
        assertThat(generator.next().flaw()).isEqualTo(OrderGenerator.Flaw.NEGATIVE_PRICE);
    }

    @Test
    @DisplayName("each flaw breaks exactly the field it names")
    void flawsBreakTheRightField() {
        OrderGenerator generator = generator(1.0, 0.0);

        assertThat(generator.next().order().getPrice()).isNegative();
        assertThat(generator.next().order().getPrice()).isZero();
        assertThat(generator.next().order().getOrderId()).isEmpty();
        assertThat(generator.next().order().getProduct()).isEmpty();
    }

    @Test
    @DisplayName("flawed orders still serialise, so they fail at validation rather than decoding")
    void flawedOrdersRemainSchemaValid() {
        OrderGenerator generator = generator(1.0, 0.0);

        for (int i = 0; i < 4; i++) {
            OrderGenerator.Generated generated = generator.next();
            // The point of this fault class: valid Avro carrying invalid business data.
            assertThat(generated.isPoison()).isFalse();
            assertThat(generated.order()).isNotNull();
            assertThat(generated.isValid()).isFalse();
        }
    }

    @Test
    @DisplayName("poison pills carry raw bytes and no order")
    void poisonCarriesRawBytes() {
        OrderGenerator generator = generator(0.0, 1.0);
        OrderGenerator.Generated generated = generator.next();

        assertThat(generated.isPoison()).isTrue();
        assertThat(generated.order()).isNull();
        assertThat(generated.poisonBytes()).isNotEmpty();
        assertThat(generated.flaw()).isEqualTo(OrderGenerator.Flaw.POISON);
    }

    @Test
    @DisplayName("alternates between the two poison flavours")
    void alternatesPoisonFlavours() {
        OrderGenerator generator = generator(0.0, 1.0);

        // Flavour one: plain JSON from a producer that never used Avro. It fails the very first
        // framing check, because Confluent's wire format expects a 0x00 magic byte.
        byte[] json = generator.next().poisonBytes();
        assertThat(new String(json, StandardCharsets.UTF_8)).startsWith("{").contains("orderId");

        // Flavour two: correct framing but a schema id that was never registered, so it gets
        // past the magic byte and fails at schema lookup instead.
        byte[] framed = generator.next().poisonBytes();
        assertThat(framed[0]).isEqualTo((byte) 0x00);
        assertThat(framed).hasSize(17);
    }

    @Test
    @DisplayName("rejects a nonsensical configuration")
    void rejectsInvalidConfiguration() {
        assertThatThrownBy(() -> new OrderGenerator(0, 5.0, 500.0, 0.0, 0.0, SequenceRandom.always(0.5)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("productCount");

        assertThatThrownBy(() -> new OrderGenerator(6, 500.0, 5.0, 0.0, 0.0, SequenceRandom.always(0.5)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxPrice");
    }
}
