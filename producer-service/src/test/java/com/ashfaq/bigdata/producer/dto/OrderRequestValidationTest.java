package com.ashfaq.bigdata.producer.dto;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Bean validation on the REST request, run against a plain validator with no Spring context.
 *
 * <p>The most important test here is the one that asserts a negative price is <b>accepted</b>.
 * That is deliberate: the producer must be able to publish an invalid price so the consumer can
 * reject it and route it to the dead letter queue. Anyone adding {@code @Positive} to the DTO
 * breaks the DLQ demonstration, and this test is what tells them.
 */
class OrderRequestValidationTest {

    private static ValidatorFactory factory;
    private static Validator validator;

    @BeforeAll
    static void setUpValidator() {
        factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @AfterAll
    static void tearDownValidator() {
        factory.close();
    }

    @Test
    @DisplayName("a well-formed request has no violations")
    void validRequestPasses() {
        assertThat(validate(new OrderRequest("1001", "Item1", 100.0f))).isEmpty();
    }

    @Test
    @DisplayName("a negative price is accepted - business validation belongs in the consumer")
    void negativePriceIsAccepted() {
        assertThat(validate(new OrderRequest("1004", "InvalidItem", -10.0f))).isEmpty();
    }

    @Test
    @DisplayName("a zero price is accepted for the same reason")
    void zeroPriceIsAccepted() {
        assertThat(validate(new OrderRequest("1004", "InvalidItem", 0.0f))).isEmpty();
    }

    @Test
    @DisplayName("a blank orderId is rejected on that field")
    void blankOrderIdIsRejected() {
        assertSingleViolationOn(validate(new OrderRequest("   ", "Item1", 100.0f)), "orderId");
    }

    @Test
    @DisplayName("a blank product is rejected on that field")
    void blankProductIsRejected() {
        assertSingleViolationOn(validate(new OrderRequest("1001", "", 100.0f)), "product");
    }

    @Test
    @DisplayName("a missing price is rejected on that field")
    void nullPriceIsRejected() {
        assertSingleViolationOn(validate(new OrderRequest("1001", "Item1", null)), "price");
    }

    @Test
    @DisplayName("several problems are all reported at once")
    void reportsEveryViolation() {
        Set<ConstraintViolation<OrderRequest>> violations =
                validate(new OrderRequest("", "", null));

        assertThat(violations)
                .extracting(v -> v.getPropertyPath().toString())
                .containsExactlyInAnyOrder("orderId", "product", "price");
    }

    private static Set<ConstraintViolation<OrderRequest>> validate(OrderRequest request) {
        return validator.validate(request);
    }

    private static void assertSingleViolationOn(Set<ConstraintViolation<OrderRequest>> violations,
                                                String field) {
        assertThat(violations).hasSize(1);
        assertThat(violations.iterator().next().getPropertyPath().toString()).isEqualTo(field);
    }
}
