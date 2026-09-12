package com.ashfaq.bigdata.producer;

import com.ashfaq.bigdata.avro.Order;
import com.ashfaq.bigdata.producer.dto.OrderRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The mapper is the boundary where JSON stops and Avro begins, so it is tested on its own.
 */
class OrderMapperTest {

    @Test
    @DisplayName("every field carries across from the request to the Avro record")
    void mapsAllFields() {
        Order order = OrderMapper.toAvro(new OrderRequest("1001", "Item1", 100.0f));

        assertThat(order.getOrderId()).isEqualTo("1001");
        assertThat(order.getProduct()).isEqualTo("Item1");
        assertThat(order.getPrice()).isEqualTo(100.0f);
    }

    @Test
    @DisplayName("the result is a real Avro record bound to the order.avsc schema")
    void producesAGenuineAvroRecord() {
        Order order = OrderMapper.toAvro(new OrderRequest("1001", "Item1", 100.0f));

        assertThat(order.getSchema()).isEqualTo(Order.getClassSchema());
        assertThat(order.getSchema().getFullName()).isEqualTo("com.ashfaq.bigdata.avro.Order");
    }

    @Test
    @DisplayName("a negative price is mapped faithfully, not clamped or rejected")
    void preservesNegativePrice() {
        // The consumer, not the producer, decides that a negative price is invalid.
        Order order = OrderMapper.toAvro(new OrderRequest("1004", "InvalidItem", -10.0f));

        assertThat(order.getPrice()).isEqualTo(-10.0f);
    }
}
