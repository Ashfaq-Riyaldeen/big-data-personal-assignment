package com.ashfaq.bigdata.producer;

import com.ashfaq.bigdata.producer.dto.OrderRequest;
import com.ashfaq.bigdata.producer.dto.PublishResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The HTTP contract of the producer, with Kafka replaced by a mock.
 *
 * <p>Covers what Swagger UI relies on during the demonstration: the right status codes, the
 * broker acknowledgement echoed back, readable validation errors, and each demo endpoint
 * publishing exactly the documented order.
 */
@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private OrderPublisher publisher;

    @MockitoBean
    private OrderGenerator generator;

    @BeforeEach
    void stubPublisher() {
        // Echo whatever was asked for, with fixed broker metadata, so the response can be checked.
        when(publisher.publish(any())).thenAnswer(invocation -> {
            OrderRequest request = invocation.getArgument(0);
            return new PublishResult(request.orderId(), request.product(), request.price(),
                    "orders.v1", 0, 42L);
        });
    }

    @Test
    @DisplayName("POST /api/orders publishes the order and returns the broker acknowledgement")
    void publishesAValidOrder() throws Exception {
        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"orderId\":\"1001\",\"product\":\"Item1\",\"price\":100.0}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value("1001"))
                .andExpect(jsonPath("$.product").value("Item1"))
                .andExpect(jsonPath("$.price").value(100.0))
                .andExpect(jsonPath("$.topic").value("orders.v1"))
                .andExpect(jsonPath("$.partition").value(0))
                .andExpect(jsonPath("$.offset").value(42));

        assertThat(captured().get(0)).isEqualTo(new OrderRequest("1001", "Item1", 100.0f));
    }

    @Test
    @DisplayName("a blank product is rejected with a 400 naming the field")
    void rejectsInvalidOrderWithReadableError() throws Exception {
        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"orderId\":\"1001\",\"product\":\"\",\"price\":100.0}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.fieldErrors.product").exists());

        verify(publisher, never()).publish(any());
    }

    @Test
    @DisplayName("a negative price is accepted with 200 - the DLQ demonstration depends on it")
    void acceptsNegativePrice() throws Exception {
        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"orderId\":\"1004\",\"product\":\"InvalidItem\",\"price\":-10.0}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.price").value(-10.0));
    }

    @Test
    @DisplayName("POST /api/orders/generate publishes the requested number of orders")
    void generatesOrders() throws Exception {
        when(generator.generate(anyInt())).thenReturn(List.of(
                new OrderRequest("2001", "Item1", 12.5f),
                new OrderRequest("2002", "Item2", 99.0f),
                new OrderRequest("2003", "Item3", 250.0f)));

        mockMvc.perform(post("/api/orders/generate").param("count", "3"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(3))
                .andExpect(jsonPath("$[0].orderId").value("2001"))
                .andExpect(jsonPath("$[2].orderId").value("2003"));

        verify(generator).generate(3);
        assertThat(captured()).hasSize(3);
    }

    @Test
    @DisplayName("POST /api/orders/demo/temporary publishes 1003 / TEMP_FAIL / 500.00")
    void demoTemporaryPublishesTheDocumentedOrder() throws Exception {
        mockMvc.perform(post("/api/orders/demo/temporary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value("1003"))
                .andExpect(jsonPath("$.product").value("TEMP_FAIL"))
                .andExpect(jsonPath("$.price").value(500.0));
    }

    @Test
    @DisplayName("POST /api/orders/demo/permanent publishes 1004 / InvalidItem / -10.00")
    void demoPermanentPublishesTheDocumentedOrder() throws Exception {
        mockMvc.perform(post("/api/orders/demo/permanent"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value("1004"))
                .andExpect(jsonPath("$.product").value("InvalidItem"))
                .andExpect(jsonPath("$.price").value(-10.0));
    }

    @Test
    @DisplayName("POST /api/orders/demo/always-fail publishes 1005 / ALWAYS_FAIL / 700.00")
    void demoAlwaysFailPublishesTheDocumentedOrder() throws Exception {
        mockMvc.perform(post("/api/orders/demo/always-fail"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value("1005"))
                .andExpect(jsonPath("$.product").value("ALWAYS_FAIL"))
                .andExpect(jsonPath("$.price").value(700.0));
    }

    @Test
    @DisplayName("the demo endpoints honour an orderId override so the demo can be re-run")
    void demoEndpointsHonourOrderIdOverride() throws Exception {
        mockMvc.perform(post("/api/orders/demo/temporary").param("orderId", "9003"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value("9003"))
                .andExpect(jsonPath("$.product").value("TEMP_FAIL"));
    }

    private List<OrderRequest> captured() {
        ArgumentCaptor<OrderRequest> captor = ArgumentCaptor.forClass(OrderRequest.class);
        verify(publisher, org.mockito.Mockito.atLeastOnce()).publish(captor.capture());
        return captor.getAllValues();
    }
}
