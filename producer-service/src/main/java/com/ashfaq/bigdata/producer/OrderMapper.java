package com.ashfaq.bigdata.producer;

import com.ashfaq.bigdata.avro.Order;
import com.ashfaq.bigdata.producer.dto.OrderRequest;

/**
 * Converts the REST request into the generated Avro model.
 *
 * <p>Kept as its own class rather than inlined, because this is the boundary where JSON stops
 * and Avro begins - the single place to look when asking what actually goes onto the topic.
 */
public final class OrderMapper {

    private OrderMapper() {
    }

    public static Order toAvro(OrderRequest request) {
        return Order.newBuilder()
                .setOrderId(request.orderId())
                .setProduct(request.product())
                .setPrice(request.price())
                .build();
    }
}
