package com.ashfaq.bigdata.producer;

import com.ashfaq.bigdata.producer.dto.OrderRequest;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Produces ordinary, valid orders for the bulk endpoint.
 *
 * <p>Identifiers start at 2001 so generated traffic can never collide with the fixed
 * acceptance-case orders 1001-1005, whose running average has to stay predictable.
 */
@Component
public class OrderGenerator {

    private static final long FIRST_GENERATED_ID = 2001L;
    private static final float MIN_PRICE = 10.0f;
    private static final float MAX_PRICE = 500.0f;
    private static final String[] PRODUCTS = {"Item1", "Item2", "Item3", "Item4", "Item5"};

    private final AtomicLong nextId = new AtomicLong(FIRST_GENERATED_ID);

    public List<OrderRequest> generate(int count) {
        List<OrderRequest> orders = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            orders.add(next());
        }
        return orders;
    }

    private OrderRequest next() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        String orderId = String.valueOf(nextId.getAndIncrement());
        String product = PRODUCTS[random.nextInt(PRODUCTS.length)];
        // Rounded to two decimals so the generated prices read cleanly on screen.
        float price = Math.round(random.nextFloat(MIN_PRICE, MAX_PRICE) * 100f) / 100f;
        return new OrderRequest(orderId, product, price);
    }
}
