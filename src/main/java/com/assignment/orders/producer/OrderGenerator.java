package com.assignment.orders.producer;

import com.assignment.orders.avro.Order;

import java.nio.charset.StandardCharsets;
import java.util.random.RandomGenerator;

/**
 * Produces the randomised order stream, including the deliberately broken records that give
 * the retry and DLQ machinery something to react to.
 *
 * <p>A pipeline that only ever sees clean data proves nothing about its error handling, so
 * faults are injected at configurable rates:
 *
 * <ul>
 *   <li><b>Flawed orders</b> serialise perfectly well against {@code order.avsc} but violate a
 *       business rule. They model a buggy upstream service, and drive the permanent-failure path.</li>
 *   <li><b>Poison pills</b> are not valid Avro at all. They model a foreign producer writing to
 *       our topic with the wrong serialiser -- the case that breaks naively written consumers,
 *       because the failure happens during deserialisation, before any application code runs.</li>
 * </ul>
 *
 * <p>Order identifiers start at 1001, matching the examples in the assignment brief.
 */
public final class OrderGenerator {

    /** The first order identifier issued, as per the brief's example ("1001", "1002"). */
    public static final long FIRST_ORDER_ID = 1001L;

    private final int productCount;
    private final double minPrice;
    private final double maxPrice;
    private final double badRecordRate;
    private final double poisonRate;
    private final RandomGenerator random;

    private long nextOrderId = FIRST_ORDER_ID;
    private int flawCursor = 0;
    private int poisonCursor = 0;

    public OrderGenerator(int productCount,
                          double minPrice,
                          double maxPrice,
                          double badRecordRate,
                          double poisonRate,
                          RandomGenerator random) {

        if (productCount < 1) {
            throw new IllegalArgumentException("productCount must be at least 1, got " + productCount);
        }
        if (maxPrice < minPrice) {
            throw new IllegalArgumentException(
                    "maxPrice (" + maxPrice + ") must be >= minPrice (" + minPrice + ")");
        }
        this.productCount = productCount;
        this.minPrice = minPrice;
        this.maxPrice = maxPrice;
        this.badRecordRate = badRecordRate;
        this.poisonRate = poisonRate;
        this.random = random;
    }

    /** What kind of record was generated, for logging and for the demo narrative. */
    public enum Flaw {
        /** A well-formed, valid order. */
        NONE,
        NEGATIVE_PRICE,
        ZERO_PRICE,
        BLANK_ORDER_ID,
        BLANK_PRODUCT,
        /** Not valid Avro; carried as raw bytes instead of an {@link Order}. */
        POISON
    }

    /**
     * One generated message.
     *
     * @param order       the order to publish, or null when this is a poison pill
     * @param poisonBytes raw bytes to publish instead, or null for a normal order
     */
    public record Generated(String key, Order order, byte[] poisonBytes, Flaw flaw) {

        public boolean isPoison() {
            return poisonBytes != null;
        }

        public boolean isValid() {
            return flaw == Flaw.NONE;
        }
    }

    /** Generates the next message in the stream. */
    public Generated next() {
        String key = Long.toString(nextOrderId++);

        if (poisonRate > 0.0 && random.nextDouble() < poisonRate) {
            return new Generated(key, null, nextPoison(), Flaw.POISON);
        }

        if (badRecordRate > 0.0 && random.nextDouble() < badRecordRate) {
            Flaw flaw = nextFlaw();
            return new Generated(key, flawedOrder(key, flaw), null, flaw);
        }

        return new Generated(key, validOrder(key), null, Flaw.NONE);
    }

    private Order validOrder(String orderId) {
        return Order.newBuilder()
                .setOrderId(orderId)
                .setProduct(randomProduct())
                .setPrice(randomPrice())
                .build();
    }

    private Order flawedOrder(String orderId, Flaw flaw) {
        Order.Builder builder = Order.newBuilder()
                .setOrderId(orderId)
                .setProduct(randomProduct())
                .setPrice(randomPrice());

        switch (flaw) {
            // Negative and zero prices would corrupt the running average if they slipped through.
            case NEGATIVE_PRICE -> builder.setPrice(-randomPrice());
            case ZERO_PRICE -> builder.setPrice(0.0f);
            // Avro's string type permits an empty string, so these serialise cleanly and are
            // only caught by business validation on the consumer -- which is the point.
            case BLANK_ORDER_ID -> builder.setOrderId("");
            case BLANK_PRODUCT -> builder.setProduct("");
            default -> throw new IllegalArgumentException("not a record-level flaw: " + flaw);
        }
        return builder.build();
    }

    /** Cycles through the flaw types so a short demo run still shows all of them. */
    private Flaw nextFlaw() {
        Flaw[] flaws = {Flaw.NEGATIVE_PRICE, Flaw.ZERO_PRICE, Flaw.BLANK_ORDER_ID, Flaw.BLANK_PRODUCT};
        return flaws[flawCursor++ % flaws.length];
    }

    /**
     * Builds bytes that will fail deserialisation, alternating between two realistic corruptions.
     */
    private byte[] nextPoison() {
        if (poisonCursor++ % 2 == 0) {
            // Plain JSON: a producer that forgot to use the Avro serialiser at all. The first
            // byte is '{' rather than Confluent's 0x00 magic byte, so the deserialiser rejects
            // it immediately with "Unknown magic byte!".
            String json = "{\"orderId\":\"" + nextOrderId + "\",\"product\":\"Item1\",\"price\":42.0}";
            return json.getBytes(StandardCharsets.UTF_8);
        }

        // Subtler: a correct magic byte followed by a schema id that was never registered.
        // This one gets past the framing check and fails at schema lookup instead.
        byte[] payload = new byte[12];
        random.nextBytes(payload);

        byte[] bytes = new byte[5 + payload.length];
        bytes[0] = 0x00;                    // Confluent magic byte
        bytes[1] = 0x00;                    // schema id 0x00FFFFFE -- not in the registry
        bytes[2] = (byte) 0xFF;
        bytes[3] = (byte) 0xFF;
        bytes[4] = (byte) 0xFE;
        System.arraycopy(payload, 0, bytes, 5, payload.length);
        return bytes;
    }

    private String randomProduct() {
        return "Item" + (1 + random.nextInt(productCount));
    }

    /** A price in [minPrice, maxPrice], rounded to two decimals like real money. */
    private float randomPrice() {
        double raw = minPrice + random.nextDouble() * (maxPrice - minPrice);
        return (float) (Math.round(raw * 100.0) / 100.0);
    }
}
