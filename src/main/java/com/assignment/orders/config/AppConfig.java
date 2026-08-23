package com.assignment.orders.config;

import java.util.Locale;

/**
 * Every tunable in the system, read from environment variables with sensible defaults.
 *
 * <p>Nothing here is a {@code static final} constant: values are read on each call so a
 * variable set just before launching a script takes effect, and so tests can exercise the
 * parsing helpers directly. The application classes never read the environment themselves --
 * they take their settings as constructor arguments, which is what makes them unit testable
 * without a broker.
 *
 * <p>A malformed value fails loudly at startup rather than silently falling back to the
 * default, because "I set the rate to 1.0 and nothing changed" is a miserable thing to debug
 * five minutes before a demo.
 */
public final class AppConfig {

    private AppConfig() {
    }

    /* ------------------------------------------------------------------ connection */

    public static String bootstrapServers() {
        return str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    }

    public static String schemaRegistryUrl() {
        return str("SCHEMA_REGISTRY_URL", "http://localhost:8081");
    }

    public static String ordersTopic() {
        return str("ORDERS_TOPIC", "orders");
    }

    public static String dlqTopic() {
        return str("DLQ_TOPIC", "orders.DLQ");
    }

    public static String consumerGroup() {
        return str("CONSUMER_GROUP", "order-processor");
    }

    /* -------------------------------------------------------------------- producer */

    /** Target send rate. The producer paces itself to roughly this many orders per second. */
    public static double produceRatePerSecond() {
        return positive("PRODUCE_RATE_PER_SEC", dbl("PRODUCE_RATE_PER_SEC", 8.0));
    }

    /** Number of messages to send, or 0 to keep producing until interrupted. */
    public static long totalMessages() {
        return lng("TOTAL_MESSAGES", 0L);
    }

    /** How many distinct products appear in the stream (Item1 .. ItemN). */
    public static int productCount() {
        return intg("PRODUCT_COUNT", 6);
    }

    public static double minPrice() {
        return dbl("MIN_PRICE", 5.0);
    }

    public static double maxPrice() {
        return dbl("MAX_PRICE", 500.0);
    }

    /**
     * Fraction of orders that are valid Avro but violate a business rule (negative price,
     * blank identifier). These exercise the <em>permanent</em> failure path.
     */
    public static double badRecordRate() {
        return rate("BAD_RECORD_RATE", 0.08);
    }

    /**
     * Fraction of messages published as raw non-Avro bytes, simulating a foreign producer
     * writing garbage onto our topic. These exercise the <em>poison pill</em> path.
     */
    public static double poisonRate() {
        return rate("POISON_RATE", 0.04);
    }

    /* -------------------------------------------------------------------- consumer */

    /**
     * Probability that processing an otherwise valid order fails with a simulated downstream
     * outage. These exercise the <em>transient</em> failure path and should mostly recover
     * within the retry budget.
     */
    public static double transientFailureRate() {
        return rate("TRANSIENT_FAILURE_RATE", 0.15);
    }

    /** Total attempts per record, including the first. 3 means one try plus two retries. */
    public static int maxAttempts() {
        return intg("MAX_ATTEMPTS", 3);
    }

    public static long initialBackoffMs() {
        return lng("INITIAL_BACKOFF_MS", 200L);
    }

    public static double backoffMultiplier() {
        return dbl("BACKOFF_MULTIPLIER", 2.0);
    }

    public static long maxBackoffMs() {
        return lng("MAX_BACKOFF_MS", 5_000L);
    }

    /** Proportional jitter, e.g. 0.2 spreads each backoff over +/-20%. */
    public static double jitterFactor() {
        return rate("JITTER_FACTOR", 0.2);
    }

    /** Width of the sliding window used for the "recent" average and throughput figures. */
    public static int windowSeconds() {
        return intg("WINDOW_SECONDS", 60);
    }

    /**
     * Whether the consumer paints the live ANSI dashboard. Set to false to fall back to plain
     * scrolling log lines -- useful in a terminal that does not render escape sequences.
     */
    public static boolean dashboardEnabled() {
        return bool("DASHBOARD_ENABLED", true);
    }

    /* --------------------------------------------------------------------- parsing */

    static String str(String name, String fallback) {
        String raw = System.getenv(name);
        return (raw == null || raw.isBlank()) ? fallback : raw.trim();
    }

    static int intg(String name, int fallback) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw badValue(name, raw, "a whole number");
        }
    }

    static long lng(String name, long fallback) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            throw badValue(name, raw, "a whole number");
        }
    }

    static double dbl(String name, double fallback) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Double.parseDouble(raw.trim());
        } catch (NumberFormatException e) {
            throw badValue(name, raw, "a number");
        }
    }

    static boolean bool(String name, boolean fallback) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        String v = raw.trim().toLowerCase(Locale.ROOT);
        return switch (v) {
            case "true", "yes", "y", "1", "on" -> true;
            case "false", "no", "n", "0", "off" -> false;
            default -> throw badValue(name, raw, "true or false");
        };
    }

    /** Reads a probability and insists it lands in [0, 1]. */
    static double rate(String name, double fallback) {
        double value = dbl(name, fallback);
        if (value < 0.0 || value > 1.0) {
            throw new IllegalStateException(
                    name + "=" + value + " is not a valid probability; expected a value between 0.0 and 1.0");
        }
        return value;
    }

    private static double positive(String name, double value) {
        if (value <= 0.0) {
            throw new IllegalStateException(name + "=" + value + " must be greater than zero");
        }
        return value;
    }

    private static IllegalStateException badValue(String name, String raw, String expected) {
        return new IllegalStateException(name + "=\"" + raw + "\" is not valid; expected " + expected);
    }
}
