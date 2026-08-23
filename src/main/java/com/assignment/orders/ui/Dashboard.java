package com.assignment.orders.ui;

import com.assignment.orders.aggregate.PriceAggregator;
import com.assignment.orders.aggregate.ProcessingMetrics;
import com.assignment.orders.dlq.DlqReason;

import java.io.PrintStream;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A live terminal panel showing the running average and pipeline health, repainted a few times
 * a second on its own daemon thread.
 *
 * <p>Scrolling log lines make a stream demo hard to follow: the numbers that matter fly past
 * and the audience is left reading whatever happened to land at the bottom. A fixed panel keeps
 * the running average -- the figure the assignment is actually about -- in the same place the
 * whole time, so a change in it is obvious.
 *
 * <p>Each frame is assembled into a single string and written in one call. Painting piecemeal
 * would let the terminal display a half-drawn frame and flicker.
 */
public final class Dashboard implements AutoCloseable {

    private static final int WIDTH = 78;
    private static final int BAR_WIDTH = 22;
    private static final int VISIBLE_EVENTS = 7;

    // ANSI escapes. Windows Terminal and PowerShell on Windows 11 render these natively.
    /** ASCII escape (0x1B), built from its code point so no control character sits in the source. */
    private static final String ESC = Character.toString(27);
    private static final String CLEAR_SCREEN = ESC + "[2J" + ESC + "[H";
    private static final String HIDE_CURSOR = ESC + "[?25l";
    private static final String SHOW_CURSOR = ESC + "[?25h";
    private static final String RESET = ESC + "[0m";
    private static final String BOLD = ESC + "[1m";
    private static final String DIM = ESC + "[2m";
    private static final String CYAN = ESC + "[36m";
    private static final String GREEN = ESC + "[32m";
    private static final String YELLOW = ESC + "[33m";
    private static final String RED = ESC + "[31m";
    private static final String WHITE = ESC + "[97m";

    private static final DateTimeFormatter CLOCK =
            DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    private final PriceAggregator aggregator;
    private final ProcessingMetrics metrics;
    private final EventLog events;
    private final PrintStream out;

    private final String bootstrapServers;
    private final String topic;
    private final String dlqTopic;
    private final String consumerGroup;

    private final long startedAtMillis = System.currentTimeMillis();
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "dashboard");
                thread.setDaemon(true);
                return thread;
            });

    public Dashboard(PriceAggregator aggregator,
                     ProcessingMetrics metrics,
                     EventLog events,
                     PrintStream out,
                     String bootstrapServers,
                     String topic,
                     String dlqTopic,
                     String consumerGroup) {
        this.aggregator = aggregator;
        this.metrics = metrics;
        this.events = events;
        this.out = out;
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.dlqTopic = dlqTopic;
        this.consumerGroup = consumerGroup;
    }

    /** Begins repainting at roughly 4 frames per second. */
    public void start() {
        out.print(HIDE_CURSOR);
        scheduler.scheduleAtFixedRate(this::renderQuietly, 0, 250, TimeUnit.MILLISECONDS);
    }

    private void renderQuietly() {
        try {
            render();
        } catch (RuntimeException e) {
            // A rendering fault must never take down the consumer; the pipeline matters, the
            // pretty panel does not.
            out.print(RESET);
        }
    }

    /** Paints one frame. */
    public void render() {
        long now = System.currentTimeMillis();
        PriceAggregator.Snapshot prices = aggregator.snapshot(now);
        ProcessingMetrics.Snapshot health = metrics.snapshot();

        StringBuilder frame = new StringBuilder(4096);
        frame.append(CLEAR_SCREEN);

        appendHeader(frame, now);
        appendRunningAverage(frame, prices);
        appendPerProduct(frame, prices);
        appendReliability(frame, health);
        appendEvents(frame);

        frame.append(DIM).append("  Ctrl+C to stop").append(RESET).append('\n');

        out.print(frame);
        out.flush();
    }

    private void appendHeader(StringBuilder frame, long now) {
        String title = "KAFKA AVRO ORDER PIPELINE";
        String uptime = "up " + formatDuration(Duration.ofMillis(now - startedAtMillis));

        frame.append(CYAN).append("  ").append("=".repeat(WIDTH)).append(RESET).append('\n');
        frame.append("  ").append(BOLD).append(WHITE).append(title).append(RESET);
        frame.append(" ".repeat(Math.max(1, WIDTH - title.length() - uptime.length())));
        frame.append(DIM).append(uptime).append(RESET).append('\n');
        frame.append(CYAN).append("  ").append("=".repeat(WIDTH)).append(RESET).append('\n');
        frame.append(DIM)
                .append("  ").append(bootstrapServers)
                .append("  |  topic ").append(topic)
                .append("  |  group ").append(consumerGroup)
                .append("  |  dlq ").append(dlqTopic)
                .append(RESET).append("\n\n");
    }

    private void appendRunningAverage(StringBuilder frame, PriceAggregator.Snapshot prices) {
        frame.append("  ").append(BOLD).append("RUNNING AVERAGE PRICE").append(RESET).append('\n');
        frame.append("  ").append("-".repeat(WIDTH)).append('\n');

        frame.append("   ")
                .append(BOLD).append(GREEN)
                .append(String.format(Locale.US, "%,12.2f", prices.mean()))
                .append(RESET)
                .append(DIM).append("   lifetime").append(RESET);

        frame.append(String.format(Locale.US, "        last %.0fs %s%,10.2f%s",
                prices.windowSeconds(), CYAN, prices.windowMean(), RESET));

        frame.append(String.format(Locale.US, "   %s%.1f/s%s%n",
                DIM, prices.throughputPerSecond(), RESET));

        frame.append(String.format(Locale.US,
                "   %saggregated%s %,-10d %smin%s %,9.2f   %smax%s %,9.2f   %sstd dev%s %,8.2f%n",
                DIM, RESET, prices.count(),
                DIM, RESET, safe(prices.min()),
                DIM, RESET, safe(prices.max()),
                DIM, RESET, prices.stdDev()));
        frame.append('\n');
    }

    private void appendPerProduct(StringBuilder frame, PriceAggregator.Snapshot prices) {
        frame.append("  ").append(BOLD).append("AVERAGE BY PRODUCT").append(RESET).append('\n');
        frame.append("  ").append("-".repeat(WIDTH)).append('\n');

        List<PriceAggregator.ProductStat> products = prices.perProduct();
        if (products.isEmpty()) {
            frame.append(DIM).append("   waiting for orders...").append(RESET).append("\n\n");
            return;
        }

        // Scale the bars against the dearest product so the comparison fills the width.
        double maxMean = products.stream()
                .mapToDouble(PriceAggregator.ProductStat::mean)
                .max()
                .orElse(1.0);

        for (PriceAggregator.ProductStat product : products) {
            int filled = maxMean <= 0 ? 0 : (int) Math.round(BAR_WIDTH * product.mean() / maxMean);
            filled = Math.max(0, Math.min(BAR_WIDTH, filled));

            frame.append(String.format(Locale.US, "   %-8s %s%s%s%s%s %,9.2f  %sn=%-6d%s%n",
                    product.product(),
                    CYAN, "#".repeat(filled), RESET,
                    DIM, ".".repeat(BAR_WIDTH - filled),
                    product.mean(),
                    DIM, product.count(), RESET));
        }
        frame.append('\n');
    }

    private void appendReliability(StringBuilder frame, ProcessingMetrics.Snapshot health) {
        frame.append("  ").append(BOLD).append("RELIABILITY").append(RESET).append('\n');
        frame.append("  ").append("-".repeat(WIDTH)).append('\n');

        frame.append(String.format(Locale.US,
                "   %sconsumed%s %,-8d %sprocessed%s %s%,-8d%s %sretried%s %s%,-6d%s %srecovered%s %s%,d%s (%.0f%%)%n",
                DIM, RESET, health.consumed(),
                DIM, RESET, GREEN, health.processedOk(), RESET,
                DIM, RESET, YELLOW, health.retryAttempts(), RESET,
                DIM, RESET, GREEN, health.recoveredAfterRetry(), RESET,
                health.retrySuccessRate() * 100));

        frame.append(String.format(Locale.US,
                "   %sdead-lettered%s %s%,-6d%s  %svalidation%s %,-5d %spoison%s %,-5d %sexhausted%s %,-5d %sother%s %,d%n",
                DIM, RESET, health.deadLettered() > 0 ? RED : GREEN, health.deadLettered(), RESET,
                DIM, RESET, health.countFor(DlqReason.VALIDATION_FAILED),
                DIM, RESET, health.countFor(DlqReason.DESERIALIZATION_FAILED),
                DIM, RESET, health.countFor(DlqReason.RETRIES_EXHAUSTED),
                DIM, RESET, health.countFor(DlqReason.UNEXPECTED_ERROR)));
        frame.append('\n');
    }

    private void appendEvents(StringBuilder frame) {
        frame.append("  ").append(BOLD).append("RECENT EVENTS").append(RESET).append('\n');
        frame.append("  ").append("-".repeat(WIDTH)).append('\n');

        List<EventLog.Event> recent = events.recent(VISIBLE_EVENTS);
        if (recent.isEmpty()) {
            frame.append(DIM).append("   nothing to report").append(RESET).append('\n');
        }
        for (EventLog.Event event : recent) {
            frame.append(String.format("   %s%s%s  %s%-9s%s %s%n",
                    DIM, CLOCK.format(Instant.ofEpochMilli(event.timestampMillis())), RESET,
                    colourFor(event.level()), event.level(), RESET,
                    truncate(event.message(), WIDTH - 22)));
        }
        // Keep the panel a constant height so the footer does not jump around as events arrive.
        for (int i = recent.size(); i < VISIBLE_EVENTS; i++) {
            frame.append('\n');
        }
        frame.append('\n');
    }

    private static String colourFor(EventLog.Level level) {
        return switch (level) {
            case RETRY -> YELLOW;
            case DLQ -> RED;
            case RECOVERED -> GREEN;
            case INFO -> DIM;
        };
    }

    /** {@link PriceAggregator} reports NaN for min/max until the first sample arrives. */
    private static double safe(double value) {
        return Double.isNaN(value) ? 0.0 : value;
    }

    private static String truncate(String text, int max) {
        if (text == null) {
            return "";
        }
        return text.length() <= max ? text : text.substring(0, Math.max(0, max - 3)) + "...";
    }

    private static String formatDuration(Duration duration) {
        long seconds = Math.max(0, duration.getSeconds());
        return String.format("%02d:%02d:%02d", seconds / 3600, (seconds % 3600) / 60, seconds % 60);
    }

    /** Stops repainting, leaves one final frame on screen, and restores the cursor. */
    @Override
    public void close() {
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            render();
        } catch (RuntimeException ignored) {
            // Nothing useful to do while shutting down.
        }
        out.print(SHOW_CURSOR + RESET);
        out.flush();
    }
}
