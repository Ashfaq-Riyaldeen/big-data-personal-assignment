package com.ashfaq.bigdata.consumer.web;

import com.ashfaq.bigdata.consumer.aggregate.RunningAverage;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Read-only view of the running average.
 *
 * <p>The consumer log already reports the aggregate after every successful order, but a single
 * URL is easier to show on screen than a scrolling terminal, and it lets the number be checked
 * independently of the log during the demonstration.
 *
 * <p>The three values come from one {@link RunningAverage.Snapshot}, taken under the same lock
 * the listener thread writes with, so they are always a consistent triple: the total shown is
 * exactly the total that produced the average shown.
 */
@RestController
@RequestMapping("/api/stats")
public class StatsController {

    private final RunningAverage runningAverage;

    public StatsController(RunningAverage runningAverage) {
        this.runningAverage = runningAverage;
    }

    @GetMapping
    public StatsResponse stats() {
        RunningAverage.Snapshot snapshot = runningAverage.snapshot();
        return new StatsResponse(
                snapshot.count(),
                twoDecimals(snapshot.total()),
                twoDecimals(snapshot.average()));
    }

    /** Rounded the same way as the [AVERAGE] log line, so the two always agree on screen. */
    private static double twoDecimals(double value) {
        return BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP).doubleValue();
    }

    /** What the endpoint returns. Field names match the assignment plan. */
    public record StatsResponse(long successfulOrders, double totalPrice, double runningAverage) {
    }
}
