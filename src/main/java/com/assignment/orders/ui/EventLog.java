package com.assignment.orders.ui;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * A small bounded ring buffer of recent notable events.
 *
 * <p>The live dashboard repaints the whole screen several times a second, so the consumer
 * cannot also write log lines to stdout -- the two would interleave and shred each other.
 * Instead the consumer records events here and the dashboard renders the tail of the buffer
 * as part of its frame.
 *
 * <p>Oldest entries are dropped once the buffer is full, so memory is bounded no matter how
 * long the demo runs.
 */
public final class EventLog {

    public enum Level {
        INFO,
        /** A failed attempt that is about to be retried. */
        RETRY,
        /** An attempt that succeeded after previously failing. */
        RECOVERED,
        /** A message routed to the dead letter queue. */
        DLQ
    }

    public record Event(long timestampMillis, Level level, String message) {
    }

    private final int capacity;
    private final Deque<Event> events = new ArrayDeque<>();

    public EventLog(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be at least 1, got " + capacity);
        }
        this.capacity = capacity;
    }

    public synchronized void add(Level level, String message) {
        if (events.size() >= capacity) {
            events.removeFirst();
        }
        events.addLast(new Event(System.currentTimeMillis(), level, message));
    }

    /** The buffered events, oldest first. */
    public synchronized List<Event> recent() {
        return List.copyOf(events);
    }

    /** The most recent {@code count} events, oldest first. */
    public synchronized List<Event> recent(int count) {
        List<Event> all = List.copyOf(events);
        if (all.size() <= count) {
            return all;
        }
        return List.copyOf(all.subList(all.size() - count, all.size()));
    }
}
