package com.hazelcast.simulator.worker.metronome;

import java.util.concurrent.TimeUnit;

import static org.apache.commons.lang3.RandomUtils.nextLong;

/**
 * Simple {@link Metronome} implementation which busy loops on a fixed interval.
 *
 * The wait interval on the first {@link #waitForNext()} call is randomized.
 *
 * It is recommended to create a new instance for each worker thread, so they are clocked interleaved.
 */
public final class SimpleMetronome implements Metronome {

    private static final Metronome EMPTY_METRONOME = new EmptyMetronome();

    private final long intervalNanos;

    private long waitUntil;

    private SimpleMetronome(long intervalNanos) {
        this.intervalNanos = intervalNanos;
    }

    /**
     * Creates a {@link Metronome} instance with a fixed millisecond interval.
     *
     * @param intervalMs wait interval in milliseconds
     * @return a {@link Metronome} instance
     */
    public static Metronome withFixedIntervalMs(int intervalMs) {
        if (intervalMs == 0) {
            return EMPTY_METRONOME;
        }
        return new SimpleMetronome(TimeUnit.MILLISECONDS.toNanos(intervalMs));
    }

    @Override
    public void waitForNext() {
        // set random interval on the first run
        if (waitUntil == 0) {
            waitUntil = System.nanoTime() + nextLong(0, intervalNanos);
        }

        // busy loop
        long now;
        do {
            now = System.nanoTime();
        } while (now < waitUntil);

        // set regular interval for next call
        waitUntil = now + intervalNanos;
    }

    private static class EmptyMetronome implements Metronome {

        @Override
        public void waitForNext() {
        }
    }
}
