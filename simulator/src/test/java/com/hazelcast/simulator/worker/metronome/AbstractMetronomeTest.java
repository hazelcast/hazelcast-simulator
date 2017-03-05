package com.hazelcast.simulator.worker.metronome;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertTrue;

public abstract class AbstractMetronomeTest {

    private Metronome metronome;

    public abstract Metronome createMetronome(long interval, TimeUnit unit);

    @Test
    public void testWithFixedInterval_50Ms() {
        int intervalMs = 50;
        metronome = createMetronome(intervalMs, MILLISECONDS);

        testMetronome(intervalMs);
    }

    @Test
    public void testWithFixedInterval_100Ms() {
        int intervalMs = 100;
        metronome = createMetronome(intervalMs, MILLISECONDS);

        testMetronome(intervalMs);
    }

    @Test
    public void testWithFixedInterval_1s() {
        int intervalMs = 1;
        metronome = createMetronome(intervalMs, MILLISECONDS);

        testMetronome(intervalMs);
    }

    private void testMetronome(int intervalMs) {
        // we don't want to measure the first invocation, since it has a random delay
        metronome.waitForNext();

        long lastTimestamp = 0;
        for (int i = 0; i < 10; i++) {
            long startTimestamp = System.currentTimeMillis();
            metronome.waitForNext();

            if (lastTimestamp != 0) {
                long currentTimestamp = System.currentTimeMillis();
                long actual = lastTimestamp + intervalMs;
                assertTrue(format("Expected currentTimestamp >= lastTimestamp + interval, but was %d >= %d (diff: %d ms)",
                        currentTimestamp, actual, actual - currentTimestamp), currentTimestamp >= actual);
            }
            lastTimestamp = startTimestamp;
        }
    }
}
