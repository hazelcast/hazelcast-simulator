package com.hazelcast.simulator.worker.metronome;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class SimpleMetronomeTest {

    @Test
    public void testSimple() {
        int intervalMs = 50;
        Metronome metronome = SimpleMetronome.withFixedIntervalMs(intervalMs);
        long lastTimestamp = 0;
        for (int i = 0; i < 10; i++) {
            long startTimestamp = System.currentTimeMillis();
            metronome.waitForNext();

            if (lastTimestamp != 0) {
                long currentTimestamp = System.currentTimeMillis();
                assertTrue(currentTimestamp >= lastTimestamp + intervalMs);
            }
            lastTimestamp = startTimestamp;
        }
    }
}
