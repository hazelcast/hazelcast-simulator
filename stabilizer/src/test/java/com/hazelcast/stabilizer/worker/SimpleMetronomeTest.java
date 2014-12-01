package com.hazelcast.stabilizer.worker;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class SimpleMetronomeTest {

    @Test
    public void testSimple() {
        int intervalMs = 10;
        Metronome metronome = SimpleMetronome.withFixedIntervalMs(intervalMs);
        long lastTimestamp = 0;
        for (int i = 0; i < 1000; i++) {
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
