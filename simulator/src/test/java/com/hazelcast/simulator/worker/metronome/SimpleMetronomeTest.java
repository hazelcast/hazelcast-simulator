package com.hazelcast.simulator.worker.metronome;

import org.junit.Test;

import static com.hazelcast.simulator.worker.metronome.SimpleMetronome.withFixedFrequency;
import static com.hazelcast.simulator.worker.metronome.SimpleMetronome.withFixedIntervalMs;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SimpleMetronomeTest {

    @Test
    public void testEmptyMetronome_withFixedIntervalMs() {
        Metronome metronome = withFixedIntervalMs(0);
        metronome.waitForNext();

        assertFalse(metronome instanceof SimpleMetronome);
    }

    @Test
    public void testEmptyMetronome_withFixedFrequency() {
        Metronome metronome = withFixedFrequency(0);
        metronome.waitForNext();

        assertFalse(metronome instanceof SimpleMetronome);
    }

    @Test
    public void testSimpleMetronome_withFixedIntervalMs() {
        int intervalMs = 50;
        Metronome metronome = withFixedIntervalMs(intervalMs);
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

    @Test
    public void testSimpleMetronome_withFixedFrequency_25() {
        float frequency = 25;
        int intervalMs = 40;

        Metronome metronome = withFixedFrequency(frequency);
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

    @Test
    public void testSimpleMetronome_withFixedFrequency_100() {
        float frequency = 100;
        int intervalMs = 10;

        Metronome metronome = withFixedFrequency(frequency);
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

    @Test
    public void testSimpleMetronome_withFixedFrequency_1000() {
        float frequency = 1000;
        int intervalMs = 1;

        Metronome metronome = withFixedFrequency(frequency);
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
