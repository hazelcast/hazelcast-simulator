package com.hazelcast.simulator.worker.metronome;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SimpleMetronomeTest {

    @Test
    public void testEmptyMetronome() {
        Metronome metronome = SimpleMetronome.withFixedIntervalMs(0);
        metronome.waitForNext();

        assertFalse(metronome instanceof SimpleMetronome);
    }

    @Test
    public void testSimpleMetronome_withFixedIntervalMs() {
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

    @Test
    public void testSimpleMetronome_withFixedFrequency_25() {
        float frequency = 25;
        int intervalMs = 40;

        Metronome metronome = SimpleMetronome.withFixedFrequency(frequency);
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

        Metronome metronome = SimpleMetronome.withFixedFrequency(frequency);
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

        Metronome metronome = SimpleMetronome.withFixedFrequency(frequency);
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
