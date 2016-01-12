package com.hazelcast.simulator.worker.metronome;

import org.junit.Test;

import static com.hazelcast.simulator.worker.metronome.MetronomeFactory.withFixedFrequency;
import static com.hazelcast.simulator.worker.metronome.MetronomeFactory.withFixedIntervalMs;
import static org.junit.Assert.assertTrue;

public abstract class AbstractMetronomeTest {

    abstract MetronomeType getMetronomeType();

    private Metronome getFixedIntervalMsMetronome(int intervalMs) {
        return withFixedIntervalMs(intervalMs, getMetronomeType());
    }

    private Metronome getFixedFrequencyMetronome(float frequency) {
        return withFixedFrequency(frequency, getMetronomeType());
    }

    @Test
    public void testWithFixedIntervalMs() {
        int intervalMs = 50;
        Metronome metronome = getFixedIntervalMsMetronome(intervalMs);
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
    public void testWithFixedFrequency_25() {
        float frequency = 25;
        int intervalMs = 40;

        Metronome metronome = getFixedFrequencyMetronome(frequency);
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
    public void testWithFixedFrequency_100() {
        float frequency = 100;
        int intervalMs = 10;

        Metronome metronome = getFixedFrequencyMetronome(frequency);
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
    public void testWithFixedFrequency_1000() {
        float frequency = 1000;
        int intervalMs = 1;

        Metronome metronome = getFixedFrequencyMetronome(frequency);
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
