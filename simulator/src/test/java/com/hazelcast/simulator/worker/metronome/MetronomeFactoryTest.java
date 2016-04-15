package com.hazelcast.simulator.worker.metronome;

import org.junit.Test;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static com.hazelcast.simulator.worker.metronome.MetronomeFactory.withFixedFrequency;
import static com.hazelcast.simulator.worker.metronome.MetronomeFactory.withFixedIntervalMs;
import static org.junit.Assert.assertTrue;

public class MetronomeFactoryTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(MetronomeFactory.class);
    }

    @Test
    public void testWithFixedIntervalMs_returnsSleepingMetronome_asDefault() {
        Metronome metronome = withFixedIntervalMs(23);

        assertTrue(metronome instanceof SleepingMetronome);
    }

    @Test
    public void testWithFixedIntervalMs_returnsEmptyMetronome_BUSY_SPINNING() {
        Metronome metronome = withFixedIntervalMs(0, MetronomeType.BUSY_SPINNING);

        assertTrue(metronome instanceof EmptyMetronome);
    }

    @Test
    public void testWithFixedIntervalMs_returnsEmptyMetronome_SLEEPING() {
        Metronome metronome = withFixedIntervalMs(0, MetronomeType.SLEEPING);

        assertTrue(metronome instanceof EmptyMetronome);
    }

    @Test
    public void testWithFixedIntervalMs_returnsEmptyMetronome() {
        Metronome metronome = withFixedIntervalMs(23, MetronomeType.NOP);

        assertTrue(metronome instanceof EmptyMetronome);
    }

    @Test
    public void testWithFixedIntervalMs_returnsBusySpinningMetronome() {
        Metronome metronome = withFixedIntervalMs(23, MetronomeType.BUSY_SPINNING);

        assertTrue(metronome instanceof BusySpinningMetronome);
    }

    @Test
    public void testWithFixedIntervalMs_returnsSleepingMetronome() {
        Metronome metronome = withFixedIntervalMs(23, MetronomeType.SLEEPING);

        assertTrue(metronome instanceof SleepingMetronome);
    }

    @Test
    public void testWithFixedFrequency_returnsSleepingMetronome_asDefault() {
        Metronome metronome = withFixedFrequency(23);

        assertTrue(metronome instanceof SleepingMetronome);
    }

    @Test
    public void testWithFixedFrequency_returnsEmptyMetronome_BUSY_SPINNING() {
        Metronome metronome = withFixedFrequency(0, MetronomeType.BUSY_SPINNING);

        assertTrue(metronome instanceof EmptyMetronome);
    }

    @Test
    public void testWithFixedFrequency_returnsEmptyMetronome_SLEEPING() {
        Metronome metronome = withFixedFrequency(0, MetronomeType.SLEEPING);

        assertTrue(metronome instanceof EmptyMetronome);
    }

    @Test
    public void testWithFixedFrequency_returnsEmptyMetronome() {
        Metronome metronome = withFixedFrequency(23, MetronomeType.NOP);

        assertTrue(metronome instanceof EmptyMetronome);
    }

    @Test
    public void testWithFixedFrequency_returnsBusySpinningMetronome() {
        Metronome metronome = withFixedFrequency(23, MetronomeType.BUSY_SPINNING);

        assertTrue(metronome instanceof BusySpinningMetronome);
    }

    @Test
    public void testWithFixedFrequency_returnsSleepingMetronome() {
        Metronome metronome = withFixedFrequency(23, MetronomeType.SLEEPING);

        assertTrue(metronome instanceof SleepingMetronome);
    }
}
