package com.hazelcast.simulator.worker.metronome;

import org.junit.Test;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertTrue;

public class MetronomeFactoryTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(MetronomeFactory.class);
    }

    @Test
    public void testWithFixedIntervalMs_returnsEmptyMetronome_BUSY_SPINNING() {
        Metronome metronome = MetronomeFactory.withFixedIntervalMs(0, MetronomeType.BUSY_SPINNING);

        assertTrue(metronome instanceof EmptyMetronome);
    }

    @Test
    public void testWithFixedIntervalMs_returnsEmptyMetronome_SLEEPING() {
        Metronome metronome = MetronomeFactory.withFixedIntervalMs(0, MetronomeType.SLEEPING);

        assertTrue(metronome instanceof EmptyMetronome);
    }

    @Test
    public void testWithFixedIntervalMs_returnsSleepingMetronome_asDefault() {
        Metronome metronome = MetronomeFactory.withFixedIntervalMs(23);

        assertTrue(metronome instanceof SleepingMetronome);
    }

    @Test
    public void testWithFixedIntervalMs_returnsSleepingMetronome() {
        Metronome metronome = MetronomeFactory.withFixedIntervalMs(23, MetronomeType.SLEEPING);

        assertTrue(metronome instanceof SleepingMetronome);
    }

    @Test
    public void testWithFixedFrequency_returnsEmptyMetronome_BUSY_SPINNING() {
        Metronome metronome = MetronomeFactory.withFixedFrequency(0, MetronomeType.BUSY_SPINNING);

        assertTrue(metronome instanceof EmptyMetronome);
    }

    @Test
    public void testWithFixedFrequency_returnsEmptyMetronome_SLEEPING() {
        Metronome metronome = MetronomeFactory.withFixedFrequency(0, MetronomeType.SLEEPING);

        assertTrue(metronome instanceof EmptyMetronome);
    }

    @Test
    public void testWithFixedFrequency_returnsBusySpinningMetronome() {
        Metronome metronome = MetronomeFactory.withFixedFrequency(23, MetronomeType.BUSY_SPINNING);

        assertTrue(metronome instanceof BusySpinningMetronome);
    }

    @Test
    public void testWithFixedFrequency_returnsSleepingMetronome_asDefault() {
        Metronome metronome = MetronomeFactory.withFixedFrequency(23);

        assertTrue(metronome instanceof SleepingMetronome);
    }

    @Test
    public void testWithFixedFrequency_returnsSleepingMetronome() {
        Metronome metronome = MetronomeFactory.withFixedFrequency(23, MetronomeType.SLEEPING);

        assertTrue(metronome instanceof SleepingMetronome);
    }

    @Test
    public void testWithFixedIntervalMs_returnsBusySpinningMetronome() {
        Metronome metronome = MetronomeFactory.withFixedIntervalMs(23, MetronomeType.BUSY_SPINNING);

        assertTrue(metronome instanceof BusySpinningMetronome);
    }
}
