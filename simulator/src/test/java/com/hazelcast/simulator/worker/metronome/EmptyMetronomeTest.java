package com.hazelcast.simulator.worker.metronome;

import org.junit.Test;

public class EmptyMetronomeTest {

    @Test
    public void testWaitForNext() {
        Metronome metronome = MetronomeFactory.withFixedIntervalMs(0, MetronomeType.BUSY_SPINNING);
        metronome.waitForNext();
    }
}
