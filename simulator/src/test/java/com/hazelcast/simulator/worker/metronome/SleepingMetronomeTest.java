package com.hazelcast.simulator.worker.metronome;

public class SleepingMetronomeTest extends AbstractMetronomeTest {

    @Override
    MetronomeType getMetronomeType() {
        return MetronomeType.SLEEPING;
    }
}
