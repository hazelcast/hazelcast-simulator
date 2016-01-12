package com.hazelcast.simulator.worker.metronome;

public class BusySpinningMetronomeTest extends AbstractMetronomeTest {

    @Override
    MetronomeType getMetronomeType() {
        return MetronomeType.BUSY_SPINNING;
    }
}
