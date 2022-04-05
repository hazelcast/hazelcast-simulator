package com.hazelcast.simulator.worker.metronome;

import java.util.concurrent.TimeUnit;

public class BusySpinningMetronomeTest extends AbstractMetronomeTest {

    @Override
    public Metronome createMetronome(long interval, TimeUnit unit) {
        return new BusySpinningMetronome(unit.toNanos(interval),true);
    }

}
