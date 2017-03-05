package com.hazelcast.simulator.worker.metronome;

import java.util.concurrent.TimeUnit;

public class SleepingMetronomeTest extends AbstractMetronomeTest {

    @Override
    public Metronome createMetronome(long interval, TimeUnit unit) {
        return new SleepingMetronome(unit.toNanos(interval), true);
    }
}
