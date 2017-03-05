package com.hazelcast.simulator.worker.metronome;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;

public class ConstantCombinedRateMetronomeTest {

    @Test
    public void test_getIntervalNanos(){
        long intervalNanos = MILLISECONDS.toNanos(100);
        ConstantCombinedRateMetronome master = new ConstantCombinedRateMetronome(intervalNanos, true);
        assertEquals(intervalNanos, master.getIntervalNanos());
    }

    @Test
    public void test() throws InterruptedException {
        long intervalNanos = MILLISECONDS.toNanos(100);
        ConstantCombinedRateMetronome master = new ConstantCombinedRateMetronome(intervalNanos, true);

        ConstantCombinedRateMetronome metronome1 = new ConstantCombinedRateMetronome(master);
        ConstantCombinedRateMetronome metronome2 = new ConstantCombinedRateMetronome(master);

        long next = metronome1.waitForNext() + intervalNanos;

        assertEquals(next, metronome1.waitForNext());
        next += intervalNanos;
        assertEquals(next, metronome1.waitForNext());
        next += intervalNanos;
        assertEquals(next, metronome2.waitForNext());
        next += intervalNanos;
        assertEquals(next, metronome2.waitForNext());
        next += intervalNanos;
        assertEquals(next, metronome1.waitForNext());

    }
}
