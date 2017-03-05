package com.hazelcast.simulator.worker.metronome;

import org.junit.Test;

public class EmtyMetronomeTest {

    @Test
    public void test(){
        EmptyMetronome m = EmptyMetronome.INSTANCE;
        long whatever = m.waitForNext();
    }
}
