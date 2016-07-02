package com.hazelcast.simulator.worker;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.BaseThreadContext;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.TimeStep;

import java.util.ArrayList;

public class DummyTest {

    @TimeStep(prob = 0)
    public void get(ArrayList list, Probe probe) {
    }

    @TimeStep(prob = 0)
    public void put(ArrayList list) {
    }

    @AfterRun
    public void afterRun(){

    }
}
