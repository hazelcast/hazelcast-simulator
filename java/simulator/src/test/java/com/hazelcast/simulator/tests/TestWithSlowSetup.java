package com.hazelcast.simulator.tests;

import com.hazelcast.simulator.test.StopException;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;

import java.util.concurrent.TimeUnit;

public class TestWithSlowSetup {

    @Setup
    public void setup() throws InterruptedException {
        TimeUnit.SECONDS.sleep(5);
    }

    @TimeStep
    public void timeStep() {
        throw new StopException();
    }
}
