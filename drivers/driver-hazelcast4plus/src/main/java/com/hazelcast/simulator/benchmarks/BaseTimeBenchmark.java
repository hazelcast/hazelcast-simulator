package com.hazelcast.simulator.benchmarks;

import com.hazelcast.simulator.test.annotations.TimeStep;

import java.util.Random;

public class BaseTimeBenchmark {


    // properties
    // the number of map entries
    public int entryCount = 10_000_000;

    @TimeStep(prob = 0.5)
    public void NoopTimeStep() throws Exception {

    }

    @TimeStep(prob = 0.5)
    public void RandomIntTimeStamp() throws Exception {
        int randomInt = new Random().nextInt(entryCount);
    }
}
