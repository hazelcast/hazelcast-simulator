package com.hazelcast.simulator.worker;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.test.TestContext;

public class DummyTestContext implements TestContext {

    private volatile boolean isStopped;

    @Override
    public HazelcastInstance getTargetInstance() {
        return null;
    }

    @Override
    public String getTestId() {
        return "DummyTestContext";
    }

    @Override
    public boolean isStopped() {
        return isStopped;
    }

    @Override
    public void stop() {
        isStopped = true;
    }
}
