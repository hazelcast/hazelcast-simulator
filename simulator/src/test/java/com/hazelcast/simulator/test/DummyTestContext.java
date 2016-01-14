package com.hazelcast.simulator.test;

import com.hazelcast.core.HazelcastInstance;

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
