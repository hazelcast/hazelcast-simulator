package com.hazelcast.simulator.worker;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.test.TestContext;

class TestContextImpl implements TestContext {
    private final String testId;
    private final HazelcastInstance hazelcastInstance;

    private volatile boolean stopped;

    TestContextImpl(String testId, HazelcastInstance hazelcastInstance) {
        this.testId = testId;
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public HazelcastInstance getTargetInstance() {
        return hazelcastInstance;
    }

    @Override
    public String getTestId() {
        return testId;
    }

    @Override
    public boolean isStopped() {
        return stopped;
    }

    @Override
    public void stop() {
        stopped = true;
    }
}
