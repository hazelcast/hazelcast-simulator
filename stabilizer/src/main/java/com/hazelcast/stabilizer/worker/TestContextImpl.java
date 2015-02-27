package com.hazelcast.stabilizer.worker;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.stabilizer.test.TestContext;

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
