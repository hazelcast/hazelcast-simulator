package com.hazelcast.simulator.test;

import com.hazelcast.core.HazelcastInstance;

public interface TestContext {

    HazelcastInstance getTargetInstance();

    String getTestId();

    boolean isStopped();

    void stop();
}
