package com.hazelcast.stabilizer.test;

import com.hazelcast.core.HazelcastInstance;

public interface TestContext {

    HazelcastInstance getTargetInstance();

    String getTestId();

    boolean isStopped();

    void stop();
}
