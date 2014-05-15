package com.hazelcast.stabilizer.tests;

import com.hazelcast.core.HazelcastInstance;

public interface TestContext {

    HazelcastInstance getTargetInstance();

    String getTestId();

    boolean isStopped();
}
