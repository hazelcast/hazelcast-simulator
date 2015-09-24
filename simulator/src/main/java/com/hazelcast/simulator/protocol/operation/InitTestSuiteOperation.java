package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.test.TestSuite;

public class InitTestSuiteOperation implements SimulatorOperation {

    private final TestSuite testSuite;

    public InitTestSuiteOperation(TestSuite testSuite) {
        this.testSuite = testSuite;
    }

    public TestSuite getTestSuite() {
        return testSuite;
    }
}
