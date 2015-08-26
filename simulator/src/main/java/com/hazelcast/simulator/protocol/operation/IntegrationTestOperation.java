package com.hazelcast.simulator.protocol.operation;

/**
 * Operation for integration tests of the Simulator Protocol.
 */
public class IntegrationTestOperation implements SimulatorOperation {

    public static final String TEST_DATA = "IntegrationTestData";

    private final String testData;

    public IntegrationTestOperation(String testData) {
        this.testData = testData;
    }

    public String getTestData() {
        return testData;
    }
}
