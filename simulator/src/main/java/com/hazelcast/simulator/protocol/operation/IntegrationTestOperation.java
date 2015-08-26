package com.hazelcast.simulator.protocol.operation;

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
