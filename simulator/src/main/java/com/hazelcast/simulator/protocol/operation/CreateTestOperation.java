package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.test.TestCase;

import java.util.Map;

/**
 * Creates a new Simulator test with the defined properties.
 */
public class CreateTestOperation implements SimulatorOperation {

    private final String testId;
    private final Map<String, String> properties;

    public CreateTestOperation(TestCase testCase) {
        this.testId = testCase.getId();
        this.properties = testCase.getProperties();
    }

    public TestCase getTestCase() {
        return new TestCase(testId, properties);
    }

    @Override
    public String toString() {
        return "CreateTestOperation{"
                + "testId='" + testId + '\''
                + '}';
    }
}
