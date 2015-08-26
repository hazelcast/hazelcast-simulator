package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.test.TestCase;

import java.util.Map;

public class CreateTestOperation implements SimulatorOperation {

    private final String testId;
    private final Map<String, String> properties;

    public CreateTestOperation(TestCase testCase) {
        this.testId = testCase.getId();
        this.properties = testCase.getProperties();
    }

    @Override
    public OperationType getOperationType() {
        return OperationType.CREATE_TEST;
    }

    public TestCase getTestCase() {
        return new TestCase(testId, properties);
    }
}
