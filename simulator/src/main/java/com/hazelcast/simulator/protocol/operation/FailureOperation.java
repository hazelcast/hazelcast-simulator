package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.test.FailureType;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestSuite;

public class FailureOperation implements SimulatorOperation {

    private final long time = System.currentTimeMillis();

    private final String message;
    private final String type;
    private final String agentAddress;
    private final String workerAddress;
    private final String hzAddress;
    private final String workerId;
    private final String testId;
    private final TestSuite testSuite;
    private final String cause;

    public FailureOperation(String message, FailureType type, String agentAddress, SimulatorAddress workerAddress,
                            String hzAddress, String workerId, String testId, TestSuite testSuite, String cause) {
        this.message = message;
        this.type = type.name();
        this.agentAddress = agentAddress;
        this.workerAddress = workerAddress.toString();
        this.hzAddress = hzAddress;
        this.workerId = workerId;
        this.testId = testId;
        this.testSuite = testSuite;
        this.cause = cause;
    }

    public FailureType getType() {
        return FailureType.valueOf(type);
    }

    public String getAgentAddress() {
        return agentAddress;
    }

    public String getWorkerAddress() {
        return workerAddress;
    }

    public String getHzAddress() {
        return hzAddress;
    }

    public String getTestId() {
        return testId;
    }

    public String getCause() {
        return cause;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Failure[\n");
        sb.append("   message='").append(message).append("'\n");
        sb.append("   type='").append(type).append("'\n");
        sb.append("   agentAddress=").append(agentAddress).append("\n");
        sb.append("   time=").append(time).append("\n");
        sb.append("   hzAddress=").append(hzAddress).append("\n");
        sb.append("   workerId=").append(workerId).append("\n");

        TestCase testCase = testSuite.getTestCase(testId);
        if (testCase != null) {
            String prefix = "   test=";
            for (String testString : testCase.toString().split("\n")) {
                sb.append(prefix).append(testString).append("\n");
                prefix = "    ";
            }
        } else {
            sb.append("   test=").append(testId).append(" unknown").append("\n");
        }

        sb.append("   cause=").append(cause != null ? cause : "null").append("\n");
        sb.append("]");

        return sb.toString();
    }
}
