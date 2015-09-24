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

    public long getTime() {
        return time;
    }

    public String getMessage() {
        return message;
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

    public String getWorkerId() {
        return workerId;
    }

    public String getTestId() {
        return testId;
    }

    public TestSuite getTestSuite() {
        return testSuite;
    }

    public String getCause() {
        return cause;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Failure[\n");
        sb.append("   message='").append(getMessage()).append("'\n");
        sb.append("   type='").append(getType()).append("'\n");
        sb.append("   agentAddress=").append(getAgentAddress()).append("\n");
        sb.append("   time=").append(getTime()).append("\n");
        sb.append("   hzAddress=").append(getHzAddress()).append("\n");
        sb.append("   workerId=").append(getWorkerId()).append("\n");

        TestCase testCase = getTestSuite().getTestCase(getTestId());
        if (testCase != null) {
            String prefix = "   test=";
            for (String aTestString : testCase.toString().split("\n")) {
                sb.append(prefix).append(aTestString).append("\n");
                prefix = "    ";
            }
        } else {
            sb.append("   test=").append(getTestId()).append(" unknown").append("\n");
        }

        sb.append("   cause=").append(getCause() != null ? getCause() : "null").append("\n");
        sb.append("]");

        return sb.toString();
    }
}
