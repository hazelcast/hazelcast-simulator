package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.test.FailureType;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestSuite;

import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;

public class FailureOperation implements SimulatorOperation {

    private final long timestamp = System.currentTimeMillis();

    private final String message;
    private final String type;
    private final String workerAddress;
    private final String agentAddress;
    private final String hzAddress;
    private final String workerId;
    private final String testId;
    private final TestSuite testSuite;
    private final String cause;

    public FailureOperation(String message, FailureType type, SimulatorAddress workerAddress, String agentAddress,
                            String hzAddress, String workerId, String testId, TestSuite testSuite, String cause) {
        this.message = message;
        this.type = type.name();
        this.workerAddress = (workerAddress == null) ? null : workerAddress.toString();
        this.agentAddress = agentAddress;
        this.hzAddress = hzAddress;
        this.workerId = workerId;
        this.testId = testId;
        this.testSuite = testSuite;
        this.cause = cause;
    }

    public FailureType getType() {
        return FailureType.valueOf(type);
    }

    public SimulatorAddress getWorkerAddress() {
        return SimulatorAddress.fromString(workerAddress);
    }

    public String getTestId() {
        return testId;
    }

    public String getCause() {
        return cause;
    }

    public String getLogMessage(int failureNumber) {
        StringBuilder sb = new StringBuilder();
        sb.append("Failure #").append(failureNumber);

        if (workerAddress != null) {
            sb.append(' ');
            sb.append(workerAddress);
        } else if (agentAddress != null) {
            sb.append(' ');
            sb.append(agentAddress);
        }

        if (testId != null) {
            sb.append(' ');
            sb.append(testId);
        }

        sb.append(' ');
        sb.append(type);

        if (cause != null) {
            String[] lines = cause.split(NEW_LINE);
            if (lines.length > 0) {
                sb.append('[');
                sb.append(lines[0]);
                sb.append(']');
            }
        } else {
            sb.append('[').append(message).append(']');
        }

        return sb.toString();
    }

    public String getFileMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("Failure[").append(NEW_LINE);
        sb.append("   message='").append(message).append('\'').append(NEW_LINE);
        sb.append("   type=").append(type).append(NEW_LINE);
        sb.append("   timestamp=").append(timestamp).append(NEW_LINE);
        sb.append("   workerAddress=").append(workerAddress).append(NEW_LINE);
        sb.append("   agentAddress=").append(agentAddress).append(NEW_LINE);
        sb.append("   hzAddress=").append(hzAddress).append(NEW_LINE);
        sb.append("   workerId=").append(workerId).append(NEW_LINE);

        TestCase testCase = (testSuite != null) ? testSuite.getTestCase(testId) : null;
        if (testCase != null) {
            String prefix = "   test=";
            for (String testString : testCase.toString().split(NEW_LINE)) {
                sb.append(prefix).append(testString).append(NEW_LINE);
                prefix = "    ";
            }
        } else {
            sb.append("   test=").append(testId);
            if (testId != null) {
                sb.append(" (unknown)");
            }
            sb.append(NEW_LINE);
        }

        sb.append("   cause=").append(cause != null ? cause.trim() : "null").append(NEW_LINE);
        sb.append("]").append(NEW_LINE);

        return sb.toString();
    }
}
