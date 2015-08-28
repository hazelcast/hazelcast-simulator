package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.protocol.exception.ExceptionType;
import com.hazelcast.simulator.test.TestCase;

import java.util.Date;

import static com.hazelcast.simulator.utils.CommonUtils.throwableToString;

/**
 * Reports an exception which occurred on a Simulator component.
 */
public class ExceptionOperation implements SimulatorOperation {

    private final String type;
    private final String address;
    private final String testId;
    private final String cause;
    private final String stacktrace;
    private final long time;

    public ExceptionOperation(String type, String address, String testId, Throwable cause) {
        this.type = type;
        this.address = address;
        this.testId = testId;
        this.cause = cause.toString();
        this.stacktrace = throwableToString(cause);
        this.time = System.currentTimeMillis();
    }

    public String getTestId() {
        return testId;
    }

    public String getStacktrace() {
        return stacktrace;
    }

    public String getConsoleLog(long failureId) {
        StringBuilder sb = new StringBuilder();
        sb.append("Failure #").append(failureId).append(' ');
        if (address != null) {
            sb.append(address).append(' ');
        }
        if (testId != null) {
            sb.append("Test: ").append(testId).append(' ');
        }
        sb.append(type);
        if (cause != null) {
            String[] lines = cause.split("\n");
            if (lines.length > 0) {
                sb.append('[');
                sb.append(lines[0]);
                sb.append(']');
            }
        }
        return sb.toString();
    }

    public String getFileLog(TestCase testCase) {
        StringBuilder sb = new StringBuilder();
        sb.append("Failure[\n");
        sb.append("   message='").append(ExceptionType.valueOf(type).getHumanReadable()).append("'\n");
        sb.append("   type=").append(type).append("\n");
        sb.append("   address=").append(address).append("\n");
        sb.append("   time=").append(new Date(time)).append("\n");

        if (testCase != null) {
            String prefix = "   test=";
            for (String testCaseLine : testCase.toString().split("\n")) {
                sb.append(prefix).append(testCaseLine).append("\n");
                prefix = "    ";
            }
        } else if (testId != null) {
            sb.append("   test=").append(testId).append(" unknown").append("\n");
        } else {
            sb.append("   test=null\n");
        }

        sb.append("   cause=").append(cause != null ? cause : "null").append("\n");
        sb.append("   stacktrace=").append(stacktrace != null ? stacktrace : "null\n");
        sb.append("]");

        return sb.toString();
    }
}
