/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.coordinator.operations;

import com.hazelcast.simulator.common.FailureType;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;

import static com.hazelcast.simulator.utils.CommonUtils.throwableToString;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;

/**
 * Reports a Simulator Worker failure.
 */
public class FailureOperation implements SimulatorOperation {

    private final long timestamp = System.currentTimeMillis();

    private final String message;
    private final String type;
    private final String workerAddress;
    private final String agentAddress;
    private final String workerId;
    private final String testId;
    private TestCase testCase;
    private final String cause;
    private long durationMs;

    public FailureOperation(String message, FailureType type, SimulatorAddress workerAddress, String agentAddress,
                            Throwable cause) {
        this(message, type, workerAddress, agentAddress, null, null, cause == null ? "" : throwableToString(cause));
    }

    public FailureOperation(String message, FailureType type, SimulatorAddress workerAddress, String agentAddress,
                           String workerId, String testId, String cause) {
        this.message = message;
        this.type = type.name();
        this.workerAddress = workerAddress == null ? null : workerAddress.toString();
        this.agentAddress = agentAddress;
        this.workerId = workerId;
        this.testId = testId;
        this.cause = cause;
    }

    public void setDuration(long durationMs) {
        this.durationMs = durationMs;
    }

    public FailureOperation setTestCase(TestCase testCase) {
        this.testCase = testCase;
        return this;
    }

    public TestCase getTestCase() {
        return testCase;
    }

    public FailureType getType() {
        return FailureType.valueOf(type);
    }

    public SimulatorAddress getWorkerAddress() {
        if (workerAddress == null) {
            return null;
        }
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

        if (message != null) {
            sb.append('[').append(message).append(']');
        } else if (cause != null) {
            String[] lines = cause.split(NEW_LINE);
            if (lines.length > 0) {
                sb.append('[');
                sb.append(lines[0]);
                sb.append(']');
            }
        }

        return sb.toString();
    }

    public String getFileMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("Failure[").append(NEW_LINE);
        sb.append("   message='").append(message).append('\'').append(NEW_LINE);
        sb.append("   type=").append(type).append(NEW_LINE);
        sb.append("   timestamp=").append(timestamp).append(NEW_LINE);
        sb.append("   duration=").append(durationMs).append(" ms").append(NEW_LINE);
        sb.append("   workerAddress=").append(workerAddress).append(NEW_LINE);
        sb.append("   agentAddress=").append(agentAddress).append(NEW_LINE);
        sb.append("   workerId=").append(workerId).append(NEW_LINE);

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
