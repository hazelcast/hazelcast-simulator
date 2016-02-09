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
package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.protocol.exception.ExceptionType;
import com.hazelcast.simulator.test.TestCase;

import java.util.Date;

import static com.hazelcast.simulator.utils.CommonUtils.throwableToString;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;

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
            String[] lines = cause.split(NEW_LINE);
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
        sb.append("Failure[").append(NEW_LINE);
        sb.append("   message='").append(ExceptionType.valueOf(type).getHumanReadable()).append('\'').append(NEW_LINE);
        sb.append("   type=").append(type).append(NEW_LINE);
        sb.append("   address=").append(address).append(NEW_LINE);
        sb.append("   time=").append(new Date(time)).append(NEW_LINE);

        if (testCase != null) {
            String prefix = "   test=";
            for (String testCaseLine : testCase.toString().split(NEW_LINE)) {
                sb.append(prefix).append(testCaseLine).append(NEW_LINE);
                prefix = "    ";
            }
        } else if (testId != null) {
            sb.append("   test=").append(testId).append(" unknown").append(NEW_LINE);
        } else {
            sb.append("   test=null").append(NEW_LINE);
        }

        sb.append("   cause=").append(cause != null ? cause : "null").append(NEW_LINE);
        sb.append("   stacktrace=").append(stacktrace != null ? stacktrace : "null" + NEW_LINE);
        sb.append(']');

        return sb.toString();
    }
}
