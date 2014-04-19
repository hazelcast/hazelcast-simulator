/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.stabilizer;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.util.Date;

public class Failure implements Serializable {

    private static final long serialVersionUID = 1;

    private final String message;
    private final InetSocketAddress agentAddress;
    private final InetSocketAddress workerAddress;
    private final String workerId;
    private final Date time;
    private final TestRecipe testRecipe;
    private final Throwable cause;

    public Failure(String message, InetSocketAddress agentAddress, InetSocketAddress workerAddress,
                   String workerId, TestRecipe testRecipe) {
        this.message = message;
        this.agentAddress = agentAddress;
        this.workerId = workerId;
        this.time = new Date();
        this.testRecipe = testRecipe;
        this.workerAddress = workerAddress;
        this.cause = null;
    }

    public Failure(String message, InetSocketAddress agentAddress, InetSocketAddress workerAddress,
                   String workerId, TestRecipe testRecipe, Throwable cause) {
        this.message = message;
        this.agentAddress = agentAddress;
        this.workerId = workerId;
        this.time = new Date();
        this.testRecipe = testRecipe;
        this.workerAddress = workerAddress;
        this.cause = cause;
    }

    public Throwable getCause() {
        return cause;
    }

    public String getMessage() {
        return message;
    }

    public String getWorkerId() {
        return workerId;
    }

    public InetSocketAddress getAgentAddress() {
        return agentAddress;
    }

    public Date getTime() {
        return time;
    }

    public TestRecipe getTestRecipe() {
        return testRecipe;
    }

    public InetSocketAddress getWorkerAddress() {
        return workerAddress;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("Failure[\n");
        sb.append("   message='").append(message).append("'\n");
        sb.append("   agentAddress=").append(agentAddress).append("\n");
        sb.append("   time=").append(time).append("\n");
        sb.append("   workerAddress=").append(workerAddress).append("\n");
        sb.append("   workerId=").append(workerId).append("\n");
        if (testRecipe != null) {
            String[] testString = testRecipe.toString().split("\n");
            sb.append("   test=").append(testString[0]).append("\n");
            for (int k = 1; k < testString.length; k++) {
                sb.append("    ").append(testString[k]).append("\n");
            }
        } else {
            sb.append("   test=").append("null").append("\n");
        }

        if (cause != null) {
            StringWriter sw = new StringWriter();
            cause.printStackTrace(new PrintWriter(sw));
            sb.append("   cause=").append(sw.toString()).append("\n");
        } else {
            sb.append("   cause=").append("null").append("\n");
        }

        sb.append("]");
        return sb.toString();
    }
}
